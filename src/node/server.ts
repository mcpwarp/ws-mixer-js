/**
 * Serving mcpwarp streams with a Node `http.Server` or `(req, res)`
 * listener: each stream is handed, as a socket, to a private HTTP server, so
 * Node's own parser and real IncomingMessage/ServerResponse objects reach
 * the app (Express, Fastify, MCP SDK v1's StreamableHTTPServerTransport, ...).
 * The private server never listens and is never the user's: the user's
 * server only receives `request` events.
 */
import http, { type IncomingMessage, type RequestListener, type Server, type ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import type { MixerStream } from "../stream.js";
import { concatBytes, latin1Decode, latin1Encode } from "../http1/headers.js";
import { HEAD_DEADLINE_MS, MAX_HEAD_BYTES, requestPath } from "../http1/request.js";
import { MCP_PATH, type ServeContext, type StreamServer } from "../mcpwarp/serve.js";
import { toDuplex, type DuplexSource } from "./duplex.js";

export type { ServeContext, StreamServer } from "../mcpwarp/serve.js";

function sendJson(res: ServerResponse, status: number, message: string): void {
  if (res.headersSent) {
    res.destroy();
    return;
  }
  const body = JSON.stringify({ error: message });
  res.sendDate = false;
  res.writeHead(status, { "content-type": "application/json", "content-length": Buffer.byteLength(body) });
  res.end(body);
}

function rawErrorResponse(status: number, reason: string, message: string): string {
  const body = JSON.stringify({ error: message });
  return `HTTP/1.1 ${status} ${reason}\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`;
}

const CRLFCRLF = "\r\n\r\n";
/** Node adds these for its own client connection; the edge copies response headers to the public client. */
const NODE_CONNECTION_HEADERS = new Set(["connection", "keep-alive"]);

/** Per-stream bookkeeping shared by `dispatch`, `clientError` and the socket source. */
interface Exchange {
  res?: ServerResponse;
  /** We answered (404/503/413/431/400) and Node is only discarding the rest of the request. */
  answered: boolean;
}

/**
 * The stream as the private server's socket, minus Node's own
 * `Connection`/`Keep-Alive` lines in the response head (the edge copies
 * response headers to the public client). Counts the raw request bytes
 * before Node parses them: past `limit` (a chunked body over the cap),
 * `onOverflow` takes the stream over and nothing more reaches Node; from
 * then on the source is detached (reads never settle, writes and resets
 * are dropped).
 */
function nodeSocketSource(stream: MixerStream, ex: Exchange, limit: number, onOverflow: () => void): DuplexSource {
  let head: Uint8Array | null = new Uint8Array(0);
  let received = 0;
  let detached = false;
  return {
    read: async () => {
      if (detached) return new Promise<never>(() => {});
      const chunk = await stream.read();
      if (chunk === null) return chunk;
      received += chunk.length;
      // Until the head is dispatched, Node's own head cap (431) applies.
      if (received <= limit || ex.answered || !ex.res) return chunk;
      detached = true;
      onOverflow();
      return new Promise<never>(() => {});
    },
    closeWrite: () => (detached ? Promise.resolve() : stream.closeWrite()),
    getState: () => stream.getState(),
    reset: (code, message) => {
      if (!detached) stream.reset(code, message);
    },
    write: (chunk) => {
      if (detached) return Promise.resolve();
      if (head === null) return stream.write(chunk);
      const bytes = concatBytes([head, typeof chunk === "string" ? latin1Encode(chunk) : chunk]);
      const text = latin1Decode(bytes);
      const end = text.indexOf(CRLFCRLF);
      if (end < 0) {
        head = bytes;
        return Promise.resolve();
      }
      head = null;
      const lines = text.slice(0, end).split("\r\n").filter((line, i) => i === 0 || !NODE_CONNECTION_HEADERS.has(line.slice(0, line.indexOf(":")).trim().toLowerCase()));
      return stream.write(concatBytes([latin1Encode(lines.join("\r\n") + CRLFCRLF), bytes.subarray(end + CRLFCRLF.length)]));
    },
  };
}

/** A StreamServer plus a way to tear down every stream still being served. */
export interface NodeStreamServer extends StreamServer {
  destroyAll(): void;
}

export function serveWithNodeServer(server: Server | RequestListener, ctx: ServeContext): NodeStreamServer {
  const sockets = new Set<Duplex>();
  const headTimers = new WeakMap<object, ReturnType<typeof setTimeout>>();
  const exchanges = new WeakMap<object, Exchange>();

  const dispatch = (req: IncomingMessage, res: ServerResponse): void => {
    const timer = headTimers.get(req.socket);
    if (timer !== undefined) {
      clearTimeout(timer);
      headTimers.delete(req.socket);
    }
    const ex = exchanges.get(req.socket);
    if (ex) ex.res = res;
    const answer = (status: number, message: string): void => {
      if (ex) ex.answered = true;
      sendJson(res, status, message);
    };
    // The edge treats any 1xx as the final response: never send one.
    const noop = () => {};
    res.writeContinue = noop;
    res.writeProcessing = noop;
    (res as { writeEarlyHints?: unknown }).writeEarlyHints = noop;
    delete req.headers.expect;
    // Asterisk-form (`OPTIONS *`) is 400 here as in the handler/target parser.
    if (req.url === "*") {
      answer(400, "unsupported request target form");
      req.resume();
      return;
    }
    if (requestPath(req.url ?? "") !== MCP_PATH) {
      answer(404, "not_found");
      req.resume();
      return;
    }
    if (!ctx.isAvailable()) {
      answer(503, ctx.unavailableReason?.() ?? "service unavailable");
      req.resume();
      return;
    }
    const length = Number(req.headers["content-length"]);
    if (Number.isFinite(length) && length > ctx.maxBodyBytes) {
      answer(413, "request body too large");
      req.resume();
      return;
    }
    // Same Host rule as `target:`: local servers commonly check Host against localhost.
    const publicHost = req.headers.host;
    if (publicHost !== undefined) req.headers["x-forwarded-host"] = publicHost;
    req.headers["x-forwarded-proto"] = "https";
    req.headers.host = "localhost";
    try {
      if (typeof server === "function") {
        server(req, res);
      } else if (server.listenerCount("request") === 0) {
        answer(502, "no request handler");
      } else {
        server.emit("request", req, res);
      }
    } catch (e) {
      ctx.onError(e);
      answer(500, "internal error");
    }
  };

  const priv = http.createServer({ maxHeaderSize: MAX_HEAD_BYTES }, dispatch);
  // Undocumented: without it, Node ends the socket as soon as the edge's
  // request CLOSE (read-side EOF) arrives, before the response is written.
  (priv as unknown as { httpAllowHalfOpen: boolean }).httpAllowHalfOpen = true;
  // A listener here stops Node from answering `Expect` itself (100 Continue / 417).
  priv.on("checkContinue", (req, res) => priv.emit("request", req, res));
  priv.on("checkExpectation", (req, res) => priv.emit("request", req, res));
  // Having a listener stops Node's own reply, so answer like the handler/target adapters do.
  priv.on("clientError", (err: Error & { code?: string }, socket: Duplex) => {
    if (!socket.writable) return;
    const ex = exchanges.get(socket);
    if (ex) ex.answered = true;
    ctx.onError(err);
    // A bad body after the app started its response: a second status line would corrupt it (Node's own handler destroys too).
    if (ex?.res?.headersSent) {
      socket.destroy();
      return;
    }
    socket.end(
      err.code === "HPE_HEADER_OVERFLOW"
        ? rawErrorResponse(431, "Request Header Fields Too Large", "request head too large")
        : rawErrorResponse(400, "Bad Request", "malformed request"),
    );
    // Read and drop the rest of the request (away from Node's parser) so the edge's writer is never left blocked on credit.
    socket.removeAllListeners("data");
    socket.resume();
  });

  // Raw bytes, so chunk framing counts too: a chunked body may run up to the head cap past the body cap.
  const rawLimit = MAX_HEAD_BYTES + ctx.maxBodyBytes;

  const serve = ((stream: MixerStream) => {
    const ex: Exchange = { answered: false };
    // A chunked body over the cap (Content-Length is checked in `dispatch`):
    // a 413 if the app has not started its response, a plain CLOSE if it
    // already finished it, else a RESET. Destroying the socket aborts `req`
    // (Node emits its 'error' only to existing listeners) and turns the
    // app's later `res` writes into no-ops.
    const onOverflow = (): void => {
      const res = ex.res!;
      if (!res.headersSent || res.writableFinished) {
        const answer = res.headersSent ? null : rawErrorResponse(413, "Payload Too Large", "request body too large");
        void (async () => {
          try {
            if (answer !== null) await stream.write(answer);
            await stream.closeWrite();
            while ((await stream.read()) !== null) {
              // discard the rest of the request
            }
          } catch {
            // the edge gave up on the stream
          }
        })();
      } else {
        stream.reset("CANCEL", "request body too large");
      }
      sock.destroy();
    };
    const sock = toDuplex(nodeSocketSource(stream, ex, rawLimit, onOverflow));
    exchanges.set(sock, ex);
    sockets.add(sock);
    headTimers.set(
      sock,
      setTimeout(() => {
        headTimers.delete(sock);
        stream.reset("CANCEL", "request head deadline exceeded");
      }, HEAD_DEADLINE_MS),
    );
    const done = new Promise<void>((resolve) => {
      sock.once("close", () => {
        const t = headTimers.get(sock);
        if (t !== undefined) clearTimeout(t);
        sockets.delete(sock);
        resolve();
      });
    });
    priv.emit("connection", sock);
    return done;
  }) as NodeStreamServer;
  serve.destroyAll = () => {
    for (const s of [...sockets]) s.destroy();
  };
  return serve;
}
