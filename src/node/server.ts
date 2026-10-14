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
import { concatBytes, latin1Decode, latin1Encode, reasonPhrase } from "../http1/headers.js";
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
  res.writeHead(status, reasonPhrase(status), { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
}

function rawErrorResponse(status: number, message: string): string {
  const body = JSON.stringify({ error: message });
  return `HTTP/1.1 ${status} ${reasonPhrase(status)}\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`;
}

const CRLFCRLF = "\r\n\r\n";
/** Node adds these for its own client connection; the edge copies response headers to the public client. */
const NODE_CONNECTION_HEADERS = new Set(["connection", "keep-alive"]);

/** Per-stream bookkeeping shared by `dispatch`, `clientError` and the socket source. */
interface Exchange {
  res?: ServerResponse;
  /** We answered (404/503/413/431/400) and Node is only discarding the rest of the request. */
  answered: boolean;
  /** The body went over the cap: the stream was taken over and the socket source is detached. */
  overflowed: boolean;
  /** A response byte reached the stream (a head Node has buffered but not flushed does not count). */
  wrote: boolean;
  /** Decoded request body bytes Node's parser pushed into `req`. */
  bodyBytes: number;
  /** Takes the stream over for a body over the cap; runs once. */
  overflow: () => void;
}

const never = () => new Promise<never>(() => {});

/**
 * The stream as the private server's socket, minus Node's own
 * `Connection`/`Keep-Alive` lines in the response head (the edge copies
 * response headers to the public client). Once `ex.overflowed`, the source
 * is detached (reads never settle, writes and resets are dropped), so nothing
 * more reaches Node.
 */
function nodeSocketSource(stream: MixerStream, ex: Exchange): DuplexSource {
  let head: Uint8Array | null = new Uint8Array(0);
  return {
    read: async () => {
      if (ex.overflowed) return never();
      const chunk = await stream.read();
      return ex.overflowed ? never() : chunk;
    },
    closeWrite: () => (ex.overflowed ? Promise.resolve() : stream.closeWrite()),
    getState: () => stream.getState(),
    reset: (code, message) => {
      if (!ex.overflowed) stream.reset(code, message);
    },
    write: (chunk) => {
      if (ex.overflowed) return Promise.resolve();
      if (head === null) {
        ex.wrote = true;
        return stream.write(chunk);
      }
      const bytes = concatBytes([head, typeof chunk === "string" ? latin1Encode(chunk) : chunk]);
      const text = latin1Decode(bytes);
      const end = text.indexOf(CRLFCRLF);
      if (end < 0) {
        head = bytes;
        return Promise.resolve();
      }
      head = null;
      ex.wrote = true;
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
    // A chunked body is counted as Node's parser pushes it into `req`, decoded:
    // the chunk that crosses the cap is withheld, so the app never reads past it.
    if (ex) {
      const push = req.push.bind(req);
      req.push = (chunk: Buffer | null, encoding?: BufferEncoding): boolean => {
        if (ex.overflowed) return false;
        if (chunk !== null && !ex.answered) {
          ex.bodyBytes += chunk.length;
          if (ex.bodyBytes > ctx.maxBodyBytes) {
            ex.overflow();
            return false;
          }
        }
        return push(chunk, encoding);
      };
    }
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
      answer(500, "handler error");
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
    const ex = exchanges.get(socket);
    // Node already ended the socket (a `Connection: close` response); `parser.finish()` can report the same error again.
    if (!socket.writable) {
      if (!ex?.answered) ctx.onError(err);
      if (ex) ex.answered = true;
      return;
    }
    if (ex) ex.answered = true;
    ctx.onError(err);
    if (ex?.res?.headersSent) {
      // A bad body after a response that is still being written: a second status line would corrupt it (Node's own handler destroys too).
      if (!ex.res.writableFinished) {
        socket.destroy();
        return;
      }
      // The response (ours or the app's) is complete: end with CLOSE, not RESET.
      socket.end();
    } else {
      socket.end(
        err.code === "HPE_HEADER_OVERFLOW"
          ? rawErrorResponse(431, "request head too large")
          : rawErrorResponse(400, "malformed request"),
      );
    }
    // Read and drop the rest of the request (away from Node's parser) so the edge's writer is never left blocked on credit.
    socket.removeAllListeners("data");
    socket.resume();
  });

  // Raw request bytes minus the decoded body: the head plus chunk framing,
  // which the decoded count never sees. Allowed: the head cap, 64 KiB, and
  // 8 bytes per decoded body byte (a 1-byte chunk, `1\r\nx\r\n`, costs 5), so
  // only pathological framing (long chunk extensions, padded sizes) trips it.
  const framingLimit = (bodyBytes: number) => 2 * MAX_HEAD_BYTES + 8 * bodyBytes;

  const serve = ((stream: MixerStream) => {
    // A chunked body over the cap (Content-Length is checked in `dispatch`):
    // a 413 if no response byte reached the stream yet (a `writeHead` Node
    // has not flushed does not count), a plain CLOSE if the app already
    // finished its response, else a RESET. Destroying the socket aborts `req`
    // (Node emits its 'error' only to existing listeners) and turns the
    // app's later `res` writes into no-ops.
    const onOverflow = (): void => {
      if (ex.overflowed) return;
      ex.overflowed = true;
      if (!ex.wrote || ex.res!.writableFinished) {
        const answer = ex.wrote ? null : rawErrorResponse(413, "request body too large");
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
    const ex: Exchange = { answered: false, overflowed: false, wrote: false, bodyBytes: 0, overflow: onOverflow };
    const sock = toDuplex(nodeSocketSource(stream, ex));
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
    // Added after Node's own 'data' listener, which parses each chunk synchronously,
    // so `ex.bodyBytes` already includes it. Also bounds a body Node no longer
    // pushes into a dumped `req` (the app finished its response).
    let received = 0;
    sock.on("data", (chunk: Buffer) => {
      received += chunk.length;
      // Until the head is dispatched, Node's own head cap (431) applies.
      if (ex.overflowed || ex.answered || !ex.res) return;
      if (received - ex.bodyBytes > framingLimit(ex.bodyBytes)) ex.overflow();
    });
    return done;
  }) as NodeStreamServer;
  serve.destroyAll = () => {
    for (const s of [...sockets]) s.destroy();
  };
  return serve;
}
