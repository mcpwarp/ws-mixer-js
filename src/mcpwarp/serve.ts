/**
 * Serving one mcpwarp request per stream: parse the edge's HTTP/1.1
 * request, hand it to the integration (a fetch-style handler, or a local
 * HTTP server via fetch), write the response back. Precedence (MCPWARP.md
 * section 4.4): 404 for any path but `MCP_PATH`, then 503 while disabled or
 * closing, both right after the head without reading the body; then 413/400
 * for a body over the cap or malformed; then the app. Other error replies:
 * 400/431 for a bad head, 500 for a failing handler, 502/504 for an
 * unreachable/slow target. Never a 1xx.
 */
import type { MixerStream } from "../stream.js";
import { HttpParseError, StreamByteReader, UnexpectedEof } from "../http1/reader.js";
import { HEAD_DEADLINE_MS, forwardableHeaders, readRequestBody, readRequestHead, requestPath, toWebRequest, type ParsedRequest } from "../http1/request.js";
import { writeErrorResponse, writeResponse } from "../http1/response.js";

/** The only request path forwarded (exact, case-sensitive, query ignored); anything else is answered 404 `{"error":"not_found"}`. */
export const MCP_PATH = "/mcp";

export type FetchHandler = (request: Request) => Response | Promise<Response>;
/** Serves one stream; never rejects. */
export type StreamServer = (stream: MixerStream) => Promise<void>;

export interface ServeContext {
  /** false while disabled/closing: requests get 503. */
  isAvailable(): boolean;
  /** The 503 message, e.g. "service disabled" / "service closing". */
  unavailableReason?(): string;
  maxBodyBytes: number;
  /** Non-fatal errors (handler throws, ...); never throws. */
  onError(err: unknown): void;
}

type Responder = (
  req: ParsedRequest,
  signal?: AbortSignal,
) => Promise<{ response: Response; dropContentEncoding?: boolean } | { error: number; message: string }>;

export const DEFAULT_TARGET_TIMEOUT_MS = 30_000;

/** Methods the Fetch standard refuses to send (the `Request` constructor throws). */
const FORBIDDEN_METHODS = new Set(["CONNECT", "TRACE", "TRACK"]);

function newAbortController(): AbortController | undefined {
  const AC = (globalThis as Record<string, any>).AbortController as typeof AbortController | undefined;
  return typeof AC === "function" ? new AC() : undefined;
}

/** Reads and discards the rest of a request we already answered, so the edge's writer is never left blocked on credit. */
function discardRest(reader: StreamByteReader): void {
  void (async () => {
    try {
      while ((await reader.readChunk()) !== null) {
        // discard
      }
    } catch {
      // the stream ended
    }
  })();
}

function isResponseLike(v: unknown): v is Response {
  const r = v as Response | null | undefined;
  return typeof r === "object" && r !== null && typeof r.status === "number" && typeof r.headers?.forEach === "function" && "body" in r;
}

/** @internal Shared front half: head deadline, parse, availability, cancellation, error replies. */
export async function serveHttp(stream: MixerStream, ctx: ServeContext, respond: Responder): Promise<void> {
  const reader = new StreamByteReader(stream);
  let timer: ReturnType<typeof setTimeout> | undefined = setTimeout(() => {
    timer = undefined;
    stream.reset("CANCEL", "request head deadline exceeded");
  }, HEAD_DEADLINE_MS);
  const clearTimer = () => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  let req: ParsedRequest;
  try {
    const head = await readRequestHead(reader, { onHead: clearTimer });
    if (requestPath(head.target) !== MCP_PATH) {
      await writeErrorResponse(stream, 404, "not_found", head.method);
      discardRest(reader);
      return;
    }
    if (!ctx.isAvailable()) {
      await writeErrorResponse(stream, 503, ctx.unavailableReason?.() ?? "service unavailable", head.method);
      discardRest(reader);
      return;
    }
    const { framing: _, ...fields } = head;
    req = { ...fields, body: await readRequestBody(reader, head, ctx.maxBodyBytes) };
  } catch (e) {
    clearTimer();
    if (e instanceof HttpParseError) {
      await writeErrorResponse(stream, e.status, e.message, "GET");
      discardRest(reader);
    } else if (e instanceof UnexpectedEof) {
      await writeErrorResponse(stream, 400, "incomplete request", "GET");
    }
    // Otherwise the stream was reset or the connection lost: nobody to answer.
    return;
  }
  clearTimer();

  let wrote = false;
  try {
    const ac = newAbortController();
    if (ac) {
      void stream.closed.then((r) => {
        if (r.kind !== "finished") ac.abort(new Error("request cancelled by the edge"));
      });
    }
    const result = await respond(req, ac?.signal);
    wrote = true;
    if ("error" in result) await writeErrorResponse(stream, result.error, result.message, req.method);
    else await writeResponse(stream, result.response, req.method, { dropContentEncoding: result.dropContentEncoding });
  } catch (e) {
    if (!wrote) await writeErrorResponse(stream, 500, "internal error", req.method);
    else stream.reset("INTERNAL_ERROR", "internal error");
    ctx.onError(e);
  }
}

/** Each request becomes a web `Request` (URL = the public URL) for `handler`; its `Response` is streamed back. */
export function serveWithHandler(handler: FetchHandler, ctx: ServeContext): StreamServer {
  return (stream) =>
    serveHttp(stream, ctx, async (req, signal) => {
      let request: Request;
      try {
        request = toWebRequest(req, signal);
      } catch {
        return { error: 400, message: "request cannot be represented as a web Request" };
      }
      let res: unknown;
      try {
        res = await handler(request);
      } catch (e) {
        ctx.onError(e);
        return { error: 500, message: "handler error" };
      }
      if (!isResponseLike(res)) {
        ctx.onError(new TypeError("handler did not return a Response"));
        return { error: 500, message: "handler error" };
      }
      return { response: res };
    });
}

/**
 * Each request is forwarded with `fetch` to a local HTTP server (mirrors the
 * Go CLI's forwarder): `/mcp` maps onto the target URL (the request's raw query
 * appended to the target's after "&", never decoded or re-encoded; no
 * other path gets this far); Host becomes the target's,
 * `x-forwarded-host`/`x-forwarded-proto` carry the public ones;
 * `accept-encoding: identity`. 504 after `timeoutMs` (default 30 s)
 * without response headers, 502 when the target is unreachable, 400 when
 * the request cannot be built (as for `handler`).
 */
export function serveWithTarget(target: string | URL, ctx: ServeContext, timeoutMs = DEFAULT_TARGET_TIMEOUT_MS): StreamServer {
  const t = new URL(String(target));
  return (stream) =>
    serveHttp(stream, ctx, async (req, edgeSignal) => {
      const url = new URL(t.href);
      const headers = new Headers();
      try {
        const q = req.target.indexOf("?");
        const query = q < 0 ? "" : req.target.slice(q + 1);
        if (query) url.search = "?" + (url.search ? url.search.slice(1) + "&" : "") + query;
        for (const [name, value] of forwardableHeaders(req.headers)) {
          if (name.toLowerCase() !== "host") headers.append(name, value);
        }
        headers.set("accept-encoding", "identity");
        headers.set("x-forwarded-host", req.host);
        headers.set("x-forwarded-proto", "https");
        // fetch() would reject these with a TypeError indistinguishable from a network failure.
        if (FORBIDDEN_METHODS.has(req.method.toUpperCase())) throw new TypeError(`forbidden method ${req.method}`);
      } catch {
        return { error: 400, message: "request cannot be represented as a web Request" };
      }

      const ac = newAbortController();
      let timedOut = false;
      let rejectOnTimeout: ((e: Error) => void) | undefined;
      const timer = setTimeout(() => {
        timedOut = true;
        ac?.abort(new Error("local server timed out"));
        rejectOnTimeout?.(new Error("local server timed out"));
      }, timeoutMs);
      if (edgeSignal) {
        if (edgeSignal.aborted) ac?.abort(edgeSignal.reason);
        else edgeSignal.addEventListener("abort", () => ac?.abort(edgeSignal.reason), { once: true });
      }
      const init: RequestInit = { method: req.method, headers, redirect: "manual" };
      if (req.body !== null && req.method !== "GET" && req.method !== "HEAD") init.body = req.body as BodyInit;
      if (ac) init.signal = ac.signal;
      let response: Response;
      try {
        const doFetch = (globalThis as Record<string, any>).fetch as typeof fetch;
        const fetched = doFetch(url.href, init);
        if (ac) {
          response = await fetched;
        } else {
          // Nothing can cancel this fetch: answer 504 on time and discard whatever it settles with later.
          fetched.then(
            (r) => {
              if (timedOut) void r.body?.cancel().catch(() => {});
            },
            () => {},
          );
          response = await Promise.race([fetched, new Promise<never>((_, reject) => (rejectOnTimeout = reject))]);
        }
      } catch (e) {
        clearTimeout(timer);
        if (timedOut) return { error: 504, message: "local server timed out" };
        const code = (e as { cause?: { code?: unknown } })?.cause?.code;
        return { error: 502, message: typeof code === "string" ? `local server unreachable (${code})` : "local server unreachable" };
      }
      clearTimeout(timer);
      if (response.status === 0 || response.type === "opaqueredirect") {
        void response.body?.cancel().catch(() => {});
        return { error: 502, message: "local server sent a redirect this runtime cannot pass through" };
      }
      // fetch already decoded any content-encoding.
      return { response, dropContentEncoding: response.headers.has("content-encoding") };
    });
}
