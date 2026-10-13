/**
 * The WebSocket seam: the WHATWG subset the SDK uses (`WebSocketLike`), the
 * factory type that creates one (`WebSocketFactory`, injectable through
 * `ConnectOptions.webSocket`), the default factory over the runtime's global
 * `WebSocket`, and the dial that opens one per WIRE.md section 2.1.
 * Platform-neutral: no `ws`, no `node:*`.
 */
import { ErrorCode } from "./errors.js";
import { isWsPackageConstructor, webSocketAcceptsHeaders } from "./runtime.js";
import { errorMessage } from "./util.js";

export const SUBPROTOCOL = "ws-mixer.v1";

/** The subset of the WHATWG WebSocket the SDK uses. Node >= 22 / Deno / Bun / browser WebSocket satisfy it. */
export interface WebSocketLike {
  /** 0 CONNECTING, 1 OPEN, 2 CLOSING, 3 CLOSED. */
  readonly readyState: number;
  readonly protocol: string;
  readonly bufferedAmount: number;
  /** The SDK sets "arraybuffer". */
  binaryType: string;
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "message" | "error" | "close", listener: (ev: any) => void): void;
  removeEventListener?(type: "open" | "message" | "error" | "close", listener: (ev: any) => void): void;
  /**
   * Non-standard, optional. A factory that can observe a rejected HTTP
   * upgrade sets this before firing "error"/"close"; the SDK then applies
   * WIRE.md section 2.9's 401/403/404/429 rules. Standard WebSockets never set it.
   */
  upgradeFailure?: { status: number; headers?: Record<string, string | undefined> };
}

export interface WebSocketInit {
  /** Always `["ws-mixer.v1"]`. */
  protocols: string[];
  /** Authorization, User-Agent, user headers, mcpwarp identity headers. */
  headers: Record<string, string>;
}

/** Creates a socket. May throw synchronously (treated as a fatal dial failure). */
export type WebSocketFactory = (url: string, init: WebSocketInit) => WebSocketLike;

/** A standard (WHATWG) WebSocket constructor: protocols only, no headers (browsers, Electron renderers, Zotero's window). */
export type WebSocketConstructorLike = new (url: string, protocols?: string | string[]) => WebSocketLike;

/** A WebSocket constructor that also takes `{protocols, headers}` (Node >= 22, Deno, Bun). */
export type WebSocketConstructorWithInit = new (url: string, init?: { protocols?: string[]; headers?: Record<string, string> }) => WebSocketLike;

/**
 * Wraps a WebSocket constructor. `headers: true` (the default) passes
 * `{protocols, headers}` (Node >= 22, Deno, Bun), or `(protocols,
 * { headers })` to the `ws` package's class; `false` passes the protocols
 * positionally and drops the headers (browsers, Electron renderers,
 * Firefox chrome code such as Zotero).
 */
export function webSocketFactory(ctor: WebSocketConstructorLike, opts: { headers: false }): WebSocketFactory;
export function webSocketFactory(ctor: WebSocketConstructorWithInit, opts?: { headers?: boolean }): WebSocketFactory;
export function webSocketFactory(ctor: WebSocketConstructorLike | WebSocketConstructorWithInit, opts?: { headers?: boolean }): WebSocketFactory {
  const withHeaders = opts?.headers ?? true;
  const isWs = withHeaders && isWsPackageConstructor(ctor);
  return (url, init) =>
    isWs
      ? new (ctor as new (url: string, protocols: string[], options: { headers: Record<string, string> }) => WebSocketLike)(url, init.protocols, { headers: init.headers })
      : withHeaders
        ? new (ctor as WebSocketConstructorWithInit)(url, { protocols: init.protocols, headers: init.headers })
        : new (ctor as WebSocketConstructorLike)(url, init.protocols);
}

/**
 * `globalThis.WebSocket`, looked up per call; headers passed iff that
 * constructor accepts them (`webSocketAcceptsHeaders`). Throws TypeError if
 * there is no global WebSocket.
 */
export const defaultWebSocketFactory: WebSocketFactory = (url, init) => {
  const ctor = (globalThis as Record<string, any>).WebSocket as WebSocketConstructorWithInit | undefined;
  if (typeof ctor !== "function") {
    throw new TypeError("ws-mixer: no global WebSocket in this runtime; pass a `webSocket` factory (e.g. webSocketFactory(win.WebSocket, { headers: false }))");
  }
  if (!webSocketAcceptsHeaders(ctor)) return webSocketFactory(ctor as WebSocketConstructorLike, { headers: false })(url, init);
  try {
    return webSocketFactory(ctor)(url, init);
  } catch (e) {
    const detail = errorMessage(e);
    throw new TypeError(
      `ws-mixer: the global WebSocket threw on {protocols, headers} (${detail}); if this runtime cannot send upgrade headers, pass \`webSocket: webSocketFactory(WebSocket, { headers: false })\``,
      { cause: e },
    );
  }
};

/** @internal What a failed dial rejects with; read by client.ts's dial/handshake attempt. */
export interface DialError extends Error {
  fatal: boolean;
  /**
   * Which ws-mixer error code a fatal dial failure maps to (WIRE.md
   * section 2.9). Only ever set for a missing/mismatched subprotocol echo
   * (UNSUPPORTED) -- an SDK-raised, local ws-mixer error, no close frame or
   * HTTP status involved. `undefined` for everything else: a plain network
   * failure (no wire error occurred), an HTTP upgrade rejection of any kind
   * including 401/403/404 (never a ws-mixer wire code -- D-2026-09-20-09),
   * and a token-provider throw/reject (an application error, not a wire
   * error) -- none of those get `errorCode`/`errorName` on the disconnect
   * reason, only `httpStatus`/`cause` as applicable (nit 5).
   */
  errorCode?: number;
  /** Present only for HTTP 429 with a parseable Retry-After header. */
  retryAfterMs?: number;
  /** The HTTP status of the upgrade response, when known (only via `WebSocketLike.upgradeFailure`). */
  httpStatus?: number;
  /** The token provider's thrown/rejected error, or the factory's synchronous throw. */
  cause?: unknown;
}

/** @internal */
export function dialError(
  message: string,
  fatal: boolean,
  opts?: { retryAfterMs?: number; errorCode?: number; httpStatus?: number; cause?: unknown },
): DialError {
  const err = new Error(message) as DialError;
  err.fatal = fatal;
  if (opts?.errorCode !== undefined) err.errorCode = opts.errorCode;
  if (opts?.retryAfterMs !== undefined) err.retryAfterMs = opts.retryAfterMs;
  if (opts?.httpStatus !== undefined) err.httpStatus = opts.httpStatus;
  if (opts?.cause !== undefined) err.cause = opts.cause;
  return err;
}

/** Parses a `Retry-After` header: either delta-seconds or an HTTP-date (RFC 9110 section 10.2.3). */
export function parseRetryAfter(value: string | string[] | undefined): number | undefined {
  const v = Array.isArray(value) ? value[0] : value;
  if (v === undefined) return undefined;
  const seconds = Number(v);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const dateMs = Date.parse(v);
  if (!Number.isNaN(dateMs)) return Math.max(0, dateMs - Date.now());
  return undefined;
}

/**
 * Classifies a rejected HTTP upgrade (WIRE.md section 2.9): 429 carries
 * Retry-After; 401/403/404 are fatal (connectOnce decides whether a 401 gets
 * the one token-provider refresh-retry); anything else looks transient. No
 * `errorCode`: an HTTP upgrade rejection is never a ws-mixer wire error
 * (D-2026-09-20-09), only `httpStatus`.
 */
function upgradeFailureError(failure: NonNullable<WebSocketLike["upgradeFailure"]>): DialError {
  const status = failure.status;
  if (status === 429) {
    return dialError(`HTTP 429 (rate limited) during handshake`, false, {
      retryAfterMs: parseRetryAfter(failure.headers?.["retry-after"]),
      httpStatus: 429,
    });
  }
  const fatal = status === 401 || status === 403 || status === 404;
  return dialError(`unexpected HTTP response during handshake: ${status}`, fatal, { httpStatus: status });
}

function eventMessage(ev: unknown, fallback: string): string {
  try {
    const e = ev as { message?: unknown; error?: { message?: unknown } } | undefined;
    if (typeof e?.message === "string" && e.message) return e.message;
    if (typeof e?.error?.message === "string" && e.error.message) return e.error.message;
  } catch {
    // fall through
  }
  return fallback;
}

/**
 * Opens a WebSocket per WIRE.md section 2.1: binary (`arraybuffer`),
 * `ws-mixer.v1` offered and required to be echoed. Resolves with the open
 * socket, or rejects with a `DialError`:
 *  - the factory throws synchronously: fatal (a configuration error, e.g.
 *    no global WebSocket);
 *  - open without the subprotocol echo: close 4010, fatal UNSUPPORTED;
 *  - "error"/"close" before open: `upgradeFailure`'s status rules when the
 *    factory set it, otherwise a non-fatal network failure (a standard
 *    WebSocket never exposes the upgrade's HTTP status);
 *  - `timeoutMs` without either: non-fatal.
 * Settles on "error" alone too: Node 22 never fires "close" after a failed
 * upgrade.
 */
export function dialWebSocket(url: string, init: WebSocketInit, factory: WebSocketFactory | undefined, timeoutMs: number): Promise<WebSocketLike> {
  return new Promise((resolve, reject) => {
    let socket: WebSocketLike;
    try {
      socket = (factory ?? defaultWebSocketFactory)(url, init);
      socket.binaryType = "arraybuffer";
    } catch (e) {
      reject(dialError(errorMessage(e), true, { cause: e }));
      return;
    }

    let settled = false;
    const cleanup = (): void => {
      clearTimeout(timer);
      socket.removeEventListener?.("open", onOpen);
      socket.removeEventListener?.("error", onError);
      socket.removeEventListener?.("close", onClose);
    };
    const fail = (err: DialError): void => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };
    const failBeforeOpen = (message: string): void => {
      const failure = socket.upgradeFailure;
      fail(failure ? upgradeFailureError(failure) : dialError(message, false));
    };

    const onOpen = (): void => {
      if (settled) return;
      if (socket.protocol !== SUBPROTOCOL) {
        try {
          socket.close(4000 + ErrorCode.UNSUPPORTED, "subprotocol not echoed");
        } catch {
          // best effort
        }
        // Missing/mismatched subprotocol echo is UNSUPPORTED (4010), not
        // UNAUTHORIZED (4011) -- WIRE.md section 2.9 lists it alongside 4010
        // in the "fatal, never reconnect" row.
        fail(
          dialError(`server did not echo the ${SUBPROTOCOL} subprotocol; failing fatally, do not retry`, true, {
            errorCode: ErrorCode.UNSUPPORTED,
          }),
        );
        return;
      }
      settled = true;
      cleanup();
      resolve(socket);
    };
    const onError = (ev: unknown): void => failBeforeOpen(eventMessage(ev, "WebSocket connection failed"));
    const onClose = (ev: unknown): void => {
      const code = (ev as { code?: unknown } | undefined)?.code;
      failBeforeOpen(`socket closed with code ${typeof code === "number" ? code : "unknown"}`);
    };

    const timer = setTimeout(() => {
      try {
        socket.close();
      } catch {
        // best effort
      }
      fail(dialError(`connect timed out after ${timeoutMs}ms`, false));
    }, timeoutMs);

    socket.addEventListener("open", onOpen);
    socket.addEventListener("error", onError);
    socket.addEventListener("close", onClose);
  });
}
