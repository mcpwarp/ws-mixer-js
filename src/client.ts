/**
 * Public client entry point: `connect(url, opts)`. Owns the WebSocket
 * handshake (subprotocol, headers, size limits), and the reconnect/backoff
 * policy from docs/research/2026-08-26-control-channel-and-connection-lifecycle.md
 * and OVERVIEW.md section 2.9's reconnect table. `MixerConn` (conn.ts) owns
 * everything about one already-connected socket; this file owns the loop
 * that replaces it.
 *
 * State machine (single source of truth for "are we allowed to dial /
 * reconnect right now"): idle -> dialing -> connected -> backoff -> ...,
 * terminating in closed (either `close()` was called, a fatal close code
 * was hit, or maxAttempts was exhausted). `dialing` and `backoff` both loop
 * back to `dialing` on success; every terminal transition happens exactly
 * once, in `terminal()`/`goFatal()`/`giveUp()`.
 */
import { EventEmitter } from "node:events";
import { WebSocket } from "ws";
import type { AgentInfo, DrainMsg, WelcomeMsg } from "./control.js";
import { ErrorCode, StreamError, WsMixerError, closeCode, codeName } from "./errors.js";
import { MixerConn, type WSLike } from "./conn.js";
import { MAX_MESSAGE_SIZE } from "./frame.js";
import type { MixerStream } from "./stream.js";

export const SUBPROTOCOL = "ws-mixer.v1";
// Kept in sync with package.json's "version" by hand (test/version.test.ts
// asserts the two match, so a release bump that forgets this one fails CI
// instead of silently going stale on the wire in hello.agent.sdk_version /
// the User-Agent header).
export const SDK_VERSION = "0.3.1";

export type ClientState = "idle" | "dialing" | "connected" | "backoff" | "closed";

export interface ReconnectOptions {
  /** Base delay for exponential backoff, ms. Default 1000. */
  base?: number;
  /** Backoff cap, ms. Default 60000. */
  cap?: number;
  /** Per-attempt connect timeout, ms. Default 10000. */
  connectTimeout?: number;
  /** Max reconnect attempts before giving up. Default Infinity. */
  maxAttempts?: number;
}

/** Structural subset of `ws`'s WebSocket needed during the dial (before a MixerConn exists). Lets tests inject a fake transport via `_wsFactory`. */
/**
 * Structural subset of Node's `http.ClientRequest`, as seen on `ws`'s
 * `unexpected-response` event. `ws` only calls its own `abortHandshake`
 * cleanup when nothing is listening for `unexpected-response` -- since this
 * SDK does listen (to read `statusCode`/`Retry-After`), it owns draining and
 * destroying `req`/`res` itself, or the request/socket leaks.
 */
export interface DialRequest {
  destroy?(err?: Error): void;
}

/** Structural subset of Node's `http.IncomingMessage`, as seen on `ws`'s `unexpected-response` event. */
export interface DialResponse {
  statusCode?: number;
  headers: Record<string, string | string[] | undefined>;
  resume?(): void;
  destroy?(err?: Error): void;
}

export interface DialSocket extends WSLike {
  readonly protocol: string;
  once(event: "open", listener: () => void): this;
  once(
    event: "unexpected-response",
    listener: (req: DialRequest, res: DialResponse) => void,
  ): this;
  once(event: "error", listener: (err: Error) => void): this;
}

export interface WSFactoryOptions {
  perMessageDeflate: boolean;
  maxPayload: number;
  headers: Record<string, string>;
}

/** Test-only dial hook: replaces `new WebSocket(url, [SUBPROTOCOL], options)`. */
export type WSFactory = (url: string, protocols: string[], options: WSFactoryOptions) => DialSocket;

/**
 * A ws-mixer token: either a static string or a callback returning a fresh
 * token (sync or `Promise`). Per OVERVIEW.md section 4.0, the callback MUST
 * be invoked on every dial, never cached across reconnects, and a
 * throw/rejection is fatal -- surfaced verbatim as `DisconnectReason.cause`.
 */
export type TokenProvider = string | (() => Promise<string> | string);

/** Where a disconnect originated, per OVERVIEW.md section 4.0's disconnect reason shape. */
export type DisconnectPhase = "dial" | "handshake" | "connected";

/**
 * The shape every disconnect (recoverable or fatal) is reported with, per
 * OVERVIEW.md section 4.0. `httpStatus` is set only for a dial failure whose
 * response was an HTTP status (401/403/404/429); `cause` is set only when a
 * token provider threw/rejected.
 */
export interface DisconnectReason {
  phase: DisconnectPhase;
  /**
   * The WebSocket close code, when a WS close occurred. When derived from a
   * ws-mixer error (this side's own `WsMixerError`/`ConnError`, or a peer's
   * `error{code}`), this is always the semantic `4000+error_code` -- for an
   * `error_code` outside the legal WS close range (>= 0x1000_0000, an
   * application-layer code that is legal for a stream RESET but never was
   * for a connection close), that can differ from the *actual* bytes this
   * side puts on the wire, which are clamped to `4000+INTERNAL_ERROR` (4002)
   * instead (conn.ts's `wireCloseCode`, mirroring go/wsmixer's
   * `wsCloseCode`) -- `errorCode` always keeps the real, unclamped code
   * either way. When instead observed directly from a bare close frame
   * (`onSocketClose`, no preceding `error{}`), `wsCode` is exactly what was
   * on the wire, since there is nothing else it could be.
   */
  wsCode?: number;
  errorCode?: number;
  errorName?: string;
  httpStatus?: number;
  fatal: boolean;
  message: string;
  /**
   * The reason field of the close frame *received from the peer*, verbatim
   * -- never this side's own outgoing reason (CLIENT-SDK.md's `closeReason`
   * row). Absent or empty whenever no reason was received from the peer:
   * this includes an abnormal closure (no close frame at all), this side
   * having initiated the close itself (a peer's echo carries no information
   * and RFC 6455 doesn't require it to copy the reason), and the SDK closing
   * on a peer's `error{}` without reading whatever close frame follows it,
   * as OVERVIEW.md section 2.7 allows ("logs, surfaces and closes"). The
   * human-readable text is in `message` for all of those cases instead --
   * consumers SHOULD prefer `closeReason` and fall back to `message`.
   */
  closeReason?: string;
  cause?: unknown;
}

/**
 * The actual runtime payload for every disconnect: `DisconnectReason` plus
 * the pre-existing loud "SDK bug" fields (item 7) for a protocol-bug close
 * (4001/4003/4004) -- kept alongside `errorCode`/`errorName` rather than
 * replacing them, so existing `protocolError`/`code`/`name` consumers keep
 * working.
 */
export type DisconnectPayload = DisconnectReason & (
  | { protocolError: true; code: number; name: string }
  | { protocolError?: false }
);

/**
 * Everything known about one disconnect *before* we know whether it will be
 * retried or is the last one (exhaustion/fatal): every field `DisconnectReason`
 * carries except `fatal`, built from local data at the call site (the
 * `DialError`, the handshake rejection, or the `MixerConn` close `info`) --
 * never from shared mutable instance state. This is what makes it safe for a
 * concurrent old-connection close and a new dial's failure to report
 * independently without clobbering each other (blocker 3).
 */
interface DisconnectContext {
  phase: DisconnectPhase;
  wsCode?: number;
  errorCode?: number;
  errorName?: string;
  httpStatus?: number;
  cause?: unknown;
  message: string;
  closeReason?: string;
  protocolError?: boolean;
  code?: number;
  name?: string;
}

/** Resolves a `TokenProvider` to a token string. Calling a throwing/rejecting provider propagates its error verbatim. */
async function resolveToken(token: TokenProvider): Promise<string> {
  return typeof token === "function" ? await token() : token;
}

export interface ConnectOptions {
  token: TokenProvider;
  meta?: unknown;
  agent?: Partial<AgentInfo>;
  window?: number;
  maxStreams?: number;
  capabilities?: string[];
  headers?: Record<string, string>;

  onStream?: (stream: MixerStream) => void;
  onApp?: (body: Record<string, unknown>) => void;
  onDrain?: (msg: { reason: string; deadlineMs?: number; message?: string; lastStreamId: number }) => void;
  onConnect?: (welcome: WelcomeMsg) => void;
  /**
   * Fires on every disconnect, recoverable or not (see stats() note in
   * OVERVIEW.md section 4). This is the SDK's loud, always-invoked channel
   * -- unlike `'error'`, it is not gated on a listener being attached, so a
   * `4001`/`4003`/`4004` close (OVERVIEW.md section 2.9: "these mean an SDK
   * bug and a silent retry loop hides it") always reaches an `onDisconnect`
   * the caller supplied, with `protocolError: true` plus the offending
   * `code`/`name` to make it impossible to miss.
   */
  onDisconnect?: (reason: DisconnectPayload) => void;

  reconnect?: ReconnectOptions;

  /** Test-only: injects a fake WebSocket-like transport for the dial. Never used in production. */
  _wsFactory?: WSFactory;

  /**
   * @internal Test-only: forwarded verbatim to `MixerConn`'s `ConnOptions._timing`
   * (see conn.ts) so a scaled-clock test harness (e.g. the conformance
   * runner's `--time-scale`) can run a real `connect()` against `welcome`
   * values below the wire's keepalive floors. Never set this in production;
   * stripped from the public `.d.ts` surface is not enforced today, but no
   * production code path should ever pass it.
   */
  _timing?: {
    minPingInterval?: number;
    minPingTimeout?: number;
    helloTimeout?: number;
  };
}

/** Options for an application-initiated `MixerClient.close()` (CLIENT-SDK.md's "Application close" row). See `close()`'s doc comment. */
export interface CloseOptions {
  /**
   * A ws-mixer error code (OVERVIEW.md section 2.8); the connection closes
   * with `error{code, message}` then WS close `4000+code`, instead of the
   * default graceful drain. Must be an integer in `[0, 999]` so `4000+code`
   * is a legal WS close code -- `close()` throws a `RangeError` synchronously
   * otherwise, before any close is attempted.
   */
  code?: number;
  /** The `error{}` message and (truncated to 123 UTF-8 bytes on a character boundary) the WS close reason. Defaults to "". */
  message?: string;
}

const DEFAULT_RECONNECT: Required<ReconnectOptions> = {
  base: 1000,
  cap: 60000,
  connectTimeout: 10000,
  maxAttempts: Infinity,
};

/** Close codes the client must never retry after (OVERVIEW.md section 2.9's reconnect table). */
const FATAL_WS_CODES = new Set([4000 + ErrorCode.UNSUPPORTED, 4000 + ErrorCode.UNAUTHORIZED]);
const GOING_AWAY_WS_CODE = 4000 + ErrorCode.GOING_AWAY; // 4012
const KEEPALIVE_TIMEOUT_WS_CODE = 4000 + ErrorCode.KEEPALIVE_TIMEOUT; // 4013
const ENHANCE_YOUR_CALM_WS_CODE = 4000 + ErrorCode.ENHANCE_YOUR_CALM; // 4009
const APPLICATION_CLOSE_WS_CODE = 4000 + ErrorCode.APPLICATION_CLOSE; // 4014
const ABNORMAL_CLOSURE_WS_CODE = 1001; // non-ws-mixer close treated as 4012, OVERVIEW.md section 2.8
/**
 * Close codes that mean "SDK bug", per OVERVIEW.md section 2.9's reconnect
 * table: "normal backoff and a loud developer-facing error -- these mean an
 * SDK bug and a silent retry loop hides it." Backoff already happens via the
 * normal scheduleReconnect() path below; onDisconnect (never gated on a
 * listener, unlike 'error') is the loud part.
 */
const PROTOCOL_BUG_WS_CODES = new Set([
  4000 + ErrorCode.PROTOCOL_ERROR, // 4001
  4000 + ErrorCode.FLOW_CONTROL_ERROR, // 4003
  4000 + ErrorCode.FRAME_SIZE_ERROR, // 4004
]);
// A connected-phase 4014 (APPLICATION_CLOSE) gets 4009's own "start at cap"
// treatment below (WIRE.md section 2.9): it is by nature sent *after*
// welcome (the app accepted, then refused), and welcome already reset
// `attempt` to 0, so plain full-jitter backoff would redial roughly once a
// second forever instead of climbing. A handshake-phase 4014 has no such
// problem -- it never reset `attempt` -- so it stays on the ordinary
// handshake-failure path (connectOnce's catch), attempt counter and all.

export declare interface MixerClient {
  on(event: "welcome", listener: (welcome: WelcomeMsg) => void): this;
  on(event: "stream", listener: (stream: MixerStream) => void): this;
  on(event: "app", listener: (body: Record<string, unknown>) => void): this;
  on(event: "drain", listener: (msg: DrainMsg) => void): this;
  on(event: "reconnecting", listener: (info: { attempt: number; delayMs: number; cause: string }) => void): this;
  on(event: "close", listener: (info: DisconnectPayload) => void): this;
  on(event: "error", listener: (err: WsMixerError) => void): this;
  on(event: "fatal", listener: (err: WsMixerError) => void): this;
  on(event: "pong", listener: (info: { id: number; rttMs: number }) => void): this;
}

/**
 * MixerClient owns the reconnect loop: it replaces `conn` with a fresh
 * MixerConn on every disconnect, per the policy in
 * docs/research/2026-08-26-control-channel-and-connection-lifecycle.md.
 *
 * In-flight streams are lost on reconnect -- there is no resumption
 * (OVERVIEW.md section 2.9). A handler must tell "the response ended" (EOF)
 * from "the tunnel died" (its stream is destroyed with an error) itself;
 * this SDK does not paper over the difference.
 */
export class MixerClient extends EventEmitter {
  private readonly url: string;
  private readonly opts: ConnectOptions;
  private readonly reconnectOpts: Required<ReconnectOptions>;

  private state: ClientState = "idle";
  /** The confirmed-live connection (welcome received). Null while dialing/backing off. */
  private conn: MixerConn | null = null;
  /** A MixerConn mid-handshake, not yet promoted to `conn`. Tracked so close() can tear it down too. */
  private dialingConn: MixerConn | null = null;
  /**
   * Set only while a `drain`-triggered parallel reconnect is in flight: the
   * connection that sent `drain`, still alive and finishing in-flight
   * streams. Torn down (fail()) as soon as its replacement's `welcome`
   * lands, so a superseded connection never lingers past that point.
   */
  private retiringConn: MixerConn | null = null;

  private attempt = 0;
  private everConnected = false;
  private closing = false;
  private fatal = false;
  /** Set when `drain` already started a parallel reconnect, so the close that follows it doesn't schedule a second one. */
  private drainReconnectScheduled = false;
  /**
   * Set when `drain` arrives with reconnect disabled (`maxAttempts:0`):
   * OVERVIEW.md section 2.9 says in-flight streams finish normally until the
   * server's own deadline, at which point it closes with 4012 -- so the conn
   * stays up and this flag just tells the eventual close handler to report
   * that close as the one fatal "drained; reconnect disabled" disconnect
   * instead of treating 4012 as an ordinary going-away reconnect trigger.
   */
  private drainedNoReconnect = false;
  /** OVERVIEW.md section 2.9: close 4013 gets exactly one immediate retry before falling back to normal backoff. */
  private keepaliveImmediateRetryUsed = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  /** Resolves connectOnce's backoff-delay await; settled directly by clearReconnectTimer() so close()/goFatal()/giveUp() during backoff don't leave that await dangling forever. */
  private reconnectTimerResolve: (() => void) | null = null;

  private startResolve: (() => void) | null = null;
  private startReject: ((err: Error) => void) | null = null;

  constructor(url: string, opts: ConnectOptions) {
    super();
    this.url = url;
    this.opts = opts;
    this.reconnectOpts = { ...DEFAULT_RECONNECT, ...opts.reconnect };
  }

  /** Starts the first connection attempt and resolves once `welcome` completes (or rejects if it never gets there and reconnecting is pointless: a fatal auth failure, or maxAttempts exhausted before the first success). */
  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.startResolve = resolve;
      this.startReject = reject;
      void this.connectOnce(0, "initial");
    });
  }

  /** The currently active MixerConn, if connected. */
  currentConn(): MixerConn | null {
    return this.conn;
  }

  /** Current reconnect state machine position: idle/dialing/connected/backoff/closed. */
  currentState(): ClientState {
    return this.state;
  }

  /**
   * Writes an `app` frame; resolves once `ws.send`'s callback confirms it
   * actually reached the socket (OVERVIEW.md section 4), or rejects with the
   * connection's terminal error if it fails first -- conn.ts's control queue
   * carries the same per-write resolver `sendData()` already uses for stream
   * bytes.
   */
  sendApp(body: Record<string, unknown>): Promise<void> {
    if (!this.conn) return Promise.reject(new Error("ws-mixer: sendApp called with no active connection"));
    return this.conn.sendApp(body);
  }

  /** "Ignore and count" counters for the active connection (item 11); `undefined` when there is none. */
  stats(): ReturnType<MixerConn["stats"]> | undefined {
    return this.conn?.stats();
  }

  /**
   * Graceful client shutdown: stops reconnecting for good and closes every
   * live/in-flight connection. A dial already in flight when this is called
   * is closed as soon as it resolves (see connectOnce); nothing reconnects
   * after this returns.
   *
   * With `opts.code`, this is instead an application-initiated close
   * (CLIENT-SDK.md's "Application close" row, e.g. `code:
   * ErrorCode.APPLICATION_CLOSE`): `error{code, message}` on stream 0, WS
   * close `4000+code` (message truncated to 123 UTF-8 bytes on a character
   * boundary), then the socket -- `MixerConn.fail()`'s `teardownConn`
   * already performs exactly those three steps in order. No `drain`, no
   * grace period. Validated synchronously (before either connection is
   * touched): `code` must be an integer in `[0, 999]` so `4000+code` is a
   * legal WS close code, or this throws a `RangeError`.
   */
  close(opts?: CloseOptions): Promise<void> {
    if (opts?.code !== undefined && (!Number.isInteger(opts.code) || opts.code < 0 || opts.code > 999)) {
      throw new RangeError(
        `ws-mixer: close() code must be an integer in [0, 999] (so 4000+code is a legal WS close code); got ${opts.code}`,
      );
    }
    return this.closeImpl(opts);
  }

  private async closeImpl(opts?: CloseOptions): Promise<void> {
    // Marked closing/closed FIRST, before either connection is touched, so
    // nothing below -- the 'close' handler's own reconnect-scheduling
    // branches -- ever schedules a reconnect for this shutdown.
    this.closing = true;
    this.state = "closed";
    // An app-initiated close() always wins over a pending drain deadline:
    // once this returns, the drain handler's own close (4012) must be
    // reported as an ordinary graceful close, not the drained/fatal report
    // below (blocker: drainedNoReconnect branch runs unconditionally).
    this.drainedNoReconnect = false;
    // If this races an in-flight first dial (never connected once), start()
    // must settle rather than hang forever: it's not a fatal error, just a
    // client-initiated shutdown before any connection ever completed.
    this.rejectStartIfNeverConnected(new Error("ws-mixer: closed before the first connection completed"));
    this.clearReconnectTimer();
    // Built once and reused for every conn this call tears down, so a
    // code/message given to close() isn't silently dropped for a
    // retiring/dialing conn just because it wasn't yet the primary `conn`
    // (previously these two always hard-coded NO_ERROR/"client closing"
    // regardless of opts).
    const closeErr = opts?.code !== undefined ? new WsMixerError(opts.code, opts.message ?? "") : new WsMixerError(ErrorCode.NO_ERROR, "client closing");
    if (this.retiringConn) {
      const old = this.retiringConn;
      this.retiringConn = null;
      old.fail(closeErr);
    }
    if (this.dialingConn) {
      const dialing = this.dialingConn;
      this.dialingConn = null;
      dialing.fail(closeErr);
    }
    if (this.conn) {
      if (opts?.code !== undefined) {
        this.conn.fail(closeErr);
      } else {
        await this.conn.close();
      }
    }
  }

  // this.state is a plain string-literal-union property, and TS's control
  // flow narrowing does *not* invalidate a narrowed `this.state === "x"`
  // across an `await` (even though the awaited call can, and here does,
  // reassign it) -- so connectOnce below reads it through this indirection
  // to force a fresh, unnarrowed check every time.
  private isClosed(): boolean {
    return this.state === "closed";
  }

  /** Cancels a pending backoff timer, if any, and settles connectOnce's awaited delay promise so it doesn't dangle. */
  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.reconnectTimerResolve) {
      const resolve = this.reconnectTimerResolve;
      this.reconnectTimerResolve = null;
      resolve();
    }
  }

  private async connectOnce(delayMs: number, cause: string): Promise<void> {
    if (this.closing || this.isClosed()) return;
    if (delayMs > 0) {
      this.state = "backoff";
      this.emit("reconnecting", { attempt: this.attempt, delayMs, cause });
      await new Promise<void>((resolve) => {
        this.reconnectTimerResolve = resolve;
        this.reconnectTimer = setTimeout(resolve, delayMs);
      });
      this.reconnectTimer = null;
      this.reconnectTimerResolve = null;
      if (this.closing || this.isClosed()) return;
    }

    this.state = "dialing";
    let ws: DialSocket;
    let dialedToken: string;
    try {
      const dialed = await this.dialWithAuth(this.reconnectOpts.connectTimeout);
      ws = dialed.ws;
      dialedToken = dialed.token;
    } catch (e) {
      if (this.closing || this.isClosed()) return; // close() raced the dial: nothing left to do here
      this.handleConnectFailure(e as DialError);
      return;
    }

    if (this.closing || this.isClosed()) {
      // close() during an in-flight dial: close the socket once the dial resolves, never reconnect.
      try {
        ws.close(1000, "client closing");
      } catch {
        ws.terminate?.();
      }
      return;
    }

    const conn = new MixerConn(ws as unknown as WSLike, {
      token: dialedToken,
      agent: {
        sdk: "ws-mixer-js",
        sdk_version: SDK_VERSION,
        runtime: `node/${process.version.replace(/^v/, "")}`,
        os: `${process.platform}/${process.arch}`,
        ...this.opts.agent,
      },
      meta: this.opts.meta,
      window: this.opts.window,
      maxStreams: this.opts.maxStreams,
      capabilities: this.opts.capabilities,
      _timing: this.opts._timing,
    });
    this.dialingConn = conn;
    this.wireConn(conn);

    try {
      const welcome = await conn.handshake();
      // Reset backoff ONLY on welcome (OVERVIEW.md section 2.9), never on TCP connect/101.
      this.attempt = 0;
      this.keepaliveImmediateRetryUsed = false;
      this.everConnected = true;
      this.dialingConn = null;
      this.conn = conn;
      this.state = "connected";

      // A `drain`-triggered parallel reconnect just succeeded: the old
      // connection is no longer needed, so tear it down now instead of
      // waiting on the server's own deadline (OVERVIEW.md section 2.9's
      // "old MixerConn fully torn down" requirement).
      if (this.retiringConn && this.retiringConn !== conn) {
        const old = this.retiringConn;
        this.retiringConn = null;
        // Blocker 3: this drain cycle's parallel reconnect has now landed,
        // so its "suppress one extra reconnect" job is done. Clearing it
        // here -- not in old's close handler below -- matters because
        // `old.fail()` fires that handler synchronously, and by then
        // `this.retiringConn` is already null (just cleared above), so the
        // handler can no longer tell "this was the retiring conn" from
        // `this.retiringConn === conn`. Left uncleared, the flag would still
        // be true the next time the (now active) replacement disconnects for
        // an unrelated reason, silently swallowing that reconnect too.
        this.drainReconnectScheduled = false;
        // In-flight streams on the superseded conn are still live -- they
        // didn't fail, the *connection* they were riding on was replaced.
        // Give each one a stream-scoped CANCEL ("connection drained"), not
        // the connection-level NO_ERROR used to tear the socket itself down,
        // so an app distinguishes "this stream was cancelled" from "the
        // whole conn errored" (see MixerStream.terminateNoThrow).
        old.fail(
          new WsMixerError(ErrorCode.NO_ERROR, "superseded by a new connection"),
          (streamId) => new StreamError(ErrorCode.CANCEL, streamId, "connection drained"),
        );
      }

      this.opts.onConnect?.(welcome);
      if (this.startResolve) {
        const resolve = this.startResolve;
        this.startResolve = null;
        this.startReject = null;
        resolve();
      }
    } catch (e) {
      this.dialingConn = null;
      const err = e as WsMixerError;
      // A raw peer close (onSocketClose) carries `err.wsCode`: derive
      // errorCode/wsCode from it exactly like the connected-phase 'close'
      // handler below does (`info.errorCode ?? info.wsCode! - 4000`), rather
      // than trusting onSocketClose's generic INTERNAL_ERROR placeholder --
      // a bare 4010/4011 close before welcome must classify (and go fatal)
      // the same way a post-welcome one does. A *locally* raised failure
      // (hello timeout, welcome validation, ...) has no observed wsCode: its
      // `err.code` is already the genuine classification, so derive the WS
      // code it's about to close with instead (CLIENT-SDK.md's
      // "Handshake-phase close" row: the welcome timeout carries the locally
      // generated wsCode 4001, no closeReason).
      let errorCode: number | undefined;
      let wsCode = err.wsCode;
      if (wsCode !== undefined) {
        errorCode = wsCode >= 4000 && wsCode < 5000 ? wsCode - 4000 : undefined;
      } else {
        errorCode = err.code;
        wsCode = closeCode(err.code);
      }
      const ctx: DisconnectContext = {
        phase: "handshake",
        wsCode,
        errorCode,
        errorName: errorCode !== undefined ? codeName(errorCode) : undefined,
        message: err.message,
        closeReason: err.closeReason,
      };
      if (err.fatal || errorCode === ErrorCode.UNSUPPORTED || errorCode === ErrorCode.UNAUTHORIZED) {
        this.goFatal(ctx);
        return;
      }
      if (this.closing || this.isClosed()) return;
      // Non-fatal handshake failure (e.g. no `welcome` within the hello
      // timeout, or the peer closing the socket before it ever arrived):
      // still "every disconnect...is reported" (OVERVIEW.md section 4.0) --
      // exactly once, whether that's this retry's report or (if maxAttempts
      // is already exhausted) the single merged exhaustion report
      // scheduleReconnect emits via giveUp instead.
      this.scheduleReconnect(ctx, "handshake failed: " + err.message);
    }
  }

  /**
   * Resolves the token provider and dials once, per OVERVIEW.md section 4.0:
   * the provider is called fresh on every dial (before headers are built),
   * and an HTTP 401 gets exactly one immediate refresh-retry -- call the
   * provider again and redial right away, no backoff -- when a provider
   * (not a static string) is present and this dial hasn't already used its
   * retry. A static token, or a second 401, is fatal (`dialWebSocket`
   * already marks 401 fatal by default; this only overrides that to retry
   * once). The retry still counts toward `maxAttempts`, so it's skipped if
   * the ceiling is already reached. A throwing/rejecting provider is always
   * fatal, verbatim, whether on the first call or the retry's.
   *
   * This one-time refresh-retry is scoped to the dial's own HTTP 401
   * response (CLIENT-SDK.md's "401 on upgrade" row). A *handshake-phase*
   * UNAUTHORIZED (4011) -- whether from the normative error{11}+close
   * rejection (OVERVIEW.md section 3.4's Authenticate hook,
   * spec/fixtures/sequences/auth_failure.json) or a bare 4011 close with no
   * error{} -- has no equivalent retry at this layer and is simply fatal,
   * the same as any other 4011 (conn.ts's dispatchControl routes a
   * pre-welcome `error` to handlePeerError precisely so it surfaces with its
   * own code instead of being masked as a local PROTOCOL_ERROR).
   */
  private async dialWithAuth(connectTimeoutMs: number): Promise<{ ws: DialSocket; token: string }> {
    const isProvider = typeof this.opts.token === "function";
    let token: string;
    try {
      token = await resolveToken(this.opts.token);
    } catch (err) {
      throw providerDialError(err);
    }
    try {
      const ws = await dialWebSocket(this.url, token, this.opts, connectTimeoutMs);
      return { ws, token };
    } catch (e) {
      const err = e as DialError;
      if (err.httpStatus !== 401 || !isProvider || this.attempt >= this.reconnectOpts.maxAttempts) {
        throw err;
      }
      this.attempt++;
      let retryToken: string;
      try {
        retryToken = await resolveToken(this.opts.token);
      } catch (provErr) {
        throw providerDialError(provErr);
      }
      const ws = await dialWebSocket(this.url, retryToken, this.opts, connectTimeoutMs);
      return { ws, token: retryToken };
    }
  }

  private wireConn(conn: MixerConn): void {
    conn.on("welcome", (w) => this.emit("welcome", w));
    // These three return their handler's result (possibly a Promise) so
    // MixerConn's delivery loop (emitOrdered, item 2) actually awaits
    // onStream/onApp/onDrain before moving to the next queued event, and so
    // a throw/rejection there is caught by emitOrdered instead of becoming
    // an unhandled rejection.
    conn.on("stream", (s) => {
      this.emit("stream", s);
      return this.opts.onStream?.(s);
    });
    conn.on("app", (b) => {
      this.emit("app", b);
      return this.opts.onApp?.(b);
    });
    conn.on("pong", (p) => this.emit("pong", p));
    conn.on("drain", (d) => {
      this.emit("drain", d);
      if (!this.closing && this.state !== "closed") {
        if (this.reconnectOpts.maxAttempts > 0) {
          // Reconnect immediately and in parallel, before the old connection closes (OVERVIEW.md section 2.9).
          this.drainReconnectScheduled = true;
          this.retiringConn = conn;
          void this.connectOnce(Math.random() * 2000, "drain");
        } else {
          // maxAttempts:0 means this SDK will never reconnect (OVERVIEW.md
          // section 4.0's reconnect table), but that doesn't make `drain` an
          // immediate fatal event: section 2.9 says in-flight streams finish
          // normally until the server's own deadline, then the server closes
          // with 4012. So we let the conn run to that close instead of
          // tearing it down here -- the close handler below reports that
          // 4012 as the one fatal "drained; reconnect disabled" disconnect.
          this.drainedNoReconnect = true;
        }
      }
      return this.opts.onDrain?.({ reason: d.reason, deadlineMs: d.deadline_ms, message: d.message, lastStreamId: d.last_stream_id });
    });
    // Guarded like MixerConn's own emit('error') (item 2): 'close' below
    // always fires and reaches onDisconnect(reason), so nothing is silent.
    conn.on("error", (err) => {
      if (this.listenerCount("error") > 0) this.emit("error", err);
    });
    conn.on("close", (info) => {
      const wasActive = this.conn === conn;
      if (this.conn === conn) this.conn = null;
      if (this.dialingConn === conn) this.dialingConn = null;
      if (this.retiringConn === conn) this.retiringConn = null;

      // A conn that never became the active connection either had its
      // handshake() promise already handled in connectOnce's catch (a normal
      // dial/handshake failure), or was fail()'d directly by close()/the
      // retiring-conn teardown above (which already know why) -- either way,
      // scheduling a second reconnect here would race the one already in flight.
      if (!wasActive) return;

      const fatal = info.wsCode !== undefined && FATAL_WS_CODES.has(info.wsCode);
      const protocolBug = info.wsCode !== undefined && PROTOCOL_BUG_WS_CODES.has(info.wsCode);
      // A post-handshake close is always phase "connected"; it never carries
      // an HTTP status or a token-provider cause (those only apply to a dial)
      // -- built fresh from `info` (this specific close), never from shared
      // instance state, so a concurrent dial's own report can't clobber it
      // and vice versa (blocker 3).
      const ctx: DisconnectContext = protocolBug
        ? // errorCode is only set when MixerConn itself detected the failure
          // (fail()'s teardownConn); a raw WS-level close (onSocketClose)
          // never carries one, even though wsCode = 4000 + code still tells
          // us exactly which one it was (closeCode() in errors.ts) -- derive
          // it rather than falling back to something less informative than
          // the wire already told us.
          (() => {
            const code = info.errorCode ?? info.wsCode! - 4000;
            return {
              phase: "connected",
              wsCode: info.wsCode,
              errorCode: info.errorCode,
              errorName: codeName(code),
              message: info.message,
              closeReason: info.closeReason,
              protocolError: true,
              code,
              name: codeName(code),
            };
          })()
        : {
            phase: "connected",
            wsCode: info.wsCode,
            errorCode: info.errorCode,
            // Same derivation as the protocolBug branch above: a raw 4xxx
            // close with no preceding `error` control frame (e.g. 4011 from
            // onSocketClose) still tells us exactly which ws-mixer error it
            // was via wsCode - 4000, so derive errorName from that instead
            // of leaving it undefined just because errorCode wasn't set.
            errorName:
              info.errorCode !== undefined
                ? codeName(info.errorCode)
                : info.wsCode !== undefined && info.wsCode >= 4000 && info.wsCode < 5000
                  ? codeName(info.wsCode - 4000)
                  : undefined,
            message: info.message,
            closeReason: info.closeReason,
          };

      if (fatal) {
        // Exactly one report for this close: it's fatal, so there's no
        // separate "exhaustion" report to merge it with.
        this.notifyDisconnect(this.buildPayload(ctx, true));
        if (this.closing || this.fatal || this.state === "closed") return;
        this.state = "closed";
        this.fatal = true;
        this.closing = true;
        this.clearReconnectTimer();
        const err = new WsMixerError(info.errorCode ?? ErrorCode.INTERNAL_ERROR, info.message, { fatal: true });
        this.emit("fatal", err);
        this.rejectStartIfNeverConnected(err);
        return;
      }

      if (this.fatal) {
        // Already reported this disconnect fatally -- the wsCode branch
        // above on this very call. This conn's close finally catching up is
        // not a second disconnect. (A *superseded* conn's close never
        // reaches this far at all: it's caught by the `!wasActive` return
        // above, since `this.conn` was already reassigned to its replacement
        // before old.fail() ran.)
        return;
      }
      if (this.closing || this.state === "closed") {
        // Already terminating for an unrelated reason (client-initiated
        // close()) -- still one report, just no scheduling decision to make.
        // Checked before drainedNoReconnect below: an app-initiated close()
        // during a drain deadline is a normal graceful close, not a fatal
        // "drained; reconnect disabled" report (close() also clears
        // drainedNoReconnect itself, but this ordering is what actually
        // matters if that ever changes).
        this.notifyDisconnect(this.buildPayload(ctx, false));
        return;
      }

      if (this.drainedNoReconnect) {
        // The drain handler above saw maxAttempts:0 and let this conn run to
        // its own deadline instead of reconnecting; this is that deadline's
        // close (4012 per OVERVIEW.md section 2.9) arriving now. One report,
        // fatal, with a message that says why -- not a going-away reconnect.
        this.drainedNoReconnect = false;
        this.state = "closed";
        this.fatal = true;
        this.closing = true;
        this.clearReconnectTimer();
        this.notifyDisconnect(this.buildPayload(ctx, true, "drained; reconnect disabled"));
        const err = new WsMixerError(ctx.errorCode ?? ErrorCode.GOING_AWAY, "drained; reconnect disabled", { fatal: true });
        this.emit("fatal", err);
        this.rejectStartIfNeverConnected(err);
        return;
      }
      if (this.drainReconnectScheduled) {
        // Already reconnecting in parallel from the `drain` handler above.
        this.notifyDisconnect(this.buildPayload(ctx, false));
        this.drainReconnectScheduled = false;
        return;
      }
      if (info.wsCode === GOING_AWAY_WS_CODE || info.wsCode === ABNORMAL_CLOSURE_WS_CODE) {
        this.reconnectImmediately(ctx, "going_away");
        return;
      }
      if (info.wsCode === KEEPALIVE_TIMEOUT_WS_CODE) {
        this.scheduleReconnectAfterKeepaliveTimeout(ctx, "keepalive_timeout");
        return;
      }
      if (info.wsCode === ENHANCE_YOUR_CALM_WS_CODE) {
        this.scheduleReconnectAtCap(ctx, "enhance_your_calm");
        return;
      }
      if (info.wsCode === APPLICATION_CLOSE_WS_CODE) {
        // WIRE.md section 2.9: a connected-phase 4014 is, by nature, sent
        // *after* welcome already reset `attempt` to 0 -- same "start at
        // cap" treatment as 4009 above, for the same reason (see the const's
        // own comment). Covers both the error{14}+close and bare-4014
        // variants; a *handshake-phase* 4014 never reaches this handler at
        // all (it's a connectOnce catch/goFatal-or-scheduleReconnect concern,
        // where `attempt` does climb normally).
        this.scheduleReconnectAtCap(ctx, "application_close");
        return;
      }
      this.scheduleReconnect(ctx, info.message);
    });
  }

  private notifyDisconnect(payload: DisconnectPayload): void {
    this.opts.onDisconnect?.(payload);
    this.emit("close", payload);
  }

  /** Builds the final `DisconnectPayload` for a `DisconnectContext`, filling in `fatal` and, when present, the exhaustion message override. */
  private buildPayload(ctx: DisconnectContext, fatal: boolean, messageOverride?: string): DisconnectPayload {
    const base = {
      phase: ctx.phase,
      wsCode: ctx.wsCode,
      errorCode: ctx.errorCode,
      errorName: ctx.errorName,
      httpStatus: ctx.httpStatus,
      cause: ctx.cause,
      message: messageOverride ?? ctx.message,
      closeReason: ctx.closeReason,
      fatal,
    };
    if (ctx.protocolError) {
      return { ...base, protocolError: true, code: ctx.code!, name: ctx.name! };
    }
    return { ...base, protocolError: false };
  }

  private rejectStartIfNeverConnected(err: Error): void {
    if (!this.everConnected && this.startReject) {
      const reject = this.startReject;
      this.startResolve = null;
      this.startReject = null;
      reject(err);
    }
  }

  /** Fatal per OVERVIEW.md section 2.9: never reconnect, surface loudly, and fail start() if it never got a first connection. One report only. */
  private goFatal(ctx: DisconnectContext): void {
    if (this.state === "closed") return;
    this.state = "closed";
    this.fatal = true;
    this.closing = true;
    this.clearReconnectTimer();
    // Defensive: no current caller reaches goFatal() with a live `this.conn`
    // (dial/handshake failures only ever have a `dialingConn`), but if one
    // ever does, detach it before failing it so its own 'close' handler sees
    // `wasActive === false` and doesn't produce a second report -- this call
    // is still the one report for this fatal disconnect.
    const conn = this.conn;
    this.conn = null;
    const err = new WsMixerError(ctx.errorCode ?? ErrorCode.INTERNAL_ERROR, ctx.message, { fatal: true });
    if (conn) conn.fail(err);
    this.emit("fatal", err);
    this.notifyDisconnect(this.buildPayload(ctx, true));
    this.rejectStartIfNeverConnected(err);
  }

  /**
   * maxAttempts exhausted: stop, but connect() must reject if it never
   * succeeded once. Reports exactly one `DisconnectReason` -- the merged
   * exhaustion report, `fatal: true` with the exhaustion `message`, but
   * carrying the underlying failure's `wsCode`/`errorCode`/`httpStatus`/
   * `cause` from `ctx` -- never a second report on top of the one the
   * failure itself would otherwise have gotten (blocker 1).
   */
  private giveUp(ctx: DisconnectContext, message: string): void {
    if (this.state === "closed") return;
    this.state = "closed";
    this.closing = true;
    this.clearReconnectTimer();
    this.notifyDisconnect(this.buildPayload(ctx, true, message));
    this.rejectStartIfNeverConnected(new Error(message));
  }

  private handleConnectFailure(e: DialError): void {
    const ctx: DisconnectContext = { phase: "dial", httpStatus: e.httpStatus, cause: e.cause, message: e.message };
    if (e.fatal) {
      this.goFatal(
        e.errorCode !== undefined ? { ...ctx, errorCode: e.errorCode, errorName: codeName(e.errorCode) } : ctx,
      );
      return;
    }
    if (e.retryAfterMs !== undefined) {
      // HTTP 429 with Retry-After: honour the server's hint verbatim instead of our own jitter.
      this.reportAndSchedule(ctx, e.message, () => e.retryAfterMs!);
      return;
    }
    this.scheduleReconnect(ctx, e.message);
  }

  /**
   * The single point where a recoverable disconnect either gets its one
   * report and a scheduled retry, or -- if `maxAttempts` is already
   * exhausted -- gets folded into `giveUp`'s one merged exhaustion report
   * instead. Exhaustion is checked *before* any report goes out, which is
   * what makes "exactly one `DisconnectReason` per disconnect" (blocker 1)
   * hold: the two outcomes are mutually exclusive, never both.
   */
  private reportAndSchedule(ctx: DisconnectContext, cause: string, computeDelay: () => number): void {
    if (this.closing || this.fatal || this.state === "closed") return;
    if (this.attempt >= this.reconnectOpts.maxAttempts) {
      this.giveUp(ctx, `max reconnect attempts (${this.reconnectOpts.maxAttempts}) exhausted: ${cause}`);
      return;
    }
    this.notifyDisconnect(this.buildPayload(ctx, false));
    this.attempt++;
    const delay = computeDelay();
    void this.connectOnce(delay, cause);
  }

  /**
   * Close 4012 / close 1001 with no preceding `drain`: reconnect
   * immediately, jitter random(0, 2000)ms only -- but still counts toward
   * `attempt`/`maxAttempts`, so a 4012 flap loop (a server stuck
   * accepting-then-immediately-draining) still respects the ceiling instead
   * of retrying forever. (The `drain`-triggered parallel reconnect is a
   * separate, direct connectOnce() call in the `drain` handler above and
   * does not bump `attempt` -- it is the server telling us to move, not a
   * failure being retried.)
   */
  private reconnectImmediately(ctx: DisconnectContext, cause: string): void {
    this.reportAndSchedule(ctx, cause, () => Math.random() * 2000);
  }

  /**
   * Close 4013 KEEPALIVE_TIMEOUT: one immediate attempt, then normal backoff
   * (OVERVIEW.md section 2.9). The immediate attempt goes through
   * reportAndSchedule -- same as the 4012 path (reconnectImmediately) -- so
   * it respects the maxAttempts ceiling too: a 4013 flap loop with no budget
   * left gives up (one merged `fatal: true` report) instead of dialing a
   * socket reportAndSchedule would otherwise have refused to allow.
   */
  private scheduleReconnectAfterKeepaliveTimeout(ctx: DisconnectContext, cause: string): void {
    if (!this.keepaliveImmediateRetryUsed) {
      this.keepaliveImmediateRetryUsed = true;
      this.reportAndSchedule(ctx, cause, () => 0);
      return;
    }
    this.keepaliveImmediateRetryUsed = false;
    this.scheduleReconnect(ctx, cause);
  }

  /** Close 4009 ENHANCE_YOUR_CALM: start backoff at the cap, not at base. */
  private scheduleReconnectAtCap(ctx: DisconnectContext, cause: string): void {
    this.reportAndSchedule(ctx, cause, () => Math.random() * this.reconnectOpts.cap);
  }

  /** Normal AWS full-jitter backoff: everything not covered by a more specific rule above. */
  private scheduleReconnect(ctx: DisconnectContext, cause: string): void {
    this.reportAndSchedule(ctx, cause, () => fullJitterDelay(this.attempt, this.reconnectOpts.base, this.reconnectOpts.cap));
  }
}

/** AWS "full jitter": random(0, min(cap, base * 2^attempt)). */
export function fullJitterDelay(attempt: number, base: number, cap: number): number {
  const exp = Math.min(cap, base * 2 ** attempt);
  return Math.random() * exp;
}

interface DialError extends Error {
  fatal: boolean;
  /**
   * Which ws-mixer error code a fatal dial failure maps to (OVERVIEW.md
   * section 2.9): UNAUTHORIZED for 401/403/404, UNSUPPORTED for a
   * missing/mismatched subprotocol echo. Left `undefined` for a plain
   * network failure (no wire error occurred) and for a token-provider
   * throw/reject -- that's an application error, not a ws-mixer wire error,
   * so it gets no `errorCode`/`errorName` on the disconnect reason, only
   * `cause` (nit 5).
   */
  errorCode?: number;
  /** Present only for HTTP 429 with a parseable Retry-After header. */
  retryAfterMs?: number;
  /** The HTTP status of the upgrade response, when the dial failed at the HTTP layer (401/403/404/429). */
  httpStatus?: number;
  /** The token provider's thrown/rejected error, when the dial failed there instead of at the socket. */
  cause?: unknown;
}

function dialError(
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

/**
 * Wraps a token provider's thrown/rejected error as a fatal dial failure
 * (OVERVIEW.md section 4.0: "no retry, the thrown/rejected error is
 * surfaced verbatim"). Deliberately carries no `errorCode`: this is an
 * application-level failure, not a ws-mixer wire error (nit 5) -- `cause`
 * is how the caller gets at it.
 */
function providerDialError(err: unknown): DialError {
  const message = err instanceof Error ? err.message : String(err);
  return dialError(message, true, { cause: err });
}

/** Parses a `Retry-After` header: either delta-seconds or an HTTP-date (RFC 9110 section 10.2.3). */
function parseRetryAfter(value: string | string[] | undefined): number | undefined {
  const v = Array.isArray(value) ? value[0] : value;
  if (v === undefined) return undefined;
  const seconds = Number(v);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const dateMs = Date.parse(v);
  if (!Number.isNaN(dateMs)) return Math.max(0, dateMs - Date.now());
  return undefined;
}

function defaultWsFactory(url: string, protocols: string[], options: WSFactoryOptions): DialSocket {
  return new WebSocket(url, protocols, options) as unknown as DialSocket;
}

/**
 * Opens the WebSocket per OVERVIEW.md section 2.1: binary, no compression,
 * `ws-mixer.v1` subprotocol offered and required to be echoed, Bearer auth
 * header. Fatal (never retried) on a missing/mismatched subprotocol echo or
 * an HTTP 401/403/404 handshake response; HTTP 429 carries `Retry-After`
 * (if present) back to the caller instead of our own jittered delay.
 */
function dialWebSocket(url: string, token: string, opts: ConnectOptions, connectTimeoutMs: number): Promise<DialSocket> {
  return new Promise((resolve, reject) => {
    const factory = opts._wsFactory ?? defaultWsFactory;
    const ws = factory(url, [SUBPROTOCOL], {
      perMessageDeflate: false,
      maxPayload: MAX_MESSAGE_SIZE,
      headers: {
        Authorization: `Bearer ${token}`,
        "User-Agent": `ws-mixer-js/${SDK_VERSION} (node/${process.version.replace(/^v/, "")} ${process.platform}/${process.arch})`,
        ...opts.headers,
      },
    });

    const timer = setTimeout(() => {
      ws.terminate?.();
      reject(dialError(`connect timed out after ${connectTimeoutMs}ms`, false));
    }, connectTimeoutMs);

    let settled = false;
    ws.once("unexpected-response", (req, res) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        // `ws` skips its own abortHandshake cleanup whenever an
        // 'unexpected-response' listener is attached (ours is, right here) --
        // it assumes we'll do it ourselves. Drain the response so its
        // underlying socket can be reused/closed cleanly, then destroy the
        // request and the `ws` wrapper so nothing lingers (item 2). `req`/
        // `res` are optional-chained throughout: some transports/mocks omit
        // one or both, and a defensive cleanup here must never itself throw.
        res?.resume?.();
        req?.destroy?.();
        res?.destroy?.();
        ws.terminate?.();
        const status = res?.statusCode;
        if (status === 429) {
          reject(
            dialError(`HTTP 429 (rate limited) during handshake`, false, {
              retryAfterMs: parseRetryAfter(res?.headers?.["retry-after"]),
              httpStatus: 429,
            }),
          );
          return;
        }
        if (status === 401 || status === 403) {
          // Auth/authorization failure (OVERVIEW.md section 2.9 groups it
          // with UNAUTHORIZED, 4011). A 401 is provisionally fatal here --
          // dialWithAuth below is the one place that knows whether a token
          // provider is present and this dial hasn't already used its one
          // refresh-retry, and overrides this by catching and retrying
          // instead of propagating it.
          reject(
            dialError(`unexpected HTTP response during handshake: ${status}`, true, {
              errorCode: ErrorCode.UNAUTHORIZED,
              httpStatus: status,
            }),
          );
          return;
        }
        if (status === 404) {
          // Not found is fatal (retrying the same URL can't fix it), but
          // unlike 401/403 it isn't a ws-mixer wire error -- no errorCode,
          // only httpStatus.
          reject(dialError(`unexpected HTTP response during handshake: ${status}`, true, { httpStatus: status }));
          return;
        }
        // Any other status (5xx, or none at all) looks transient rather than
        // a hard rejection: retry with normal backoff instead of giving up.
        // No errorCode -- this never came from a ws-mixer wire error.
        reject(dialError(`unexpected HTTP response during handshake: ${status}`, false, { httpStatus: status }));
      } catch (e) {
        // Never let a throw escape out of `ws`'s emit (item 3): always
        // settle the dial with something, even when we don't know what the
        // response actually was.
        reject(dialError(e instanceof Error ? e.message : String(e), false));
      }
    });
    ws.once("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(dialError(err.message, false));
    });
    ws.once("open", () => {
      if (settled) return;
      if (ws.protocol !== SUBPROTOCOL) {
        settled = true;
        clearTimeout(timer);
        ws.close(1002);
        // Missing/mismatched subprotocol echo is UNSUPPORTED (4010), not
        // UNAUTHORIZED (4011) -- OVERVIEW.md section 2.9 lists it alongside
        // 4010 in the "fatal, never reconnect" row, distinct from the
        // 401/403/404 auth-failure row above.
        reject(
          dialError(`server did not echo the ${SUBPROTOCOL} subprotocol; failing fatally, do not retry`, true, {
            errorCode: ErrorCode.UNSUPPORTED,
          }),
        );
        return;
      }
      settled = true;
      clearTimeout(timer);
      resolve(ws);
    });
  });
}

/** Connects to a ws-mixer.v1 server and starts the reconnect-managed client. Resolves once the first `welcome` lands. */
export async function connect(url: string, opts: ConnectOptions): Promise<MixerClient> {
  const client = new MixerClient(url, opts);
  await client.start();
  return client;
}
