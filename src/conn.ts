/**
 * MixerConn: the per-connection ws-mixer.v1 protocol engine. Owns the
 * handshake, the read-dispatch loop, the control-priority + round-robin
 * write scheduler (OVERVIEW.md section 2.6 rule 3), keepalive and drain.
 * The JS SDK is always the answering peer (client): it never calls
 * OpenStream, only receives OPEN from the server.
 *
 * Mirrors `go/wsmixer/conn.go`, `dispatch.go`, `sched.go`, `keepalive.go` and
 * `drain.go`, adapted to Node's single-threaded event loop (no goroutines:
 * one write scheduler driven by a wake/notify queue instead of channels).
 */
import { EventEmitter } from "node:events";
import {
  type AgentInfo,
  type ControlMessage,
  type DrainMsg,
  type WelcomeMsg,
  encodeControl,
  knownDrainReason,
  parseControl,
} from "./control.js";
import { ConnError, ErrorCode, StreamError, WsMixerError, closeCode } from "./errors.js";
import {
  FrameType,
  decodeFrame,
  encodeData,
  encodeReset,
  frameTypeName,
  resetCode,
  resetMessage,
  windowIncrement,
} from "./frame.js";
import { MixerStream, type StreamHost } from "./stream.js";
import { truncateUtf8 } from "./util.js";

/** Structural subset of `ws`'s WebSocket that MixerConn depends on (keeps the transport swappable). */
export interface WSLike {
  readonly readyState: number;
  send(data: Uint8Array, cb?: (err?: Error) => void): void;
  close(code?: number, reason?: string): void;
  terminate?(): void;
  on(event: "message", listener: (data: Uint8Array | Buffer, isBinary: boolean) => void): void;
  on(event: "close", listener: (code: number, reason: Buffer) => void): void;
  on(event: "error", listener: (err: Error) => void): void;
}

export interface ConnOptions {
  token: string;
  agent: AgentInfo;
  meta?: unknown;
  window?: number;
  maxStreams?: number;
  capabilities?: string[];
  /** Timeout waiting for `welcome` after sending `hello`. Default 10000ms per OVERVIEW.md section 2.9. */
  helloTimeoutMs?: number;
  /**
   * @internal Test-only: lets a test's `welcome` use keepalive values below
   * the wire floors (OVERVIEW.md section 2.9: `ping_interval` floor 5000ms,
   * `ping_timeout` >= 2x `ping_interval`) so keepalive/sequence tests can run
   * with real timers in well under a second. Never set this in production.
   */
  _timing?: {
    minPingInterval?: number;
    minPingTimeout?: number;
    helloTimeout?: number;
  };
}

const DEFAULT_WINDOW = 262144;
const DEFAULT_MAX_STREAMS = 64;
const DEFAULT_HELLO_TIMEOUT_MS = 10000;
const DEFAULT_PING_INTERVAL_FLOOR_MS = 5000;
/** Stream-0 flood limit token bucket defaults, mirroring go/wsmixer's Options.Stream0RateLimit/Stream0Burst. */
const STREAM0_RATE_PER_SEC = 50;
const STREAM0_BURST = 100;
/** Delivery queue capacity floor, mirroring go/wsmixer's deliveryQueueSize. */
const DELIVERY_QUEUE_MIN = 128;
/** Added on top of maxStreams when that alone would exceed DELIVERY_QUEUE_MIN, mirroring deliveryQueueMargin. */
const DELIVERY_QUEUE_MARGIN = 64;

/**
 * Clamps a would-be WS close code to one actually legal to send on the wire
 * -- 1000, or 4000-4999, pass through unchanged; anything else (an
 * application-layer error code >= 0x1000_0000 is a legal RESET code,
 * errors.ts, but was never a legal *connection*-close code) is clamped to
 * INTERNAL_ERROR's mapped code (4002) instead, mirroring go/wsmixer's
 * wsCloseCode (conn.go). `ws`'s Sender.close() throws a RangeError for an
 * out-of-range code rather than sending nothing, which this file's own
 * catch would otherwise turn into `ws.terminate()` -- a bare 1006 abnormal
 * closure that tells the peer nothing at all. `error{}` (sent separately,
 * unclamped) still carries the real code either way, and the *reported*
 * `wsCode` (the 'close' event below, DisconnectReason) stays the real
 * 4000+code too -- only the bytes actually put on the wire are clamped.
 */
function wireCloseCode(wsCode: number): number {
  return wsCode === 1000 || (wsCode >= 4000 && wsCode <= 4999) ? wsCode : 4000 + ErrorCode.INTERNAL_ERROR;
}

interface OutboxItem {
  chunk: Uint8Array;
  resolve: () => void;
  reject: (err: Error) => void;
}

/** One queued control-channel (stream-0) frame, paired with the promise that resolves once it's actually written. */
interface ControlItem {
  frame: Uint8Array;
  resolve: () => void;
  reject: (err: Error) => void;
}

/** One OnStream/OnApp/OnDrain invocation queued for the delivery loop, in wire order. Mirrors go/wsmixer's deliveryEvent. */
type DeliveryEvent =
  | { kind: "stream"; stream: MixerStream }
  | { kind: "app"; body: Record<string, unknown> }
  | { kind: "drain"; msg: DrainMsg };

/** A small token bucket rate limiter (no locking needed: Node's single-threaded), mirroring go/wsmixer's tokenBucket (conn.go). */
class TokenBucket {
  private tokens: number;
  private last = Date.now();
  constructor(
    private readonly capacity: number,
    private readonly ratePerSecond: number,
  ) {
    this.tokens = capacity;
  }

  /** Reports whether one token is available and, if so, consumes it. */
  allow(): boolean {
    const now = Date.now();
    this.tokens = Math.min(this.capacity, this.tokens + ((now - this.last) / 1000) * this.ratePerSecond);
    this.last = now;
    if (this.tokens < 1) return false;
    this.tokens -= 1;
    return true;
  }
}

/**
 * Fires (and clears) once for every welcome/close event; used for the
 * `welcome`-then-timeout handshake race and to unblock `close()`'s grace wait.
 */
export declare interface MixerConn {
  on(event: "welcome", listener: (welcome: WelcomeMsg) => void): this;
  on(event: "stream", listener: (stream: MixerStream) => void): this;
  on(event: "app", listener: (body: Record<string, unknown>) => void): this;
  on(event: "drain", listener: (msg: DrainMsg) => void): this;
  on(event: "pong", listener: (info: { id: number; rttMs: number }) => void): this;
  on(event: "error", listener: (err: WsMixerError) => void): this;
  on(event: "fatal", listener: (err: WsMixerError) => void): this;
  on(
    event: "close",
    listener: (info: { wsCode: number; errorCode?: number; message: string; closeReason?: string }) => void,
  ): this;
  /** Fires when a `'stream'`/`'app'`/`'drain'` listener throws or rejects; delivery continues with the next queued event regardless (see stats().handlerErrors). */
  on(event: "handlerError", listener: (info: { event: string; error: Error }) => void): this;
}

/** One live, handshaken (or handshaking) ws-mixer connection. */
export class MixerConn extends EventEmitter {
  private readonly ws: WSLike;
  private readonly opts: ConnOptions;

  private closed = false;
  private handshakeDone = false;
  session = "";
  peerWindow = DEFAULT_WINDOW;
  ourWindow: number;
  maxStreams = DEFAULT_MAX_STREAMS;
  pingIntervalMs = 30000;
  pingTimeoutMs = 90000;
  welcomeMeta: unknown;
  private draining = false;
  /** The `last_stream_id` from the most recently received `drain`; set only once draining. */
  private lastStreamId?: number;

  private readonly streams = new Map<number, MixerStream>();
  private highestOpened = 0;

  /**
   * Minimal "ignore and count" counters (item 11 of the review: full
   * escalation/STREAM_LIMIT bucketing is intentionally out of scope for v1 --
   * see README.md and OVERVIEW.md section 4). Exposed via `stats()`.
   */
  private readonly counters = {
    unknownFrameTypes: 0,
    staleFrames: 0,
    duplicatePongs: 0,
    refusedOpens: 0,
    /**
     * OPENs received above the drain's `last_stream_id` (OVERVIEW.md section
     * 2.9 / the 2026-08-27 decision log): connection-fatal `PROTOCOL_ERROR`,
     * not a stream-scoped refusal -- the server already promised not to send
     * one, so this is it breaking that promise, not a benign race.
     */
    drainViolations: 0,
    /** `drain.reason` values outside the closed enum, normalized to "maintenance" (OVERVIEW.md section 2.7). */
    unknownDrainReasons: 0,
    /** A `'stream'`/`'app'`/`'drain'` listener that threw or rejected; delivery continued with the next event regardless. */
    handlerErrors: 0,
    /** ConnError-triggered connection failures: malformed/out-of-sequence control traffic (OVERVIEW.md section 4). */
    protocolViolations: 0,
    /**
     * Raw WS message bytes received/sent, cumulative (OVERVIEW.md section
     * 4): the full ws-mixer frame on the wire, header included, for every
     * message -- not just DATA payload bytes. This differs from Go's
     * `BytesTransferred` metric, which counts payload only; do not compare
     * the two directly.
     */
    bytesIn: 0,
    bytesOut: 0,
  };

  /** Snapshot of the counters above. */
  stats(): Readonly<typeof this.counters> {
    return { ...this.counters };
  }

  /**
   * The `StreamHost` a `MixerStream` actually talks to: a plain object of
   * bound closures, not `this` -- so `sendData`/`sendControlFrame`/
   * `retireStream` stay private implementation details of MixerConn instead
   * of leaking onto its public (and `.d.ts`) surface (item 8).
   */
  readonly #streamHost: StreamHost = {
    sendData: (streamId, chunk) => this.sendData(streamId, chunk),
    sendControlFrame: (frame) => this.sendControlFrame(frame),
    retireStream: (streamId) => this.retireStream(streamId),
  };

  // --- write scheduler state ---
  private readonly controlQueue: ControlItem[] = [];
  private readonly rotation: number[] = [];
  private readonly inRotation = new Set<number>();
  private readonly outbox = new Map<number, OutboxItem[]>();
  private wakeWaiters: Array<() => void> = [];
  private writerRunning = false;
  /** Resolved once the stream table is empty; used by close() instead of polling. */
  private idleWaiters: Array<() => void> = [];

  // --- ordered async delivery (item 2): stream/app/drain handlers fire in wire
  // order, off one queue, so a slow handler cannot stall frame parsing. ---
  private readonly deliveryQueue: DeliveryEvent[] = [];
  private deliveryRunning = false;

  // --- stream-0 flood limit (item 4), mirrors go/wsmixer's Conn.stream0Bucket ---
  private readonly stream0Bucket = new TokenBucket(STREAM0_BURST, STREAM0_RATE_PER_SEC);

  // --- keepalive state ---
  private nextPingId = 0;
  /** Watermark: every id below this has been acked (or pruned as stale) at least once. Mirrors go/wsmixer's Conn.lowestUnacked. */
  private lowestUnacked = 0;
  private lastPongAt = 0;
  private readonly outstandingPings = new Map<number, number>();
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private watchdogTimer: ReturnType<typeof setInterval> | null = null;
  private handshakeTimer: ReturnType<typeof setTimeout> | null = null;
  private jitterTimer: ReturnType<typeof setTimeout> | null = null;
  /**
   * The most recent pre-welcome `'error'` event's message, recorded so
   * `onSocketClose`'s report -- the sole reporter, see its own doc comment
   * -- can use it as `message` when `ws`'s own close carries no reason of
   * its own.
   */
  private pendingHandshakeErrorMessage: string | undefined;

  constructor(ws: WSLike, opts: ConnOptions) {
    super();
    this.ws = ws;
    this.opts = opts;
    this.ourWindow = opts.window ?? DEFAULT_WINDOW;
    this.maxStreams = opts.maxStreams ?? DEFAULT_MAX_STREAMS;
    ws.on("message", (data) => this.onMessage(data));
    ws.on("close", (code, reason) => this.onSocketClose(code, reason));
    ws.on("error", (err) => this.onSocketError(err));
  }

  // --- handshake --------------------------------------------------------------

  /** Sends `hello` and resolves once `welcome` is validated, or rejects (a WsMixerError, `fatal` set for UNSUPPORTED). */
  handshake(): Promise<WelcomeMsg> {
    return new Promise((resolve, reject) => {
      const hello: ControlMessage = {
        t: "hello",
        v: 1,
        token: this.opts.token,
        agent: this.opts.agent,
        window: this.ourWindow,
        max_streams: this.maxStreams,
        ...(this.opts.capabilities ? { capabilities: this.opts.capabilities } : {}),
        ...(this.opts.meta !== undefined ? { meta: this.opts.meta } : {}),
      } as ControlMessage;

      const timeoutMs = this.opts.helloTimeoutMs ?? this.opts._timing?.helloTimeout ?? DEFAULT_HELLO_TIMEOUT_MS;
      this.handshakeTimer = setTimeout(() => {
        this.handshakeTimer = null;
        const err = new ConnError(ErrorCode.PROTOCOL_ERROR, `no welcome within ${timeoutMs}ms of hello`);
        this.fail(err);
        reject(err);
      }, timeoutMs);

      this.once("__welcome_internal", (welcome: WelcomeMsg) => {
        if (this.handshakeTimer) {
          clearTimeout(this.handshakeTimer);
          this.handshakeTimer = null;
        }
        resolve(welcome);
      });
      this.once("__handshake_failed_internal", (err: WsMixerError) => {
        if (this.handshakeTimer) {
          clearTimeout(this.handshakeTimer);
          this.handshakeTimer = null;
        }
        reject(err);
      });

      // Fire-and-forget here: a write failure surfaces via
      // __handshake_failed_internal (onSocketClose/onSocketError/fail all
      // emit it), so this promise's rejection would just be redundant.
      void this.enqueueControl(encodeData(0, encodeControl(hello))).catch(() => {});
      this.startWriter();
    });
  }

  private applyWelcome(welcome: WelcomeMsg): void {
    const minInterval = this.opts._timing?.minPingInterval ?? DEFAULT_PING_INTERVAL_FLOOR_MS;
    if (welcome.ping_interval < minInterval) {
      throw new ConnError(ErrorCode.PROTOCOL_ERROR, `welcome.ping_interval ${welcome.ping_interval} is below the ${minInterval}ms floor`);
    }
    const minTimeout = this.opts._timing?.minPingTimeout ?? 2 * welcome.ping_interval;
    if (welcome.ping_timeout < minTimeout) {
      throw new ConnError(
        ErrorCode.PROTOCOL_ERROR,
        `welcome.ping_timeout (${welcome.ping_timeout}) must be at least ${minTimeout}ms`,
      );
    }
    this.session = welcome.session;
    this.peerWindow = welcome.window;
    this.maxStreams = this.opts.maxStreams
      ? Math.min(this.opts.maxStreams, welcome.max_streams)
      : welcome.max_streams;
    this.pingIntervalMs = welcome.ping_interval;
    this.pingTimeoutMs = welcome.ping_timeout;
    this.welcomeMeta = welcome.meta;
    this.handshakeDone = true;
    this.lastPongAt = Date.now();
    this.emit("welcome", welcome);
    this.emit("__welcome_internal", welcome);
    this.startKeepalive();
  }

  // --- read dispatch (never blocks on application code) ------------------------

  private onMessage(data: Uint8Array | Buffer): void {
    this.counters.bytesIn += data.length;
    try {
      this.dispatchFrame(data instanceof Uint8Array ? data : new Uint8Array(data));
    } catch (e) {
      this.handleDispatchError(e);
    }
  }

  private handleDispatchError(e: unknown): void {
    if (e instanceof ConnError) {
      this.counters.protocolViolations++;
      this.fail(e);
      return;
    }
    if (e instanceof StreamError) {
      const stream = this.streams.get(e.streamId!);
      if (stream) {
        // The SDK itself detected this violation (not application code), so
        // abort() -- not the app-facing reset() -- both tears the stream down
        // and emits 'reset', the same way a peer-sent RESET would (mirrors
        // handleReset()): the app didn't choose this, so it needs to hear
        // about it the same way.
        stream.abort(e.code, e.message);
      } else {
        // The stream is already gone locally (e.g. a race with retireStream),
        // but the peer still needs to hear RESET for it -- mirrors Go's
        // resetStream (dispatch.go), which always sends regardless of
        // whether its local Stream struct still exists.
        this.sendControlFrame(encodeReset(e.streamId!, e.code, e.message));
      }
      return;
    }
    this.fail(new ConnError(ErrorCode.INTERNAL_ERROR, e instanceof Error ? e.message : String(e)));
  }

  private dispatchFrame(msg: Uint8Array): void {
    const frame = decodeFrame(msg);

    const known = Object.values(FrameType).includes(frame.type as never);
    if (!known) {
      this.counters.unknownFrameTypes++;
      return;
    }

    if (frame.streamId === 0) {
      if (frame.type !== FrameType.DATA) {
        // decodeFrame already rejects OPEN/CLOSE/WINDOW/RESET on stream 0.
        return;
      }
      this.dispatchControl(frame.payload);
      return;
    }

    if (!this.handshakeDone) {
      // "The server may send nothing at all before welcome" (OVERVIEW.md
      // section 2.9): any non-stream-0 frame here means the server jumped
      // the gun on the handshake.
      throw new ConnError(ErrorCode.PROTOCOL_ERROR, `${frameTypeName(frame.type)} frame received before welcome completed the handshake`);
    }

    const id = frame.streamId;
    if (frame.type === FrameType.OPEN) {
      if (id <= this.highestOpened) {
        throw new ConnError(ErrorCode.PROTOCOL_ERROR, `duplicate or out-of-order OPEN for stream ${id}`);
      }
      this.highestOpened = id;
      // Drain enforcement (OVERVIEW.md section 2.9 and the 2026-08-27
      // decision log): "endpoints MUST NOT increase last_stream_id" and
      // "after sending drain the server MUST NOT send OPEN". An OPEN above
      // the announced last_stream_id is the server breaking a boundary it
      // already promised -- strict connection PROTOCOL_ERROR, not a
      // stream-scoped REFUSED_STREAM (only the server opens streams, so only
      // the client is ever in a position to detect this).
      if (this.draining && this.lastStreamId !== undefined && id > this.lastStreamId) {
        this.counters.drainViolations++;
        throw new ConnError(
          ErrorCode.PROTOCOL_ERROR,
          `OPEN for stream ${id} received after drain (last_stream_id=${this.lastStreamId})`,
        );
      }
      // OPEN beyond the negotiated max_streams: stream-scoped STREAM_LIMIT,
      // not a connection error (OVERVIEW.md section 2.8's error table). Only
      // the server opens streams, so only the client is ever in a position
      // to detect this (mirrors spec/fixtures/sequences/max_streams_exceeded.json).
      if (this.streams.size >= this.maxStreams) {
        this.counters.refusedOpens++;
        this.sendControlFrame(encodeReset(id, ErrorCode.STREAM_LIMIT, `max_streams=${this.maxStreams} exceeded by stream ${id}`));
        return;
      }
      const stream = new MixerStream(id, this.#streamHost, this.ourWindow, this.peerWindow);
      this.streams.set(id, stream);
      this.enqueueDelivery({ kind: "stream", stream });
      return;
    }

    if (id > this.highestOpened) {
      throw new ConnError(ErrorCode.PROTOCOL_ERROR, `${frame.type === FrameType.DATA ? "DATA" : "frame"} for stream ${id}, which was never opened`);
    }

    const stream = this.streams.get(id);
    if (!stream) {
      // Benign race: frames for a stream that was open and is now gone.
      this.counters.staleFrames++;
      return;
    }

    switch (frame.type) {
      case FrameType.DATA:
        stream.handleData(frame.payload);
        break;
      case FrameType.WINDOW:
        stream.handleWindow(windowIncrement(frame));
        break;
      case FrameType.CLOSE:
        stream.handleClose();
        break;
      case FrameType.RESET:
        stream.handleReset(resetCode(frame), resetMessage(frame));
        break;
    }
  }

  private dispatchControl(payload: Uint8Array): void {
    // Stream-0 flood limit (item 4, OVERVIEW.md section 2.7): checked before
    // parsing, mirroring go/wsmixer's handleControlData, so a flood of junk
    // can't burn CPU on top of exhausting the bucket.
    if (!this.stream0Bucket.allow()) {
      throw new ConnError(ErrorCode.ENHANCE_YOUR_CALM, `stream-0 message rate exceeded ${STREAM0_RATE_PER_SEC}/s (burst ${STREAM0_BURST})`);
    }
    const msg = parseControl(payload);
    switch (msg.t) {
      case "welcome":
        if (this.handshakeDone) throw new ConnError(ErrorCode.PROTOCOL_ERROR, "duplicate welcome after handshake completed");
        this.applyWelcome(msg);
        return;
      case "hello":
        throw new ConnError(ErrorCode.PROTOCOL_ERROR, "unexpected hello: only the client sends hello");
      case "ping":
        // Item 6: a ping before welcome completes is treated like any other
        // pre-welcome control frame (OVERVIEW.md sections 2.9/2.10: "the
        // server may send nothing at all before welcome").
        if (!this.handshakeDone) {
          throw new ConnError(ErrorCode.PROTOCOL_ERROR, "ping received before welcome completed the handshake");
        }
        // pong jumps to the head of the control queue (item 6 / OVERVIEW.md
        // section 2.7: "A pong MUST ... jump ahead of queued DATA").
        void this.sendControlPriority({ t: "pong", id: msg.id, ...(msg.ts !== undefined ? { ts: msg.ts } : {}) } as ControlMessage).catch(() => {});
        return;
      case "pong":
        // OVERVIEW.md section 2.7 defines two verdicts for an incoming pong
        // -- id never sent -> PROTOCOL_ERROR, duplicate -> ignore + count --
        // and nothing beyond "MUST carry the same id and ts" about a
        // mismatched `ts` specifically. RTT here is measured off this side's
        // own recorded send time (handlePong's `sentAt`), never off the
        // peer's echoed `ts`, so a wrong echo can't skew it; treating it as
        // fatal isn't required by the spec and would misidentify a
        // clock-skewed-but-otherwise-healthy peer as a protocol violation
        // (confirmed by spec/fixtures/sequences/duplicate_pong_from_server.json,
        // which replays a pong carrying a fixed, unrelated `ts` and expects
        // no error). `ts` is therefore accepted but not verified; only `id`
        // governs match/duplicate detection.
        this.handlePong(msg.id);
        return;
      case "drain": {
        // Item 1: like ping/app above, drain before welcome completes the
        // handshake is a protocol violation (OVERVIEW.md sections 2.9/2.10:
        // "the server may send nothing at all before welcome"), mirroring
        // Go dispatch.go's blanket !handshakeDone check.
        if (!this.handshakeDone) {
          throw new ConnError(ErrorCode.PROTOCOL_ERROR, "drain received before welcome completed the handshake");
        }
        // Unknown reason: tolerated, degrade to "maintenance" and count
        // (OVERVIEW.md section 2.7) -- both for the connection's own
        // draining decision and for whatever the app sees via the 'drain'
        // event, so a consumer never has to recognize a reason outside the
        // documented closed enum.
        let reason = msg.reason;
        if (!knownDrainReason(reason)) {
          this.counters.unknownDrainReasons++;
          reason = "maintenance";
        }
        const normalized: DrainMsg = reason === msg.reason ? msg : { ...msg, reason };
        this.draining = true;
        // Endpoints MUST NOT increase last_stream_id across drains
        // (OVERVIEW.md section 2.9); keep the smaller value if we somehow
        // saw more than one.
        this.lastStreamId = this.lastStreamId === undefined ? normalized.last_stream_id : Math.min(this.lastStreamId, normalized.last_stream_id);
        this.enqueueDelivery({ kind: "drain", msg: normalized });
        return;
      }
      case "error":
        // The normative pre-welcome rejection shape (OVERVIEW.md section
        // 3.4's Authenticate hook, WIRE.md section 2.7/2.10 step 12): the
        // server MAY reject `hello` with `error{code,message}` + close
        // *before* ever sending `welcome` -- confirmed by
        // spec/fixtures/sequences/auth_failure.json (error{UNAUTHORIZED} +
        // close 4011, no welcome). Unlike drain/ping/app/hello above, this is
        // never a protocol violation on the peer's part even pre-welcome, so
        // it always routes to handlePeerError -- which never replies with
        // another error{} (WIRE.md section 2.7) and closes with 4000+code --
        // surfacing the peer's own code/message to the handshake rejection
        // instead of masking it as a local PROTOCOL_ERROR.
        this.handlePeerError(msg.code, msg.message, msg.stream_id);
        return;
      case "app":
        if (!this.handshakeDone) throw new ConnError(ErrorCode.PROTOCOL_ERROR, "app message received before welcome");
        this.enqueueDelivery({ kind: "app", body: msg.body });
        return;
    }
  }

  // --- ordered async delivery (item 2) -------------------------------------

  /**
   * Queues one `'stream'`/`'app'`/`'drain'` event for in-order, async
   * delivery, mirroring go/wsmixer's deliveryLoop: the read/dispatch path
   * above never blocks on application code, but all three still fire in
   * wire order, off a single loop, one at a time. Handlers registered via
   * `on('stream'|'app'|'drain', ...)` must not block for long -- a handler
   * that never returns/resolves permanently wedges this loop, exactly like
   * the Go side. Throws ConnError(ENHANCE_YOUR_CALM) if the bounded queue
   * (`max(128, max_streams + 64)`) is full, which the caller (dispatchFrame/
   * dispatchControl, inside onMessage's try/catch) turns into fail().
   *
   * Refuses once `this.closed` (CLIENT-SDK.md's "Handler delivery" row):
   * `dispatchFrame`/`dispatchControl` -- the only callers -- always run
   * before whatever ends the connection sets `this.closed` (every
   * fail()/close()/handlePeerError path runs teardownConn synchronously,
   * `error` is always the last message on the wire), so nothing legitimate
   * should ever reach here once closed; this is the defensive backstop that
   * makes runDeliveryLoop's post-close drain below provably bounded and
   * terminating regardless.
   */
  private enqueueDelivery(ev: DeliveryEvent): void {
    if (this.closed) return;
    const capacity = Math.max(DELIVERY_QUEUE_MIN, this.maxStreams + DELIVERY_QUEUE_MARGIN);
    if (this.deliveryQueue.length >= capacity) {
      throw new ConnError(ErrorCode.ENHANCE_YOUR_CALM, `application delivery queue full (>${capacity} pending stream/app/drain callbacks)`);
    }
    this.deliveryQueue.push(ev);
    if (!this.deliveryRunning) {
      this.deliveryRunning = true;
      void this.runDeliveryLoop();
    }
  }

  /**
   * Drains `deliveryQueue` until empty -- deliberately NOT gated on
   * `!this.closed` (CLIENT-SDK.md's "Handler delivery" row): an event
   * already queued when the connection ends is still owed to its handler
   * (`error` is the last message on the wire, so a stream OPEN/`app`/`drain`
   * queued ahead of it arrived before the connection ended), so this loop
   * finishes flushing that backlog even after `teardownConn` has already set
   * `this.closed` and emitted `'close'` -- a handler MAY therefore run
   * shortly after `MixerClient.close()`/`MixerConn.close()`'s own promise has
   * already resolved (see those methods' doc comments and README). `if
   * (!ev) break` is what actually ends the loop; since `enqueueDelivery`
   * above refuses once closed, the queue can only shrink from here, so this
   * always terminates.
   */
  private async runDeliveryLoop(): Promise<void> {
    for (;;) {
      const ev = this.deliveryQueue.shift();
      if (!ev) break;
      switch (ev.kind) {
        case "stream":
          await this.emitOrdered("stream", ev.stream);
          break;
        case "app":
          await this.emitOrdered("app", ev.body);
          break;
        case "drain":
          await this.emitOrdered("drain", ev.msg);
          break;
      }
    }
    this.deliveryRunning = false;
  }

  /**
   * Invokes every listener for `event` in registration order, awaiting each
   * one's return value before starting the next -- unlike EventEmitter's own
   * synchronous `emit`, which fires listeners back to back without waiting
   * for a returned Promise to settle.
   *
   * A listener that throws synchronously or returns a rejected promise is
   * caught here, counted (stats().handlerErrors) and surfaced via a guarded
   * `'handlerError'` emit -- never an unhandled rejection, and never fatal
   * to delivery: the next queued stream/app/drain event still runs.
   */
  private async emitOrdered(event: string, arg: unknown): Promise<void> {
    for (const listener of this.listeners(event) as Array<(a: unknown) => unknown>) {
      try {
        const result = listener.call(this, arg);
        if (result instanceof Promise) await result;
      } catch (e) {
        this.counters.handlerErrors++;
        const error = e instanceof Error ? e : new Error(String(e));
        if (this.listenerCount("handlerError") > 0) this.emit("handlerError", { event, error });
      }
    }
  }

  // --- outbound: control priority + round-robin DATA scheduler ------------------

  /**
   * Queues one control-channel frame and returns a promise that resolves
   * once `ws.send`'s callback confirms it was actually written (or rejects
   * with the connection's terminal error if the conn fails first) --
   * mirroring how `sendData()`'s per-stream outbox already works.
   */
  private enqueueControl(frame: Uint8Array, priority = false): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(new ConnError(ErrorCode.INTERNAL_ERROR, "connection closed"));
        return;
      }
      const item: ControlItem = { frame, resolve, reject };
      if (priority) this.controlQueue.unshift(item);
      else this.controlQueue.push(item);
      this.wake();
    });
  }

  /**
   * Sends a control message (hello/ping/pong/drain/app/error) as stream-0
   * DATA, queued ahead of any DATA. Resolves once the frame is actually
   * written to the socket.
   */
  sendControl(msg: ControlMessage): Promise<void> {
    return this.enqueueControl(encodeData(0, encodeControl(msg)));
  }

  /**
   * Sends a control message at the head of the control queue, ahead of
   * anything already queued (item 6: "A pong MUST ... jump ahead of queued
   * DATA" -- and ahead of other queued control frames too).
   */
  private sendControlPriority(msg: ControlMessage): Promise<void> {
    return this.enqueueControl(encodeData(0, encodeControl(msg)), true);
  }

  /** Backs `streamHost.sendControlFrame`: send a control-priority frame (WINDOW/CLOSE/RESET) immediately. */
  private sendControlFrame(frame: Uint8Array): void {
    void this.enqueueControl(frame).catch(() => {
      // A stream-level WINDOW/CLOSE/RESET write failing means the connection
      // is already going down; that failure is reported through the normal
      // 'close'/'fatal' path, not here (this method's callers, StreamHost's
      // fire-and-forget contract, don't await it).
    });
  }

  /** Backs `streamHost.sendData`: enqueue a DATA chunk for round-robin transmission. */
  private sendData(streamId: number, chunk: Uint8Array): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(new ConnError(ErrorCode.INTERNAL_ERROR, "connection closed"));
        return;
      }
      let q = this.outbox.get(streamId);
      if (!q) {
        q = [];
        this.outbox.set(streamId, q);
      }
      q.push({ chunk, resolve, reject });
      this.markReady(streamId);
    });
  }

  /** Backs `streamHost.retireStream`: retire a fully-closed stream's id. */
  private retireStream(streamId: number): void {
    this.streams.delete(streamId);
    this.outbox.delete(streamId);
    if (this.streams.size === 0) this.notifyIdle();
  }

  /** Resolves every pending close()'s wait for the stream table to empty, instead of polling. */
  private notifyIdle(): void {
    const waiters = this.idleWaiters;
    this.idleWaiters = [];
    for (const w of waiters) w();
  }

  private waitForEmpty(): Promise<void> {
    return new Promise((resolve) => {
      if (this.streams.size === 0) {
        resolve();
        return;
      }
      this.idleWaiters.push(resolve);
    });
  }

  private markReady(streamId: number): void {
    if (!this.inRotation.has(streamId)) {
      this.inRotation.add(streamId);
      this.rotation.push(streamId);
    }
    this.wake();
  }

  private wake(): void {
    const waiters = this.wakeWaiters;
    this.wakeWaiters = [];
    for (const w of waiters) w();
  }

  private waitForWork(): Promise<void> {
    return new Promise((resolve) => this.wakeWaiters.push(resolve));
  }

  private nextChunk(): { streamId: number; item: OutboxItem } | null {
    while (this.rotation.length > 0) {
      const id = this.rotation.shift()!;
      this.inRotation.delete(id);
      const q = this.outbox.get(id);
      if (!q || q.length === 0) continue;
      const item = q.shift()!;
      const stream = this.streams.get(id);
      if (stream) {
        const { done, error } = stream.sendDone();
        if (done) {
          item.reject(error!);
          continue;
        }
      }
      if (q.length > 0) this.markReady(id);
      return { streamId: id, item };
    }
    return null;
  }

  private startWriter(): void {
    if (this.writerRunning) return;
    this.writerRunning = true;
    void this.runWriter();
  }

  private async runWriter(): Promise<void> {
    while (!this.closed) {
      while (this.controlQueue.length > 0) {
        const item = this.controlQueue.shift()!;
        try {
          await this.writeRaw(item.frame);
          item.resolve();
        } catch (e) {
          item.reject(e as Error);
          return; // socket failure: onSocketClose/onSocketError handles teardown
        }
      }
      const next = this.nextChunk();
      if (next) {
        try {
          await this.writeRaw(encodeData(next.streamId, next.item.chunk));
          next.item.resolve();
        } catch (e) {
          next.item.reject(e as Error);
          return;
        }
        continue;
      }
      await this.waitForWork();
    }
  }

  /**
   * Writes one frame and resolves once `ws.send`'s callback reports it
   * flushed. This is how rule 2 (§2.6, "gate the write loop on the socket
   * send buffer") is satisfied without polling `bufferedAmount` on a timer:
   * `ws`'s `send(data, cb)` callback fires only once the chunk has actually
   * been handed to the socket, so awaiting it before writing the next frame
   * already keeps exactly one write in flight and applies backpressure for
   * free (runWriter's loop awaits writeRaw() before picking the next chunk).
   */
  private writeRaw(frame: Uint8Array): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(new Error("connection closed"));
        return;
      }
      try {
        this.ws.send(frame, (err) => {
          if (err) {
            reject(err);
            return;
          }
          this.counters.bytesOut += frame.length;
          resolve();
        });
      } catch (e) {
        reject(e as Error);
      }
    });
  }

  // --- app messages -------------------------------------------------------------

  /** Resolves once the `app` frame is actually written to the socket, or rejects with the connection's terminal error if it fails first. */
  sendApp(body: Record<string, unknown>): Promise<void> {
    return this.sendControl({ t: "app", body } as ControlMessage);
  }

  // --- keepalive ------------------------------------------------------------------

  private startKeepalive(): void {
    // First ping jittered by random(0, ping_interval) to avoid a herd after a mass reconnect.
    const jitter = Math.random() * this.pingIntervalMs;
    this.jitterTimer = setTimeout(() => {
      this.jitterTimer = null;
      if (this.closed) return;
      this.sendPing();
      this.pingTimer = setInterval(() => this.sendPing(), this.pingIntervalMs);
    }, jitter);
    // Checked at most every 1000ms in production, but no less often than
    // ping_timeout itself so a test-injected tiny ping_timeout (see
    // ConnOptions._timing) is still detected promptly instead of waiting out
    // a fixed 1s tick.
    this.watchdogTimer = setInterval(() => this.checkWatchdog(), Math.min(1000, this.pingTimeoutMs));
  }

  private sendPing(): void {
    if (this.closed) return;
    const id = this.nextPingId++;
    this.outstandingPings.set(id, Date.now());
    void this.sendControl({ t: "ping", id, ts: Date.now() } as ControlMessage).catch(() => {});
  }

  /**
   * Pong watermark scheme, mirroring go/wsmixer/dispatch.go's handlePong:
   * every id below lowestUnacked has been acked (or pruned as stale) at
   * least once, and no id >= nextPingId has ever been sent.
   *  - id >= nextPingId: never sent -> PROTOCOL_ERROR, connection-fatal.
   *  - id < lowestUnacked, or in-range but already absent from the map
   *    (already acked, or pruned by the watchdog as stale/late): a
   *    duplicate or late pong -- tolerated, just counted.
   */
  private handlePong(id: number): void {
    if (id >= this.nextPingId) {
      throw new ConnError(ErrorCode.PROTOCOL_ERROR, `pong for id ${id} that was never sent`);
    }
    if (id < this.lowestUnacked) {
      this.counters.duplicatePongs++;
      return;
    }
    const sentAt = this.outstandingPings.get(id);
    if (sentAt === undefined) {
      this.counters.duplicatePongs++;
      return;
    }
    this.outstandingPings.delete(id);
    if (id === this.lowestUnacked) {
      this.lowestUnacked++;
      while (this.lowestUnacked < this.nextPingId && !this.outstandingPings.has(this.lowestUnacked)) {
        this.lowestUnacked++;
      }
    }
    this.lastPongAt = Date.now();
    this.emit("pong", { id, rttMs: Date.now() - sentAt });
  }

  private checkWatchdog(): void {
    if (this.closed) return;
    const elapsed = Date.now() - this.lastPongAt;
    for (const [id, sentAt] of this.outstandingPings) {
      if (Date.now() - sentAt > this.pingTimeoutMs) this.outstandingPings.delete(id);
    }
    if (elapsed > this.pingTimeoutMs) {
      this.fail(new ConnError(ErrorCode.KEEPALIVE_TIMEOUT, `no pong received for ${elapsed}ms`));
    }
  }

  // --- shutdown -----------------------------------------------------------------

  /**
   * Shared teardown: WS close `4000 + code`, reject everything outstanding,
   * emit `'fatal'`/`'error'` (guarded) then `'close'`, synchronously and in
   * that order every time. `sendErrorFrame` controls whether an
   * `error{code,message}` control frame is sent first -- true for a
   * locally-detected failure (fail()), false when the peer already sent its
   * own `error` and OVERVIEW.md section 2.7 forbids replying with another
   * one (handlePeerError()). Either way `this.closed` is set *before*
   * `ws.close()` is called, so the peer's echo of this close (or any close
   * frame it happens to send around the same time) is ignored by
   * onSocketClose below rather than reported as `closeReason` -- per
   * CLIENT-SDK.md's `closeReason` row, an outgoing reason this side sent is
   * never legitimate `closeReason` data, and there is nothing to gain by
   * waiting for the peer's own close frame here: OVERVIEW.md section 2.7
   * already allows closing on `error` "without reading the close frame that
   * followed".
   */
  private teardownConn(err: WsMixerError, sendErrorFrame: boolean, streamErrorFactory?: (streamId: number) => WsMixerError): void {
    if (this.closed) return;
    this.closed = true;
    this.stopTimers();
    if (sendErrorFrame) {
      try {
        const frame = encodeData(0, encodeControl({ t: "error", code: err.code, message: err.message } as ControlMessage));
        this.ws.send(frame);
        // Best-effort like the send() call itself (no callback to confirm an
        // actual flush): still counted in bytesOut so this farewell frame
        // isn't invisible to stats() (item 3).
        this.counters.bytesOut += frame.length;
      } catch {
        // best-effort: the socket may already be gone
      }
    }
    const wsCode = closeCode(err.code);
    const reason = truncateUtf8(err.message, 123);
    try {
      this.ws.close(wireCloseCode(wsCode), reason);
    } catch {
      this.ws.terminate?.();
    }
    this.rejectOutstanding(err, streamErrorFactory);
    if (!this.handshakeDone) this.emit("__handshake_failed_internal", err);
    const fatal = err.code === ErrorCode.UNSUPPORTED || err.code === ErrorCode.UNAUTHORIZED;
    // Guarded (item 2): 'close' below always fires and carries the same
    // {errorCode, message}, so a caller with no 'error'/'fatal' listener
    // still learns why via onDisconnect(reason) instead of an uncaught throw.
    const evt = fatal ? "fatal" : "error";
    if (this.listenerCount(evt) > 0) this.emit(evt, err);
    // closeReason is deliberately absent here in both branches: this side
    // initiated the close (either directly, or -- sendErrorFrame:false, from
    // handlePeerError -- reacting to error{} without reading whatever close
    // frame the peer sends behind it, per OVERVIEW.md section 2.7). Only
    // onSocketClose, for a close frame this side actually *received*, ever
    // has a real closeReason to report.
    this.emit("close", { wsCode, errorCode: err.code, message: err.message });
  }

  /**
   * Connection-fatal failure path: error{code,message} on stream 0, WS close
   * 4000+code, teardown. Three steps, in order (OVERVIEW.md section 2.8).
   *
   * `streamErrorFactory`, when given, overrides the error every live stream
   * is torn down with (`err` is still used for the connection-level frame/
   * close/rejects) -- used by MixerClient when it fails a *superseded*
   * (drained) conn: the streams on it aren't erroring, the conn they were
   * riding on is being replaced, so each gets a stream-scoped CANCEL
   * ("connection drained") instead of the connection's own NO_ERROR.
   *
   * Synchronous, but any stream/app/drain event still queued for delivery at
   * this point is not: runDeliveryLoop keeps flushing it after this returns
   * (see close()'s doc comment above for why).
   */
  fail(err: WsMixerError, streamErrorFactory?: (streamId: number) => WsMixerError): void {
    this.teardownConn(err, true, streamErrorFactory);
  }

  /**
   * The peer sent `error{code,message}`: record it and close with
   * `4000 + code` immediately, without waiting for the peer to do anything
   * else -- mirrors go/wsmixer/dispatch.go's handlePeerError. `error` is
   * always the last message on the wire (OVERVIEW.md section 2.7), so
   * there is nothing left to negotiate.
   */
  private handlePeerError(code: number, message: string, streamId?: number): void {
    const err = new WsMixerError(code, message, { streamId });
    this.teardownConn(err, false);
  }

  /**
   * Graceful client shutdown: drain{client_requested}, brief grace period,
   * then close(NO_ERROR). This resolves once teardownConn's synchronous
   * steps are done (frame/WS close/rejects/'close' emitted) -- it does NOT
   * wait for runDeliveryLoop to finish flushing whatever stream/app/drain
   * backlog was still queued at that point. A handler for an event received
   * before this close MAY therefore still be invoked shortly after this
   * promise (and 'close') has already resolved/fired (CLIENT-SDK.md's
   * "Handler delivery" row).
   */
  async close(graceMs = 5000): Promise<void> {
    if (this.closed) return;
    if (this.handshakeDone) {
      void this.sendControl({ t: "drain", reason: "client_requested", last_stream_id: this.highestOpened } as ControlMessage).catch(() => {});
      if (this.streams.size > 0) {
        // No polling: wait for whichever happens first -- the stream table
        // draining to empty (notifyIdle, from retireStream), the grace
        // deadline, or the connection closing out from under us. Both the
        // timer and the 'close' listener are cleaned up afterwards
        // regardless of which branch won the race, so neither dangles.
        let graceTimer: ReturnType<typeof setTimeout> | undefined;
        let onClose: (() => void) | undefined;
        try {
          await Promise.race([
            this.waitForEmpty(),
            new Promise<void>((resolve) => {
              graceTimer = setTimeout(resolve, graceMs);
            }),
            new Promise<void>((resolve) => {
              onClose = () => resolve();
              this.once("close", onClose);
            }),
          ]);
        } finally {
          if (graceTimer) clearTimeout(graceTimer);
          if (onClose) this.off("close", onClose);
        }
      }
    }
    if (this.closed) return;
    this.fail(new WsMixerError(ErrorCode.NO_ERROR, "client closing"));
  }

  /**
   * A close frame this side actually *received* from `ws`'s 'close' event.
   * `code`/`reason` here are always the peer's, per `ws`'s own contract
   * (`receiverOnConclude` in `ws/lib/websocket.js` only ever updates
   * `_closeCode`/`_closeMessage` from a frame it parsed off the wire, never
   * from what this side sent via `.close()`) -- so whenever `this.closed` is
   * already `true`, this is a close frame arriving after teardownConn()
   * already ran and reported (never our own echoed close winning a race),
   * and is correctly ignored rather than folded in after the fact.
   *
   * Pre-welcome, this is also the SOLE reporter for a transport failure
   * before `welcome` -- measured against real `ws` (8.21): a TCP reset,
   * half-close or peer close frame after the 101 delivers a bare 'close'
   * with no 'error' at all, and a protocol-level failure `ws` itself
   * detects (an invalid frame: 1002, an oversized message: 1009, ...)
   * delivers 'error' immediately followed by 'close' a macrotask or more
   * later -- 'close' is never racing 'error' for which one wins; it always
   * arrives after it, if it arrives at all. `onSocketError` below therefore
   * never finalizes anything itself, only records the error's message here
   * for `message` to fall back on when the close carries no reason of its
   * own -- so this always reports the *actual* close code `ws` delivers
   * (1006, or whatever real close code it sent), never a wsCode fabricated
   * for a close that never happened.
   */
  private onSocketClose(code: number, reason: Buffer): void {
    if (this.closed) return;
    this.closed = true;
    this.stopTimers();
    const reasonStr = reason.toString();
    const handshakePhase = !this.handshakeDone;
    const message = reasonStr || (handshakePhase ? this.pendingHandshakeErrorMessage : undefined) || `socket closed with code ${code}`;
    const err = new WsMixerError(ErrorCode.INTERNAL_ERROR, message, {
      wsCode: code,
      closeReason: reasonStr || undefined,
    });
    this.rejectOutstanding(err);
    if (handshakePhase) this.emit("__handshake_failed_internal", err);
    this.emit("close", { wsCode: code, message, closeReason: reasonStr || undefined });
  }

  /**
   * Pre-welcome, `'error'` never arrives in the same microtask as (or after)
   * a 'close' that never comes -- see onSocketClose's own doc comment for
   * what real `ws` actually does. So this deliberately does NOT reject or
   * finalize the handshake itself; it just records the error's message for
   * onSocketClose (the sole reporter) to use as `message` when the close
   * that follows carries no reason of its own. If `'close'` somehow never
   * follows at all (a transport that violates `ws`'s own contract), nothing
   * here is left waiting on it: `handshake()`'s own hello/welcome timeout
   * (`handshakeTimer`, 10s default) still fires and produces the one report
   * (`wsCode` 4001) regardless -- deliberately the only fallback for that
   * case, not a second mechanism grafted on here.
   */
  private onSocketError(err: Error): void {
    if (!this.handshakeDone && !this.closed) {
      this.pendingHandshakeErrorMessage = err.message;
    }
    // Guarded (item 2): onSocketClose (or the hello/welcome timeout above)
    // still follows and emits 'close', so onDisconnect(reason) still
    // reports this even with no 'error' listener.
    if (this.listenerCount("error") > 0) this.emit("error", new WsMixerError(ErrorCode.INTERNAL_ERROR, err.message));
  }

  private stopTimers(): void {
    if (this.pingTimer) clearInterval(this.pingTimer);
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    if (this.handshakeTimer) clearTimeout(this.handshakeTimer);
    if (this.jitterTimer) clearTimeout(this.jitterTimer);
    this.pingTimer = null;
    this.watchdogTimer = null;
    this.handshakeTimer = null;
    this.jitterTimer = null;
  }

  private rejectOutstanding(err: Error, streamErrorFactory?: (streamId: number) => WsMixerError): void {
    for (const q of this.outbox.values()) {
      for (const item of q) item.reject(err);
    }
    this.outbox.clear();
    // Any control frame (including a pending sendApp()) still queued when
    // the connection dies never reaches writeRaw(), so it must be rejected
    // here instead -- otherwise its promise would hang forever.
    for (const item of this.controlQueue.splice(0, this.controlQueue.length)) {
      item.reject(err);
    }
    this.wake();
    for (const stream of this.streams.values()) {
      // Same "don't crash a consumer with no 'error' listener" rule as a
      // stream-level RESET (item 2 / stream.ts handleReset): a connection
      // failure aborts every live stream the same way -- unless the caller
      // gave a per-stream override (fail()'s streamErrorFactory), e.g. a
      // superseded conn's streams getting stream-scoped CANCEL instead of
      // the connection's own terminal error.
      stream.terminateNoThrow(
        streamErrorFactory
          ? streamErrorFactory(stream.id)
          : err instanceof WsMixerError
            ? err
            : new ConnError(ErrorCode.INTERNAL_ERROR, err.message),
      );
    }
  }

  isDraining(): boolean {
    return this.draining;
  }

  liveStreamCount(): number {
    return this.streams.size;
  }
}
