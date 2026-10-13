/**
 * MixerStream: one ws-mixer byte stream behind a small, platform-neutral,
 * promise-based API (read/write/closeWrite/reset/close/refuse/closed).
 * Mirrors `ws-mixer-go/wsmixer/stream.go`'s state machine, credit
 * accounting and half-close semantics (WIRE.md section 2.5-2.6), and
 * `ws-mixer-rust`'s `closed()`/`close_write()` semantics. The JS SDK is
 * always the answering peer: it never opens streams, only receives OPEN
 * from the server and reads/writes/closes/resets what arrives.
 *
 * How a stream ends, and what its reader/writer then sees:
 *  - CLOSE both ways (closeWrite() + the peer's CLOSE, either order):
 *    `closed` -> finished; reads drain the buffer, then null.
 *  - RESET, from the peer (handleReset), from the SDK on a stream-scoped
 *    violation (abort) or from the app (reset/close/refuse): buffered data
 *    is discarded, every pending/future read and write fails with
 *    StreamError(code); `closed` -> reset.
 *  - the connection ends first (terminate): buffered data and an
 *    already-received CLOSE are still delivered, then reads fail with the
 *    connection's error (never a clean EOF unless the peer's CLOSE had
 *    arrived); writes fail; `closed` -> connection_lost, or reset (not
 *    byPeer) when the connection hands a stream-scoped StreamError (the
 *    drain-deadline CANCEL).
 */
import { ConnError, ErrorCode, StreamError, WsMixerError, codeName, parseErrorCode } from "./errors.js";
import { MAX_CHUNK, MAX_SEND_WINDOW, encodeClose, encodeReset, encodeWindow } from "./frame.js";
import { truncateUtf8 } from "./util.js";

export type StreamState = "open" | "half_closed_local" | "half_closed_remote" | "closed";

export type StreamCloseReason =
  /** CLOSE sent and received. */
  | { kind: "finished" }
  | { kind: "reset"; code: number; name: string; message: string; byPeer: boolean }
  /** The connection ended before the stream finished or was reset. */
  | { kind: "connection_lost"; error: ConnError };

/** Callback the stream uses to hand an outbound DATA/WINDOW/CLOSE/RESET frame to the connection. */
export interface StreamHost {
  /** Enqueue a DATA chunk for round-robin transmission; resolves once handed to the socket (or rejects on failure). */
  sendData(streamId: number, chunk: Uint8Array): Promise<void>;
  /** Send a control-priority frame (WINDOW/CLOSE/RESET) immediately. */
  sendControlFrame(frame: Uint8Array): void;
  /** Called once the stream is finished or reset so the connection can retire its id. */
  retireStream(streamId: number): void;
}

const utf8Encoder = new TextEncoder();

interface ReadWaiter {
  resolve: (chunk: Uint8Array | null) => void;
  reject: (err: Error) => void;
}

type ReadOutcome = { value: Uint8Array | null } | { error: Error };

export class MixerStream implements AsyncIterable<Uint8Array> {
  readonly id: number;
  /** Set once RESET in either direction (peer, SDK, or app), or when the connection hands a stream-scoped error at teardown. */
  resetCode?: number;
  /** Resolves exactly once; never rejects. Buffered unread data stays readable after it resolves. */
  readonly closed: Promise<StreamCloseReason>;

  private readonly host: StreamHost;
  private state: StreamState = "open";
  private sendWindow: number;
  private recvWindow: number;
  private readonly initialWindow: number;
  private unacked = 0;

  private remoteClosed = false;
  /** closeWrite() was called: new writes are refused; CLOSE follows once queued writes flush. */
  private localClosing = false;
  private closeWritePromise: Promise<void> | null = null;
  /** A RESET (any direction): buffer discarded, everything fails with it. */
  private resetError: StreamError | null = null;
  /** The connection ended under this stream: buffer kept, then reads/writes fail with it. */
  private lostError: ConnError | StreamError | null = null;

  private readonly buffer: Uint8Array[] = [];
  private readonly readWaiters: ReadWaiter[] = [];
  /** Resolved when sendWindow becomes > 0 or the stream can no longer send. */
  private sendWaiters: Array<() => void> = [];
  /** Tail of the serialized write queue; never rejects. */
  private writeChain: Promise<void> = Promise.resolve();
  private resolveClosed!: (reason: StreamCloseReason) => void;
  private closedSettled = false;

  /** @internal */
  constructor(id: number, host: StreamHost, initialRecvWindow: number, initialSendWindow: number) {
    this.id = id;
    this.host = host;
    this.initialWindow = initialRecvWindow;
    this.recvWindow = initialRecvWindow;
    this.sendWindow = initialSendWindow;
    this.closed = new Promise((resolve) => {
      this.resolveClosed = resolve;
    });
  }

  getState(): StreamState {
    return this.state;
  }

  getSendWindow(): number {
    return this.sendWindow;
  }

  getRecvWindow(): number {
    return this.recvWindow;
  }

  // --- reading ----------------------------------------------------------------

  /**
   * Next received chunk, or null at clean EOF (peer CLOSE and buffer
   * drained). Rejects with StreamError after a RESET (buffer discarded), or
   * with the connection's error once the connection is lost, the buffer is
   * drained and no CLOSE had arrived. Credit is granted back here, as the
   * application consumes bytes (WIRE.md section 2.6). Concurrent read()
   * calls are served FIFO.
   */
  read(): Promise<Uint8Array | null> {
    if (this.readWaiters.length === 0) {
      const outcome = this.nextReadOutcome();
      if (outcome) return "error" in outcome ? Promise.reject(outcome.error) : Promise.resolve(outcome.value);
    }
    return new Promise((resolve, reject) => {
      this.readWaiters.push({ resolve, reject });
    });
  }

  /** Reads until null. `return()` (a `break` out of `for await`) does nothing to the stream. */
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return {
      next: async () => {
        const chunk = await this.read();
        return chunk === null ? { done: true, value: undefined } : { done: false, value: chunk };
      },
      return: async () => ({ done: true, value: undefined }),
    };
  }

  private nextReadOutcome(): ReadOutcome | null {
    const chunk = this.buffer.shift();
    if (chunk) {
      this.creditConsumed(chunk.length);
      return { value: chunk };
    }
    if (this.resetError) return { error: this.resetError };
    if (this.remoteClosed) return { value: null };
    if (this.lostError) return { error: this.lostError };
    return null;
  }

  private pumpReads(): void {
    while (this.readWaiters.length > 0) {
      const outcome = this.nextReadOutcome();
      if (!outcome) return;
      const waiter = this.readWaiters.shift()!;
      if ("error" in outcome) waiter.reject(outcome.error);
      else waiter.resolve(outcome.value);
    }
  }

  /** Credits the peer back once half the window has been consumed, mirroring Go `Stream.creditConsumed`. */
  private creditConsumed(n: number): void {
    this.unacked += n;
    if (this.unacked >= this.initialWindow / 2 && !this.remoteClosed && this.state !== "closed") {
      const increment = this.unacked;
      this.recvWindow += increment;
      this.unacked = 0;
      this.host.sendControlFrame(encodeWindow(this.id, increment));
    }
  }

  // --- writing ----------------------------------------------------------------

  /**
   * Queues bytes (a string is sent as UTF-8). Writes are serialized; each
   * resolves once all its DATA frames were handed to the socket, blocking on
   * send credit in 16 KiB chunks. Rejects with StreamError(STREAM_CLOSED)
   * after closeWrite() was called, StreamError(code) after RESET, or the
   * connection's error after the connection was lost.
   */
  write(chunk: Uint8Array | string): Promise<void> {
    const failure = this.resetError ?? this.lostError;
    if (failure) return Promise.reject(failure);
    if (this.localClosing || this.state === "half_closed_local" || this.state === "closed") {
      return Promise.reject(new StreamError(ErrorCode.STREAM_CLOSED, this.id, "write on a stream that is not open for sending"));
    }
    const bytes = typeof chunk === "string" ? utf8Encoder.encode(chunk) : chunk;
    if (bytes.length === 0) return Promise.resolve();
    const job = this.writeChain.then(() => this.sendAll(bytes));
    this.writeChain = job.then(
      () => {},
      () => {},
    );
    return job;
  }

  private async sendAll(bytes: Uint8Array): Promise<void> {
    let rest = bytes;
    try {
      while (rest.length > 0) {
        const n = await this.reserveSendCredit(rest.length);
        // Resuming past the await is the first point that can see a RESET or
        // a connection teardown that happened meanwhile; past here the stream
        // may already be retired, where nothing else stops the chunk.
        const { done, error } = this.sendDone();
        if (done) throw error;
        const piece = rest.subarray(0, n);
        rest = rest.subarray(n);
        await this.host.sendData(this.id, piece);
      }
    } catch (e) {
      throw this.resetError ?? this.lostError ?? e;
    }
  }

  /** Waits until send credit is available, then reserves and returns min(want, sendWindow, MAX_CHUNK). */
  private reserveSendCredit(want: number): Promise<number> {
    return new Promise((resolve, reject) => {
      const attempt = (): boolean => {
        const failure = this.resetError ?? this.lostError;
        if (failure) {
          reject(failure);
          return true;
        }
        if (this.state !== "open" && this.state !== "half_closed_remote") {
          reject(new StreamError(ErrorCode.STREAM_CLOSED, this.id, "write on a stream that is not open for sending"));
          return true;
        }
        if (this.sendWindow > 0) {
          const n = Math.min(want, this.sendWindow, MAX_CHUNK);
          this.sendWindow -= n;
          resolve(n);
          return true;
        }
        return false;
      };
      if (attempt()) return;
      const waiter = (): void => {
        if (!attempt()) this.sendWaiters.push(waiter);
      };
      this.sendWaiters.push(waiter);
    });
  }

  private wakeSendWaiters(): void {
    const waiters = this.sendWaiters;
    this.sendWaiters = [];
    for (const w of waiters) w();
  }

  /**
   * Half-close: refuses further writes now, waits for queued writes to
   * flush, then sends CLOSE ("I will send no more DATA"). Idempotent;
   * resolves once CLOSE is queued, or right away if the stream already
   * ended. Never rejects.
   */
  closeWrite(): Promise<void> {
    if (this.closeWritePromise) return this.closeWritePromise;
    this.localClosing = true;
    this.closeWritePromise = this.writeChain.then(() => {
      if (this.resetError || this.lostError) return;
      switch (this.state) {
        case "open":
          this.state = "half_closed_local";
          break;
        case "half_closed_remote":
          this.state = "closed";
          break;
        default:
          return;
      }
      this.host.sendControlFrame(encodeClose(this.id));
      this.wakeSendWaiters();
      if (this.state === "closed") this.finish();
    });
    return this.closeWritePromise;
  }

  /**
   * RESET in both directions: discards buffered data and fails pending
   * reads/writes with StreamError(code). `code` is a numeric ws-mixer.v1
   * error code or its wire name (e.g. "CANCEL"); `message` is truncated to
   * 256 UTF-8 bytes on a character boundary (WIRE.md section 2.3). Throws on
   * an unknown code name; otherwise a no-op once the stream has ended.
   */
  reset(code: number | string, message = ""): void {
    const numericCode = typeof code === "string" ? parseErrorCode(code) : code;
    if (numericCode === undefined) {
      throw new Error(`unknown ws-mixer error code name: ${code as string}`);
    }
    if (this.state === "closed") return;
    this.applyReset(numericCode, truncateUtf8(message, 256), true, false);
  }

  /**
   * Go semantics: if the peer has not sent CLOSE, RESET(CANCEL) (a plain
   * closeWrite() would leave it writing into a window nobody drains);
   * otherwise closeWrite().
   */
  close(): void {
    if (!this.remoteClosed) {
      this.reset(ErrorCode.CANCEL, "local close: peer had not finished sending");
      return;
    }
    void this.closeWrite();
  }

  /** RESET(REFUSED_STREAM): the application will not serve this stream. */
  refuse(): void {
    this.reset(ErrorCode.REFUSED_STREAM, "stream refused by the application");
  }

  // --- inbound frames (driven by MixerConn) -------------------------------------

  /**
   * Applies a received DATA frame. Throws ConnError(FLOW_CONTROL_ERROR) past
   * recv credit, StreamError(STREAM_CLOSED) after the peer's CLOSE.
   * @internal
   */
  handleData(payload: Uint8Array): void {
    if (this.state === "half_closed_remote") {
      throw new StreamError(ErrorCode.STREAM_CLOSED, this.id, "DATA received after this stream's CLOSE");
    }
    if (this.state === "closed") {
      throw new StreamError(ErrorCode.STREAM_CLOSED, this.id, "DATA received on a closed stream");
    }
    const n = payload.length;
    if (n > this.recvWindow) {
      throw new ConnError(ErrorCode.FLOW_CONTROL_ERROR, `stream ${this.id}: received ${n} DATA bytes with ${this.recvWindow} credit remaining`);
    }
    this.recvWindow -= n;
    if (n === 0) return;
    // The frame's buffer is reused by the transport: keep a copy.
    this.buffer.push(payload.slice());
    this.pumpReads();
  }

  /**
   * Applies a received WINDOW frame's credit increment. Throws ConnError on overflow past 2^31-1.
   * @internal
   */
  handleWindow(increment: number): void {
    // WINDOW must be tolerated on a half-closed or fully-closed stream (the
    // one race the ordered transport does not remove, WIRE.md section 2.5);
    // only the cumulative overflow case is rejected.
    const newWindow = this.sendWindow + increment;
    if (newWindow > MAX_SEND_WINDOW) {
      throw new ConnError(ErrorCode.FLOW_CONTROL_ERROR, `stream ${this.id}: WINDOW would push the send window to ${newWindow}, past 2^31-1`);
    }
    if (this.state === "closed") return;
    this.sendWindow = newWindow;
    this.wakeSendWaiters();
  }

  /**
   * Applies a received CLOSE frame: the peer will send no more DATA. Buffered data stays readable, then EOF.
   * @internal
   */
  handleClose(): void {
    switch (this.state) {
      case "open":
        this.state = "half_closed_remote";
        break;
      case "half_closed_local":
        this.state = "closed";
        break;
      case "half_closed_remote":
        throw new StreamError(ErrorCode.STREAM_CLOSED, this.id, "duplicate CLOSE received");
      case "closed":
        return;
    }
    this.remoteClosed = true;
    if (this.state === "closed") this.finish();
    this.pumpReads();
  }

  /**
   * Applies a received RESET frame: any state, discard buffered data, never answered with RESET.
   * @internal
   */
  handleReset(code: number, message: string): void {
    if (this.state === "closed") return;
    this.applyReset(code, message, false, true);
  }

  /**
   * A stream-scoped violation the SDK itself detected (MixerConn's dispatch
   * loop): like reset(), but the app did not choose it.
   * @internal
   */
  abort(code: number, message: string): void {
    if (this.state === "closed") return;
    this.applyReset(code, truncateUtf8(message, 256), true, false);
  }

  /**
   * The connection under this stream ended (MixerConn.rejectOutstanding). A
   * no-op once finished or reset. Buffered data (and an already-received
   * CLOSE) is still delivered first; then reads fail with `err`. A
   * StreamError (the drain-deadline CANCEL) ends the stream as a local reset
   * that sends nothing; anything else is a connection loss.
   * @internal
   */
  terminate(err: ConnError | StreamError): void {
    if (this.state === "closed") return;
    this.state = "closed";
    this.lostError = err;
    this.wakeSendWaiters();
    this.pumpReads();
    if (err instanceof StreamError) {
      this.resetCode = err.code;
      this.settleClosed({ kind: "reset", code: err.code, name: codeName(err.code), message: err.message, byPeer: false });
    } else {
      this.settleClosed({ kind: "connection_lost", error: err });
    }
  }

  /**
   * Reports whether this stream must no longer send DATA (CLOSE sent, RESET, or connection lost), with the error queued DATA fails with.
   * @internal
   */
  sendDone(): { done: boolean; error?: WsMixerError } {
    if (this.state === "half_closed_local" || this.state === "closed") {
      const failure = this.resetError ?? this.lostError;
      if (failure) return { done: true, error: failure };
      return { done: true, error: new StreamError(ErrorCode.STREAM_CLOSED, this.id, "DATA not sent: CLOSE already sent on this stream") };
    }
    return { done: false };
  }

  private applyReset(code: number, message: string, sendFrame: boolean, byPeer: boolean): void {
    this.state = "closed";
    this.resetCode = code;
    this.resetError = new StreamError(code, this.id, message);
    this.buffer.length = 0;
    this.wakeSendWaiters();
    this.pumpReads();
    if (sendFrame) this.host.sendControlFrame(encodeReset(this.id, code, message));
    this.host.retireStream(this.id);
    this.settleClosed({ kind: "reset", code, name: codeName(code), message, byPeer });
  }

  /** CLOSE both ways: retire the id and resolve `closed`. */
  private finish(): void {
    this.host.retireStream(this.id);
    this.settleClosed({ kind: "finished" });
  }

  private settleClosed(reason: StreamCloseReason): void {
    if (this.closedSettled) return;
    this.closedSettled = true;
    this.resolveClosed(reason);
  }
}
