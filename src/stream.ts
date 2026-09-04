/**
 * MixerStream: one ws-mixer byte stream, exposed as a Node Duplex.
 * Mirrors `go/wsmixer/stream.go`'s state machine, credit accounting and
 * half-close semantics (OVERVIEW.md section 2.5-2.6). The JS SDK is always
 * the answering peer: it never opens streams, only receives OPEN from the
 * server and reads/writes/closes/resets what arrives.
 *
 * Three ways a stream ends in RESET, all converging on the same terminal
 * state and `resetCode`, but differing in whether `'reset'` fires:
 *  - the peer sends RESET (handleReset()) -- `'reset'` fires; the stream's
 *    owner didn't choose this.
 *  - the SDK itself detects a violation on this stream (abort(), called from
 *    MixerConn's dispatch loop) -- `'reset'` fires, same reasoning as above:
 *    the app didn't choose it either.
 *  - the app calls the public reset() -- `'reset'` does NOT fire; the caller
 *    already knows why, by definition.
 * `resetCode` is set by all three, always, whether or not an `'error'`
 * listener is attached.
 */
import { Duplex } from "node:stream";
import { ConnError, ErrorCode, StreamError, WsMixerError, parseErrorCode } from "./errors.js";
import { MAX_CHUNK, MAX_SEND_WINDOW, encodeClose, encodeReset, encodeWindow } from "./frame.js";
import { truncateUtf8 } from "./util.js";

export type StreamState = "idle" | "open" | "half_closed_local" | "half_closed_remote" | "closed";

/** Payload of the `'reset'` event: fired once, whenever this stream is RESET by something other than the app's own reset() call -- a peer RESET (handleReset()) or an SDK-detected violation (abort()) -- whether or not an `'error'` listener is attached. */
export interface StreamResetInfo {
  code: number;
  message: string;
}

/** Callback the stream uses to hand an outbound DATA/WINDOW/CLOSE/RESET frame to the connection. */
export interface StreamHost {
  /** Enqueue a DATA chunk for round-robin transmission; resolves once written (or rejects on failure). */
  sendData(streamId: number, chunk: Uint8Array): Promise<void>;
  /** Send a control-priority frame (WINDOW/CLOSE/RESET) immediately. */
  sendControlFrame(frame: Uint8Array): void;
  /** Called when the stream is fully closed so the connection can retire its id. */
  retireStream(streamId: number): void;
}

export class MixerStream extends Duplex {
  readonly id: number;
  private readonly host: StreamHost;

  private state: StreamState = "open";
  private sendWindow: number;
  private recvWindow: number;
  private readonly initialWindow: number;
  private unacked = 0;

  private remoteClosed = false;
  private terminalError: WsMixerError | null = null;

  /** Resolved when sendWindow becomes > 0 or the stream can no longer send. */
  private sendWaiters: Array<() => void> = [];
  /** Bytes queued via _write, waiting on credit; drives backpressure. */
  private writeQueue: Array<{ chunk: Uint8Array; callback: (err?: Error | null) => void }> = [];
  private draining = false;

  /** Set once this stream is RESET -- by the peer (handleReset), the SDK (abort()), or the app itself (reset()); undefined until then. Set even when no `'error'` listener is attached. */
  resetCode?: number;

  constructor(id: number, host: StreamHost, initialRecvWindow: number, initialSendWindow: number) {
    super({ autoDestroy: false, emitClose: true, readableHighWaterMark: initialRecvWindow });
    this.id = id;
    this.host = host;
    this.initialWindow = initialRecvWindow;
    this.recvWindow = initialRecvWindow;
    this.sendWindow = initialSendWindow;
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

  // --- Duplex plumbing ------------------------------------------------------

  override _read(_size: number): void {
    // Nothing to do: handleData() pushes directly as bytes arrive. Node's
    // Duplex internally buffers what push() delivers, so this is a no-op
    // producer side; credit accounting happens in creditConsumed(), invoked
    // as the consumer actually reads out of that internal buffer (see
    // override of `read`/`_read` limitations below and the `data`/`resume`
    // driven credit hook installed in creditOnConsume()).
  }

  override _write(chunk: Uint8Array, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    if (this.terminalError) {
      callback(this.terminalError);
      return;
    }
    if (this.state !== "open" && this.state !== "half_closed_remote") {
      callback(new StreamError(ErrorCode.STREAM_CLOSED, this.id, "write on a stream that is not open for sending"));
      return;
    }
    this.writeQueue.push({ chunk, callback });
    this.pumpWriteQueue();
  }

  override _final(callback: (error?: Error | null) => void): void {
    this.closeWrite();
    callback();
  }

  override _destroy(err: Error | null, callback: (error?: Error | null) => void): void {
    if (this.state !== "closed") {
      this.reset(ErrorCode.CANCEL, err ? String(err.message) : "local destroy");
    }
    callback(err);
  }

  // --- outbound: send credit + chunking -------------------------------------

  private pumpWriteQueue(): void {
    if (this.draining) return;
    this.draining = true;
    void this.drainWriteQueue().finally(() => {
      this.draining = false;
      if (this.writeQueue.length > 0) this.pumpWriteQueue();
    });
  }

  private async drainWriteQueue(): Promise<void> {
    while (this.writeQueue.length > 0) {
      const item = this.writeQueue[0]!;
      let { chunk } = item;
      const { callback } = item;
      try {
        while (chunk.length > 0) {
          const n = await this.reserveSendCredit(chunk.length);
          const piece = chunk.subarray(0, n);
          chunk = chunk.subarray(n);
          await this.host.sendData(this.id, piece);
        }
        this.writeQueue.shift();
        callback();
      } catch (e) {
        this.writeQueue.shift();
        // Same rationale as handleReset(): don't hand Node's writable
        // machinery an error it will re-emit as 'error' with nobody there to
        // hear it.
        callback(this.hasErrorListener() ? (e as Error) : undefined);
      }
    }
  }

  /** Waits until send credit is available, then reserves and returns min(want, sendWindow, MAX_CHUNK). */
  private reserveSendCredit(want: number): Promise<number> {
    return new Promise((resolve, reject) => {
      const attempt = (): boolean => {
        if (this.terminalError) {
          reject(this.terminalError);
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
      this.sendWaiters.push(() => {
        if (attempt()) return;
        // still blocked; re-register (handleWindow/onTerminal calls waiters again)
      });
    });
  }

  private wakeSendWaiters(): void {
    const waiters = this.sendWaiters;
    this.sendWaiters = [];
    for (const w of waiters) w();
  }

  // --- inbound: DATA / credit-on-consume -------------------------------------

  /**
   * Applies a received DATA frame. Throws a ConnError (FLOW_CONTROL_ERROR) if it exceeds recv credit.
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
      throw new ConnError(
        ErrorCode.FLOW_CONTROL_ERROR,
        `stream ${this.id}: received ${n} DATA bytes with ${this.recvWindow} credit remaining`,
      );
    }
    this.recvWindow -= n;
    // push() delivers bytes to the Duplex's readable side; credit is granted
    // back to the peer as the *application* consumes them (creditConsumed),
    // not on receipt (OVERVIEW.md section 2.6). The read() override below
    // covers three of the four consumption modes (read(), pipe() and
    // for-await both drive Node's internal flow loop through the public
    // read() method) -- but Node has a "fast path" for on('data')/pipe()
    // consumers already in flowing mode with an empty internal buffer:
    // readableAddChunk emits 'data' synchronously from *inside* push(),
    // handing the chunk straight to the consumer without ever touching the
    // buffer or calling read(). Measuring readableLength across push() below
    // catches exactly that: whatever didn't end up buffered was delivered
    // (and thus consumed) right here, synchronously.
    const before = this.readableLength;
    this.push(Buffer.from(payload));
    const buffered = this.readableLength - before;
    const bypassed = n - buffered;
    if (bypassed > 0) this.creditConsumed(bypassed);
  }

  /**
   * Credits the peer back once bytes leave the internal buffer, mirroring Go
   * `Stream.creditConsumed` (stream.go) and its half-window threshold.
   */
  private creditConsumed(n: number): void {
    this.unacked += n;
    this.maybeDeliverEOF();
    const threshold = this.initialWindow / 2;
    if (this.unacked >= threshold && this.state !== "closed") {
      const increment = this.unacked;
      this.recvWindow += increment;
      this.unacked = 0;
      this.host.sendControlFrame(encodeWindow(this.id, increment));
    }
  }

  private eofDelivered = false;
  private internalBufferEmpty(): boolean {
    // readableLength is Duplex's own count of unread, buffered bytes.
    return this.readableLength === 0;
  }

  /**
   * Covers every consumption mode that goes through the buffer: explicit
   * paused-mode `.read()` calls, and Node's internal flow loop -- which
   * calls this same public `read()` method to back `on('data')`, `pipe()`
   * and `for await` once bytes are actually sitting in the buffer (as
   * opposed to handleData()'s synchronous fast-path bypass above, which
   * never reaches the buffer at all). Between the two, every byte is
   * credited exactly once: whatever left the buffer here, plus whatever
   * never entered it there.
   */
  override read(size?: number): unknown {
    const before = this.readableLength;
    const result: unknown = super.read(size);
    const consumed = before - this.readableLength;
    if (consumed > 0) this.creditConsumed(consumed);
    return result;
  }

  // --- inbound: WINDOW / CLOSE / RESET ---------------------------------------

  /**
   * Applies a received WINDOW frame's credit increment. Throws ConnError on overflow past 2^31-1.
   * @internal
   */
  handleWindow(increment: number): void {
    // WINDOW must be tolerated on a half-closed or fully-closed stream (the
    // one race the ordered transport does not remove, OVERVIEW.md section
    // 2.5); only the cumulative overflow case is rejected.
    const newWindow = this.sendWindow + increment;
    if (newWindow > MAX_SEND_WINDOW) {
      throw new ConnError(ErrorCode.FLOW_CONTROL_ERROR, `stream ${this.id}: WINDOW would push the send window to ${newWindow}, past 2^31-1`);
    }
    if (this.state === "closed") return;
    this.sendWindow = newWindow;
    this.wakeSendWaiters();
  }

  /**
   * Applies a received CLOSE frame: half-close-remote (peer will send no more DATA).
   * @internal
   */
  handleClose(): void {
    switch (this.state) {
      case "open":
        this.state = "half_closed_remote";
        break;
      case "half_closed_local":
        this.state = "closed";
        this.host.retireStream(this.id);
        break;
      case "half_closed_remote":
        throw new StreamError(ErrorCode.STREAM_CLOSED, this.id, "duplicate CLOSE received");
      case "closed":
        return;
    }
    this.remoteClosed = true;
    this.maybeDeliverEOF();
  }

  // CLOSE preserves buffered data; EOF (push(null)) only follows once every
  // buffered byte has been delivered to the application (OVERVIEW.md section
  // 2.5: "deliver those bytes and *then* EOF"). Called both right after
  // handleClose() (buffer may already be empty) and from creditConsumed() as
  // the application keeps draining the buffer.
  private maybeDeliverEOF(): void {
    if (!this.eofDelivered && this.remoteClosed && this.internalBufferEmpty()) {
      this.eofDelivered = true;
      this.push(null);
    }
  }

  /** True once something is listening for `'error'`: gates whether a peer RESET may destroy(err) (see handleReset). */
  private hasErrorListener(): boolean {
    return this.listenerCount("error") > 0;
  }

  /**
   * Applies a received RESET frame: any state, discard buffered data, go to
   * closed. A peer RESET (including CANCEL, the ordinary path for a client-
   * initiated `close()`) is not exceptional enough to justify crashing a
   * consumer that never attached `'error'` -- Node throws synchronously out
   * of destroy()/a write callback when an error is emitted with no listener.
   * So: destroy(err) only when someone is listening for it; otherwise end
   * the readable side cleanly (push(null)) and surface the code via
   * `resetCode`/`'reset'` instead of an exception.
   * @internal
   */
  handleReset(code: number, message: string): void {
    this.state = "closed";
    const err = new StreamError(code, this.id, message);
    this.terminalError = err;
    this.resetCode = code;
    this.wakeSendWaiters();
    this.emit("reset", { code, message });
    this.terminateNoThrow(err);
    this.host.retireStream(this.id);
  }

  /**
   * Tears this stream down with `err` the same way handleReset() does:
   * destroy(err) when someone is listening for `'error'`, otherwise a clean
   * push(null) + destroy() so nothing throws. Used by handleReset() itself
   * and by MixerConn.rejectOutstanding() (a connection-level failure aborts
   * every live stream with the connection's terminal error the same way a
   * stream-level RESET would abort just that one stream).
   * @internal
   */
  terminateNoThrow(err: WsMixerError): void {
    if (this.hasErrorListener()) {
      // Discards buffered data: destroy drops whatever Duplex has queued.
      this.destroy(err);
    } else {
      this.flushWriteQueueSilently();
      this.push(null);
      this.destroy();
    }
  }

  /** Completes every queued write with success (no error) so Node's writable machinery never re-raises an 'error' with nobody listening. */
  private flushWriteQueueSilently(): void {
    const queued = this.writeQueue.splice(0, this.writeQueue.length);
    for (const item of queued) item.callback();
  }

  /**
   * Reports whether this stream must no longer send DATA (CLOSE sent, or RESET).
   * @internal
   */
  sendDone(): { done: boolean; error?: WsMixerError } {
    if (this.state === "half_closed_local" || this.state === "closed") {
      if (this.terminalError) return { done: true, error: this.terminalError };
      return { done: true, error: new StreamError(ErrorCode.STREAM_CLOSED, this.id, "DATA not sent: CLOSE already sent on this stream") };
    }
    return { done: false };
  }

  // --- public half-close / reset API -----------------------------------------

  /** Sends CLOSE: "I will send no more DATA on this stream." end() is sugar for this via _final. */
  closeWrite(): void {
    switch (this.state) {
      case "open":
        this.state = "half_closed_local";
        break;
      case "half_closed_remote":
        this.state = "closed";
        break;
      case "half_closed_local":
      case "closed":
        return; // already sent, or nothing to send to
    }
    const closedNow = this.state === "closed";
    this.host.sendControlFrame(encodeClose(this.id));
    if (closedNow) this.host.retireStream(this.id);
  }

  /**
   * Sends CLOSE (if not already sent) and stops delivering further reads. If
   * the peer has not yet half-closed its own send side, a plain closeWrite()
   * would leave it writing into a window nobody drains, so close() also
   * RESETs with CANCEL in that case (mirrors go/wsmixer/stream.go Close()).
   */
  close(): void {
    if (!this.remoteClosed) {
      this.closeWrite();
      this.reset(ErrorCode.CANCEL, "local close: peer had not finished sending");
      return;
    }
    this.closeWrite();
    this.destroy();
  }

  /**
   * Aborts the stream in both directions with the given error code and
   * message, discarding buffered data. `code` may be a numeric ws-mixer.v1
   * error code or its wire name (e.g. `"CANCEL"`); `message` is truncated to
   * 256 UTF-8 bytes on a character boundary (OVERVIEW.md section 2.7's
   * `drain.message` limit; RESET has no documented cap of its own, so this
   * mirrors that ceiling defensively).
   *
   * Same "don't throw with nobody listening" rule as handleReset(): destroy
   * with the error only if something is listening for `'error'`, otherwise
   * end cleanly via terminateNoThrow() and surface the code via `resetCode`.
   *
   * This is the app-facing API: calling code already knows why it's
   * resetting its own stream, so unlike a peer-sent RESET (handleReset()) or
   * an SDK-detected violation (abort()), `reset()` does NOT emit `'reset'`.
   */
  reset(code: number | string, message = ""): void {
    this.applyReset(code, message);
  }

  /**
   * Internal counterpart to reset(): used when the *connection* -- not
   * application code -- is what decided to tear this stream down (an
   * SDK-detected protocol violation surfaced as a `StreamError` from
   * MixerConn's dispatch loop). Identical effect to reset(), but also emits
   * `'reset'` the same way handleReset() does for a peer-sent RESET: in both
   * cases the stream's owner didn't choose this, so it needs to hear about
   * it. No-ops (and does not emit) if the stream is already closed.
   * @internal
   */
  abort(code: number | string, message = ""): void {
    if (this.applyReset(code, message)) {
      this.emit("reset", { code: this.resetCode!, message: this.terminalError!.message });
    }
  }

  /** Shared implementation behind reset()/abort(); returns false (no-op) if the stream was already closed. */
  private applyReset(code: number | string, message: string): boolean {
    if (this.state === "closed") return false;
    const numericCode = typeof code === "string" ? parseErrorCode(code) : code;
    if (numericCode === undefined) {
      throw new Error(`unknown ws-mixer error code name: ${code as string}`);
    }
    const truncated = truncateUtf8(message, 256);
    this.state = "closed";
    this.terminalError = new StreamError(numericCode, this.id, truncated);
    this.resetCode = numericCode;
    this.wakeSendWaiters();
    this.host.sendControlFrame(encodeReset(this.id, numericCode, truncated));
    this.host.retireStream(this.id);
    this.terminateNoThrow(this.terminalError);
    return true;
  }

  /**
   * Wraps this stream as Web Streams API `{ readable, writable }`, e.g. for
   * `fetch`'s `duplex` option or any other Web Streams consumer.
   */
  toWeb(): ReturnType<typeof Duplex.toWeb> {
    return Duplex.toWeb(this);
  }
}
