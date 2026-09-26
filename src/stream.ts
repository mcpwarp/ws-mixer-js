/**
 * MixerStream: one ws-mixer byte stream, exposed as a Node Duplex.
 * Mirrors `ws-mixer-go/wsmixer/stream.go`'s state machine, credit accounting and
 * half-close semantics (WIRE.md section 2.5-2.6). The JS SDK is always
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

/** Attached by terminateNoThrow() (and failWrite()) when nothing else is listening for 'error', purely so Node's own unlistened-'error' crash never fires -- see terminateNoThrow's doc comment. */
const NOOP_ERROR_LISTENER = () => {};

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
  /** Called when the stream is fully closed so the connection can retire its id (a connection teardown still reaches it until it is destroyed). */
  retireStream(streamId: number): void;
  /** Called when the stream starts holding a write callback, so the connection holds a retired stream strongly until that callback is settled (a no-op for one not yet retired). Optional: a host that tracks no retired streams has nothing to pin. */
  pinRetired?(streamId: number): void;
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
  /** Write callbacks (_write's own terminal-error branch, or failWrite()) whose error must wait until the read side has finished delivering buffered data and emitted 'end' -- see terminateNoThrow's doc comment. Non-null exactly while that wait is pending; flushed (and reset to null) from _destroy(), or from the 'end' listener failWrite() installs. */
  private heldWriteCallbacks: Array<(err?: Error | null) => void> | null = null;

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
      // A write issued during or after termination: if the read side is
      // still mid-delivery of buffered data (heldWriteCallbacks non-null,
      // see terminateNoThrow), hold this callback too instead of calling it
      // with an error now -- same reasoning as failWrite(). Otherwise the
      // read side is long done and there's nothing left to block, so fail
      // it immediately.
      if (this.heldWriteCallbacks) {
        this.heldWriteCallbacks.push(callback);
      } else {
        callback(this.terminalError);
      }
      return;
    }
    if (this.state !== "open" && this.state !== "half_closed_remote") {
      this.failWrite(callback, new StreamError(ErrorCode.STREAM_CLOSED, this.id, "write on a stream that is not open for sending"));
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
    // Flush write callbacks terminateNoThrow held back for the read side (see
    // its doc comment) -- whether this destroy() is the one it scheduled via
    // 'end', or the read side never got there because something else (the
    // app's own destroy(), reset()) tore the stream down first.
    if (this.heldWriteCallbacks) {
      const held = this.heldWriteCallbacks;
      this.heldWriteCallbacks = null;
      for (const cb of held) cb(this.terminalError ?? err ?? undefined);
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
          // Credit can be reserved synchronously, before a closeWrite()/
          // reset()/destroy() in the same tick; resuming past this await is
          // the first point that can see it. Past here the stream may also
          // be retired from MixerConn's table, where nothing else stops the
          // chunk from going out after this stream's CLOSE/RESET.
          const { done, error } = this.sendDone();
          if (done) throw error;
          const piece = chunk.subarray(0, n);
          chunk = chunk.subarray(n);
          await this.host.sendData(this.id, piece);
        }
        this.writeQueue.shift();
        callback();
      } catch (e) {
        this.writeQueue.shift();
        // A send that fails after this stream was torn down (its socket
        // dying under the in-flight write) reports the teardown's error,
        // the one `stream.errored` already holds.
        this.failWrite(callback, this.terminalError ?? (e as Error));
      }
    }
  }

  /**
   * Settles a failed write's callback with `failure` -- now, or held until
   * the read side can no longer be hurt by it. Calling a write callback with
   * an error marks the READABLE side errored too (Node's own Writable/Duplex
   * machinery), which drops whatever is still buffered and blocks 'end'.
   *
   * - terminateNoThrow is already holding write errors for the read side
   *   (`heldWriteCallbacks` non-null): join that hold -- see its doc comment.
   * - The peer's CLOSE has arrived and 'end' hasn't fired yet, whatever this
   *   side's own write state (`half_closed_remote`, or `closed` after
   *   closeWrite()): the read side may still be delivering buffered data, so
   *   hold the callback until 'end', without relying on teardown to flush it
   *   -- there may be no teardown at all. A connection teardown that does
   *   come (it also reaches a stream closeWrite() retired) flushes it once
   *   nothing is left unread; so do destroy()/reset(). `failure` is the
   *   fallback for a flush with no terminalError of its own (always, on a
   *   stream CLOSE'd both ways -- see terminateNoThrow).
   * - Otherwise (peer still sending, 'end' already fired or never will --
   *   destroyed, or already errored): fail it now.
   *
   * Node's Writable machinery re-emits a write callback's error as 'error',
   * so -- same rationale as terminateNoThrow() -- the internal no-op listener
   * is attached first if nothing else is listening: a failure is also
   * reachable before terminateNoThrow() has run at all (the connection's
   * writer failed this chunk's socket write, and teardown follows from the
   * socket's own 'close'), and a dying connection must not crash a consumer
   * that never attached its own 'error'.
   */
  private failWrite(callback: (err?: Error | null) => void, failure: Error): void {
    if (this.heldWriteCallbacks) {
      this.heldWriteCallbacks.push((err) => callback(err ?? failure));
      return;
    }
    if (!this.hasErrorListener()) {
      this.on("error", NOOP_ERROR_LISTENER);
    }
    if (!this.remoteClosed || this.readableEnded || this.destroyed || this.errored) {
      callback(failure);
      return;
    }
    this.heldWriteCallbacks = [(err) => callback(err ?? failure)];
    // Held after closeWrite() retired this stream: the connection's weak
    // ref must become strong, or a stream the app dropped would be
    // collected with this callback never settled.
    this.host.pinRetired?.(this.id);
    // Deferred a tick past 'end' so every 'end' listener -- the app's, and
    // Node's own for-await/pipeline() handling -- has run before the
    // callback's error can re-emit as 'error'.
    this.once("end", () =>
      process.nextTick(() => {
        if (this.destroyed || !this.heldWriteCallbacks) return;
        // Closed by closeWrite(): retired, so only a connection teardown
        // (which may never come) would otherwise destroy it, and 'close'
        // needs a destroy() on an `autoDestroy: false` Duplex. _destroy()
        // flushes, after destroy has begun, so Node doesn't re-emit the error.
        if (this.state === "closed") {
          this.destroy();
          return;
        }
        // Still half_closed_remote: the teardown that's still coming
        // destroys it (right away, the buffer being empty now), but the
        // callback doesn't wait on it; _destroy() then finds the array null.
        const held = this.heldWriteCallbacks;
        this.heldWriteCallbacks = null;
        for (const cb of held) cb(this.terminalError ?? undefined);
      }),
    );
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
    // not on receipt (WIRE.md section 2.6). The read() override below
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
    // one race the ordered transport does not remove, WIRE.md section
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
    let closedNow = false;
    switch (this.state) {
      case "open":
        this.state = "half_closed_remote";
        break;
      case "half_closed_local":
        this.state = "closed";
        closedNow = true;
        this.host.retireStream(this.id);
        break;
      case "half_closed_remote":
        throw new StreamError(ErrorCode.STREAM_CLOSED, this.id, "duplicate CLOSE received");
      case "closed":
        return;
    }
    this.remoteClosed = true;
    this.maybeDeliverEOF();
    if (closedNow) this.destroyWhenDone();
  }

  /**
   * Both directions CLOSE'd cleanly (closeWrite() + the peer's CLOSE, in
   * either order): the stream is retired, so only a connection teardown
   * (which may never come) would otherwise reach it, and 'close' needs a
   * destroy() on an `autoDestroy: false` Duplex. Mirrors
   * what autoDestroy itself would do: destroy once the readable has emitted
   * 'end' (a tick later, like failWrite()'s hold, so every 'end' listener
   * runs first and 'end' always precedes 'close'), and, when the write side
   * was closed by end(), once 'finish' has fired too -- destroying inside
   * _final() would suppress 'finish' and turn finished()/pipeline() into a
   * premature close. A readable that already errored never emits 'end', so
   * that destroys right away. A consumer that never reads never sees 'end',
   * and so never 'close' -- same as any other Readable.
   */
  private destroyWhenDone(): void {
    if (this.destroyed) return;
    if (this.errored) {
      this.destroy();
      return;
    }
    if (this.writableEnded && !this.writableFinished) {
      this.once("finish", () => this.destroyWhenDone());
      return;
    }
    if (this.readableEnded) {
      this.destroy();
      return;
    }
    this.once("end", () =>
      process.nextTick(() => {
        if (!this.destroyed) this.destroy();
      }),
    );
  }

  // CLOSE preserves buffered data; EOF (push(null)) only follows once every
  // buffered byte has been delivered to the application (WIRE.md section
  // 2.5: "deliver those bytes and *then* EOF"). Called both right after
  // handleClose() (buffer may already be empty) and from creditConsumed() as
  // the application keeps draining the buffer.
  private maybeDeliverEOF(): void {
    if (!this.eofDelivered && this.remoteClosed && this.internalBufferEmpty()) {
      this.eofDelivered = true;
      this.push(null);
    }
  }

  /** True once something is listening for `'error'`: gates attaching the internal no-op `'error'` listener (terminateNoThrow, failWrite). */
  private hasErrorListener(): boolean {
    return this.listenerCount("error") > 0;
  }

  /**
   * Applies a received RESET frame: any state, discard buffered data, go to
   * closed (WIRE.md: RESET discards buffered data, unlike CLOSE). Node
   * throws synchronously out of destroy()/a write callback when an 'error'
   * is emitted with no listener -- terminateNoThrow's internal no-op
   * listener is what keeps a peer RESET (including CANCEL, the ordinary path
   * for a client-initiated `close()`) from crashing a consumer that never
   * attached its own `'error'`, while still surfacing the code via
   * `resetCode`/`'reset'`/`stream.errored` instead of a false clean end.
   * @internal
   */
  handleReset(code: number, message: string): void {
    this.state = "closed";
    const err = new StreamError(code, this.id, message);
    this.terminalError = err;
    this.resetCode = code;
    this.wakeSendWaiters();
    this.emit("reset", { code, message });
    this.terminateNoThrow(err, { discardBuffered: true });
    this.host.retireStream(this.id);
  }

  /**
   * Tears this stream down with `err`. Used by handleReset()/applyReset() (a
   * stream-level RESET, peer-sent or app-initiated -- both pass
   * `{discardBuffered: true}`, per WIRE.md: "RESET discards buffered data")
   * and by MixerConn.rejectOutstanding() (a connection-level failure aborts
   * every stream still live when it hit -- no `discardBuffered`).
   *
   * Two different endings, per CLIENT-SDK.md's "Stream teardown on
   * disconnect" row:
   *
   * Node's own 'error' event throws synchronously and crashes the process
   * when nothing is listening for it -- an internal no-op 'error' listener is
   * attached first, unconditionally, whenever nothing else is listening
   * (harmless -- the stream is terminating either way): needed by the
   * destroy(err) branch below directly, and by the push(null) branch too,
   * since even there the WRITE side is still live from Node's own Writable
   * machinery's point of view, and a later write's `_write` callback error
   * (`terminalError`, set above) still makes Node itself try to emit
   * 'error'.
   *
   * - `opts?.discardBuffered`, OR the peer's own CLOSE never arrived on this
   *   stream (`!this.remoteClosed`): destroy(err) -- this stream's read side
   *   never legitimately finished, so it ends with an error, never a clean
   *   end-of-stream. 'end' is never emitted from this branch, so a consumer
   *   with only 'data'/'end'/'close' listeners sees 'close' (and
   *   `stream.errored`) but never a false clean end; pipe()/pipeline()/
   *   for-await consumers see `err` exactly as they would with a real
   *   'error' listener attached.
   * - Otherwise (no `discardBuffered`, the peer's CLOSE already arrived --
   *   `this.remoteClosed`): this stream's READ side genuinely ended cleanly
   *   on the wire before the connection died, and CLIENT-SDK.md requires the
   *   bytes it preserved to be delivered first, followed by 'end', followed
   *   by 'close' -- never an 'error'. `push(null)` does NOT discard whatever
   *   Node's Duplex still has buffered -- it marks EOF, and Node delivers the
   *   buffered bytes to the consumer and only then emits 'end' (unlike
   *   `destroy(err)`, which WOULD discard them). No 'error' event fires: the
   *   response DID complete -- but `stream.errored` does NOT stay `null`
   *   forever: once `heldWriteCallbacks` is flushed (see below), a write
   *   callback is called with `err`, and Node's own Writable machinery marks
   *   `stream.errored` from that alone, same as any other write error. Once
   *   'end' has actually been delivered (immediately, if the buffer was
   *   already empty; otherwise once the consumer has drained it), this
   *   stream is `destroy()`'d with no error -- required for 'close' to ever
   *   fire on an `autoDestroy: false` Duplex, which this stream is
   *   (`emitClose: true` alone only arms the event, it doesn't schedule it).
   *   `eofDelivered` guards against calling `push(null)` a second time if
   *   `maybeDeliverEOF()` already did (buffer was already empty when CLOSE
   *   arrived); in that case the buffer is still (and can only still be)
   *   empty here too -- nothing can add to it once `remoteClosed` is set --
   *   so this destroys right away rather than waiting on an 'end' that may
   *   already have fired, or may never (a consumer that never reads never
   *   sees Node's own 'end', the same way it never would on any other
   *   Readable).
   *
   *   A write outstanding at this exact moment (`writeQueue` non-empty, one
   *   item mid-`drainWriteQueue`, or one issued via `_write` after this call)
   *   still needs its callback settled with `err` -- the caller is waiting on
   *   it -- but calling it with an error *before* 'end' has fired would make
   *   Node's own Writable/Duplex machinery mark the READABLE side `errored`
   *   too, permanently blocking 'end' regardless of whether an 'error'
   *   listener is attached. So instead of calling those callbacks here,
   *   `heldWriteCallbacks` (non-null for exactly this stretch) collects them
   *   -- pushed onto by failWrite() and by `_write`'s terminal-error
   *   branch -- and `_destroy()` flushes them with `err`
   *   right after `destroy()` actually runs (whether that's the 'end'-
   *   triggered `destroy()` below, or the read side never gets there because
   *   something else destroys the stream first): by then the readable side
   *   is already finished or was never going to finish anyway, so marking it
   *   `errored` is no longer a concern.
   *
   *   Accepted tradeoff: a held write callback is settled only once the read
   *   side has actually been drained ('end') or the stream is destroyed some
   *   other way (app destroy(), reset()). An app that awaits a write's
   *   callback without ever reading this stream will wait until it does one
   *   or the other.
   *
   * `this.state = "closed"` up front (not just `terminalError`) matters for
   * more than bookkeeping: `_destroy()`'s own guard (`if (this.state !==
   * "closed") this.reset(...)`) would otherwise re-enter here through
   * reset()/applyReset()/terminateNoThrow() a second time, overwriting the
   * real `err` with a generic CANCEL, in the destroy(err) branch above.
   *
   * Setting `terminalError` and waking every sendWaiter (not just calling
   * destroy()) is what makes a write already blocked on send credit
   * (drainWriteQueue's reserveSendCredit) -- and everything still queued
   * behind it -- fail promptly with `err` too, in EITHER branch: each
   * rejection drains the next queued item in turn via drainWriteQueue's own
   * loop, so no separate queue-flushing step is needed here.
   *
   * A stream already fully, cleanly closed (both directions CLOSE'd) has
   * already called `retireStream()`, but MixerConn keeps it for
   * `rejectOutstanding` until it is destroyed: it lands in the branch above
   * (`remoteClosed`), so its read side still ends cleanly, and 'close' fires
   * once nothing is left unread. Its exchange already completed, so `err`
   * is not adopted (no `terminalError`, no `resetCode`): a write callback it
   * holds keeps its own failure (STREAM_CLOSED, or its socket send's error).
   * Without a teardown, destroyWhenDone() is what ends it.
   *
   * `resetCode` is set here too, but only when `err` is a `StreamError`
   * (mirrors handleReset()/applyReset(), which construct one for exactly
   * this): the drain-hand-over `streamErrorFactory` override (client.ts)
   * hands a stream-scoped `StreamError(CANCEL, "connection drained")` for a
   * CONNECTION teardown (no `discardBuffered`) -- deliberately NOT what
   * gates the buffered-data decision above (that's `opts.discardBuffered`
   * alone), since a `StreamError` here doesn't mean "discard": it would
   * otherwise wrongly discard a response that had already completed on
   * every drain hand-over. From the stream's own perspective a `StreamError`
   * IS a fourth way it ends in RESET, on top of the three the class doc
   * above lists, distinct from an ordinary connection failure (always a
   * `ConnError` by the time rejectOutstanding() hands it here), which is not
   * RESET-shaped and leaves `resetCode` unset.
   * @internal
   */
  terminateNoThrow(err: WsMixerError, opts?: { discardBuffered?: boolean }): void {
    const closedCleanly = this.state === "closed" && !this.terminalError;
    this.state = "closed";
    if (!closedCleanly) {
      this.terminalError = err;
      if (err instanceof StreamError) this.resetCode = err.code;
    }
    const deferWriteErrors = !opts?.discardBuffered && this.remoteClosed;
    if (deferWriteErrors) this.heldWriteCallbacks = this.heldWriteCallbacks ?? [];
    this.wakeSendWaiters();
    // Attached unconditionally, before either branch below: even in the
    // push(null) branch, the WRITE side is still live from Node's own
    // Writable machinery's point of view (only the readable side ends here)
    // -- a later write's `_write` callback error (terminalError, above)
    // still makes Node itself try to emit 'error', which would otherwise
    // crash a consumer that only ever cared about reading.
    if (!this.hasErrorListener()) {
      this.on("error", NOOP_ERROR_LISTENER);
    }
    if (deferWriteErrors) {
      const hadBufferedData = !this.internalBufferEmpty();
      if (!this.eofDelivered) {
        this.eofDelivered = true;
        this.push(null);
      }
      if (hadBufferedData) {
        this.once("end", () => this.destroy());
      } else {
        this.destroy();
      }
      return;
    }
    this.destroy(err);
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

  /**
   * True while a write callback is held back for the read side (see failWrite()).
   * @internal
   */
  hasHeldWriteCallbacks(): boolean {
    return this.heldWriteCallbacks !== null && this.heldWriteCallbacks.length > 0;
  }

  // --- public half-close / reset API -----------------------------------------

  /**
   * Sends CLOSE: "I will send no more DATA on this stream." end() is sugar
   * for this via _final. A write still waiting on send credit can never go
   * out now, so it is woken to fail with STREAM_CLOSED (reserveSendCredit)
   * instead of waiting on a WINDOW that, once this stream is retired, would
   * never reach it.
   */
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
    this.wakeSendWaiters();
    if (closedNow) {
      this.host.retireStream(this.id);
      this.destroyWhenDone();
    }
  }

  /**
   * Sends CLOSE (if not already sent) and stops delivering further reads. If
   * the peer has not yet half-closed its own send side, a plain closeWrite()
   * would leave it writing into a window nobody drains, so close() also
   * RESETs with CANCEL in that case (mirrors ws-mixer-go/wsmixer/stream.go Close()).
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
   * 256 UTF-8 bytes on a character boundary (WIRE.md section 2.3: a RESET
   * message SHOULD be <= 256 B).
   *
   * Same "don't throw with nobody listening" rule as handleReset(): an
   * internal no-op 'error' listener (attached by terminateNoThrow() when
   * nothing else is) keeps this from crashing a consumer with no 'error' of
   * its own, while still surfacing the code via `resetCode`/`stream.errored`
   * (buffered data discarded, per RESET's own semantics: terminateNoThrow()
   * is called with `{discardBuffered: true}`).
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
   * it. Sends nothing and does not emit if the stream is already closed
   * (applyReset then only finishes a pending local destroy()).
   * @internal
   */
  abort(code: number | string, message = ""): void {
    if (this.applyReset(code, message)) {
      this.emit("reset", { code: this.resetCode!, message: this.terminalError!.message });
    }
  }

  /** Shared implementation behind reset()/abort(); returns false (nothing sent) if the stream was already closed. */
  private applyReset(code: number | string, message: string): boolean {
    if (this.state === "closed") {
      // Nothing left to RESET on the wire, but the stream may be closed and
      // not yet destroy()'d: terminateNoThrow holding write callbacks back
      // for the read side to drain (see its doc comment), or a clean CLOSE
      // both ways waiting on 'end' (destroyWhenDone). reset() discards
      // buffered data and flushes held callbacks, so finish it here.
      if (!this.destroyed) this.destroy();
      return false;
    }
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
    this.terminateNoThrow(this.terminalError, { discardBuffered: true });
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
