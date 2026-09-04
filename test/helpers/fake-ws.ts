/**
 * A deterministic fake WSLike transport for unit and sequence tests: no real
 * sockets, records every frame written (for exact-interleaving assertions on
 * the round-robin writer), and lets a test inject inbound messages directly.
 */
import { EventEmitter } from "node:events";
import type { WSLike } from "../../src/conn.js";

export class FakeWS extends EventEmitter implements WSLike {
  readonly sent: Uint8Array[] = [];
  bufferedAmount = 0;
  readyState = 1; // OPEN
  closedWith: { code?: number; reason?: string } | null = null;
  private pendingSendFailure: Error | null = null;

  /** Test helper: makes the next `send()` call's callback report `err` instead of succeeding. */
  failNextSend(err: Error): void {
    this.pendingSendFailure = err;
  }

  send(data: Uint8Array, cb?: (err?: Error) => void): void {
    this.sent.push(data.slice());
    if (this.pendingSendFailure) {
      const err = this.pendingSendFailure;
      this.pendingSendFailure = null;
      queueMicrotask(() => cb?.(err));
      return;
    }
    // Simulate the frame reaching the wire (and bufferedAmount draining) on
    // the next microtask, so writeRaw's backpressure loop has something real
    // to observe if a test wants to exercise it.
    queueMicrotask(() => cb?.());
  }

  close(code?: number, reason?: string): void {
    this.closedWith = { code, reason };
    this.readyState = 3; // CLOSED
    queueMicrotask(() => this.emit("close", code ?? 1000, Buffer.from(reason ?? "")));
  }

  terminate(): void {
    this.close(1006, "terminated");
  }

  /** Test helper: deliver an inbound binary message as if it arrived from the peer. */
  receive(data: Uint8Array): void {
    this.emit("message", data, true);
  }
}
