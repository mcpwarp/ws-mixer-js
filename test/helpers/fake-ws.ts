/**
 * A deterministic fake WHATWG-shaped WebSocket (`WebSocketLike`) for unit
 * and sequence tests: no real sockets, records every frame written (for
 * exact-interleaving assertions on the round-robin writer), and lets a test
 * inject inbound messages and peer closes directly.
 */
import type { WebSocketLike } from "../../src/socket.js";

type Listener = (ev: any) => void;

export class FakeWS implements WebSocketLike {
  readonly sent: Uint8Array[] = [];
  bufferedAmount = 0;
  readyState = 1; // OPEN
  protocol = "ws-mixer.v1";
  binaryType = "arraybuffer";
  closedWith: { code?: number; reason?: string } | null = null;
  private readonly listeners = new Map<string, Set<Listener>>();
  private pendingSendFailure: Error | null = null;

  /** Test helper: makes the next `send()` call throw `err` (the frame is still recorded in `sent`). */
  failNextSend(err: Error): void {
    this.pendingSendFailure = err;
  }

  send(data: Uint8Array): void {
    this.sent.push(data.slice());
    if (this.pendingSendFailure) {
      const err = this.pendingSendFailure;
      this.pendingSendFailure = null;
      throw err;
    }
  }

  /** This side's close: recorded, then a `close` event on a later microtask (ignored by a MixerConn that initiated it). */
  close(code?: number, reason?: string): void {
    if (this.closedWith) return;
    this.closedWith = { code, reason };
    this.readyState = 3; // CLOSED
    queueMicrotask(() => this.dispatch("close", { code: code ?? 1000, reason: reason ?? "" }));
  }

  addEventListener(type: string, listener: Listener): void {
    let set = this.listeners.get(type);
    if (!set) {
      set = new Set();
      this.listeners.set(type, set);
    }
    set.add(listener);
  }

  removeEventListener(type: string, listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  /** Test helper: deliver an inbound binary message as if it arrived from the peer. */
  receive(data: Uint8Array): void {
    this.dispatch("message", { data: data.slice().buffer });
  }

  /** Test helper: deliver `data` as the message event's `data` verbatim (e.g. an ArrayBuffer from another realm). */
  receiveRaw(data: unknown): void {
    this.dispatch("message", { data });
  }

  /** Test helper: the peer (or the network) closes the socket, synchronously. */
  serverClose(code: number, reason = ""): void {
    this.readyState = 3;
    this.dispatch("close", { code, reason });
  }

  /** Test helper: a socket-level `error` event. */
  emitError(message: string): void {
    this.dispatch("error", { message });
  }

  private dispatch(type: string, ev: unknown): void {
    for (const l of [...(this.listeners.get(type) ?? [])]) l(ev);
  }
}
