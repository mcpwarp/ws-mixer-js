/**
 * Dial-level fake transport for MixerClient tests (reconnect, drain
 * hand-over): a WHATWG-shaped socket the test opens/rejects/closes by hand,
 * plus a `webSocket` factory that records every socket and, by default,
 * auto-answers `hello` with `welcome`. Every test using it runs under
 * `vi.useFakeTimers()`.
 */
import { EventEmitter } from "node:events";
import { vi } from "vitest";
import { SUBPROTOCOL } from "../../src/client.js";
import { encodeControl, type ControlMessage } from "../../src/control.js";
import { decodeFrame, encodeData, FrameType } from "../../src/frame.js";
import type { WebSocketFactory, WebSocketInit, WebSocketLike } from "../../src/socket.js";

/**
 * A fake WHATWG-shaped socket (`WebSocketLike`), driven entirely by the
 * test: `open()`, `httpReject()` and `netError()` resolve/reject the dial
 * the same way `dialWebSocket` in src/socket.ts expects; `push()` delivers
 * an inbound frame once a MixerConn takes over. `send()` optionally forwards
 * every outbound frame to `onSend` so a test can script an auto-`welcome`
 * (or any other scripted server behaviour) without hand-rolling frame bytes
 * twice. Internally a node:events emitter, so a test can also `emit()` raw
 * `close(code, reason)` / `error(err)` / `message(bytes)` events, which
 * `addEventListener` listeners receive as WHATWG-shaped event objects.
 */
export class FakeSocket extends EventEmitter implements WebSocketLike {
  bufferedAmount = 0;
  readyState = 0; // CONNECTING
  protocol = "";
  binaryType = "blob";
  upgradeFailure?: { status: number; headers?: Record<string, string | undefined> };
  readonly sent: Uint8Array[] = [];
  closedWith: { code?: number; reason?: string } | null = null;
  onSend: ((data: Uint8Array) => void) | null = null;
  /** The `Authorization` header this dial was opened with (item 4: proves a fresh token is used per dial). */
  authHeader: string | undefined = undefined;
  /** Every header this dial was opened with. */
  headers: Record<string, string> = {};
  /** The `hello.token` this dial actually sent on the wire (item 4). */
  helloToken: string | undefined = undefined;
  private readonly adapted = new Map<(ev: any) => void, (...args: any[]) => void>();

  send(data: Uint8Array): void {
    this.sent.push(data);
    this.onSend?.(data);
  }

  close(code?: number, reason?: string): void {
    if (this.closedWith) return;
    this.closedWith = { code, reason };
    this.readyState = 3; // CLOSED
    queueMicrotask(() => this.emit("close", code ?? 1000, Buffer.from(reason ?? "")));
  }

  addEventListener(type: "open" | "message" | "error" | "close", listener: (ev: any) => void): void {
    const wrapped =
      type === "message"
        ? (data: Uint8Array) => listener({ data: data.slice().buffer })
        : type === "close"
          ? (code: number, reason?: unknown) => listener({ code, reason: reason === undefined ? "" : String(reason) })
          : type === "error"
            ? (err?: Error) => listener({ message: err?.message ?? "", error: err })
            : () => listener({});
    this.adapted.set(listener, wrapped);
    this.on(type, wrapped);
  }

  removeEventListener(type: "open" | "message" | "error" | "close", listener: (ev: any) => void): void {
    const wrapped = this.adapted.get(listener);
    if (wrapped) this.off(type, wrapped);
  }

  /** Test driver: the dial succeeds, `ws-mixer.v1` echoed by default. */
  open(protocol: string = SUBPROTOCOL): void {
    this.readyState = 1; // OPEN
    this.protocol = protocol;
    this.emit("open");
  }

  /** Test driver: the dial fails at the HTTP layer (401/403/404/429/anything else), as a factory that exposes `upgradeFailure` reports it. */
  httpReject(statusCode: number, headers: Record<string, string> = {}): void {
    this.upgradeFailure = { status: statusCode, headers };
    this.readyState = 3;
    this.emit("error", new Error(`Unexpected server response: ${statusCode}`));
    this.emit("close", 1006, Buffer.from(""));
  }

  /** Test driver: the dial fails at the transport layer (DNS, ECONNREFUSED, ...). */
  netError(err: Error): void {
    this.readyState = 3;
    this.emit("error", err);
    this.emit("close", 1006, Buffer.from(""));
  }

  /** Test driver: deliver an inbound binary WS message as if it came from the peer. */
  push(frame: Uint8Array): void {
    this.emit("message", frame);
  }

  /** Test driver: the server (or the network) closes the socket after the handshake. */
  serverClose(code: number, reason = ""): void {
    this.close(code, reason);
  }
}

/** Builds a `welcome` control frame and pushes it to `socket`, for tests that disable `autoWelcome` and drive the handshake by hand. */
export function pushWelcome(socket: FakeSocket, overrides: Partial<ControlMessage> = {}): void {
  const welcome = {
    t: "welcome",
    v: 1,
    session: "test-session",
    window: 262144,
    max_streams: 64,
    ping_interval: 30000,
    ping_timeout: 90000,
    ...overrides,
  } as ControlMessage;
  socket.push(encodeData(0, encodeControl(welcome)));
}

/** Builds a `webSocket` factory that records every fake socket it creates and, by default, auto-answers `hello` with `welcome` so tests can focus on the reconnect policy instead of hand-driving the handshake every time. */
export function makeFactory(
  sockets: FakeSocket[],
  opts: { autoWelcome?: boolean; welcomeOverrides?: Partial<ControlMessage> } = {},
): WebSocketFactory {
  const autoWelcome = opts.autoWelcome ?? true;
  return (_url: string, init: WebSocketInit) => {
    const socket = new FakeSocket();
    // Captured regardless of autoWelcome, so every test -- not just the ones
    // that hand-drive the handshake -- can assert a fresh token is used on
    // every dial (item 4).
    socket.authHeader = init.headers.Authorization;
    socket.headers = init.headers;
    sockets.push(socket);
    socket.onSend = (data) => {
      let frame;
      try {
        frame = decodeFrame(data);
      } catch {
        return;
      }
      if (frame.type !== FrameType.DATA || frame.streamId !== 0) return;
      let msg: ControlMessage;
      try {
        msg = JSON.parse(Buffer.from(frame.payload).toString("utf8"));
      } catch {
        return;
      }
      if (msg.t !== "hello") return;
      socket.helloToken = msg.token;
      if (!autoWelcome) return;
      queueMicrotask(() => pushWelcome(socket, opts.welcomeOverrides));
    };
    return socket;
  };
}

/**
 * Drains microtasks (queued welcome replies, promise chains) and flushes any
 * zero-delay timers along the way. `vi.advanceTimersByTimeAsync(0)` (every
 * test using this runs under `vi.useFakeTimers()`) flushes both.
 *
 * Note: under fake timers, a timer scheduled with a delay under 1ms is
 * indistinguishable from "already due" and fires inside this zero-advance
 * flush -- so a test must not assert "this timer has not fired yet" for an
 * unpinned `Math.random()`-jittered delay unless it pins `Math.random` first.
 */
export async function tick(n = 5): Promise<void> {
  for (let i = 0; i < n; i++) {
    await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
  }
}

/** Waits for a socket to exist at `sockets[index]`, then opens it and lets its handshake complete. */
export async function connectSocket(sockets: FakeSocket[], index: number): Promise<FakeSocket> {
  await tick();
  const s = sockets[index]!;
  s.open();
  await tick();
  return s;
}
