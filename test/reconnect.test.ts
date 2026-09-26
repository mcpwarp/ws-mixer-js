/**
 * Reconnect/backoff policy from docs/research/2026-08-26-control-channel-and-connection-lifecycle.md
 * and WIRE.md section 2.9's reconnect table, exercised end-to-end
 * against `MixerClient` with fake timers and a fake `ws`-shaped transport
 * injected via the test-only `_wsFactory` option -- no real sockets, no
 * real Go server (that's test/interop.test.ts's job).
 */
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MixerClient,
  SUBPROTOCOL,
  fullJitterDelay,
  type DialSocket,
  type WSFactory,
  type DisconnectPayload,
} from "../src/client.js";
import { encodeControl, type ControlMessage } from "../src/control.js";
import { decodeFrame, encodeClose, encodeData, encodeOpen, FrameType } from "../src/frame.js";
import { ErrorCode, StreamError, TokenUnavailableError, WsMixerError } from "../src/errors.js";
import type { MixerStream } from "../src/stream.js";

/** Asserts the invariant blocker 3 requires: `currentState() === "connected"` never coincides with `currentConn() === null`. */
function assertConnInvariant(client: MixerClient): void {
  if (client.currentState() === "connected") {
    expect(client.currentConn()).not.toBeNull();
  }
}

/** Builds a `welcome` control frame and pushes it to `socket`, for tests that disable `autoWelcome` and drive the handshake by hand. */
function pushWelcome(socket: FakeSocket, overrides: Partial<ControlMessage> = {}): void {
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

// --- fake transport ----------------------------------------------------------

/**
 * A fake `ws`-shaped socket, driven entirely by the test: `open()`,
 * `httpReject()` and `netError()` resolve/reject the dial the same way
 * `dialWebSocket` in src/client.ts expects; `push()` delivers an inbound
 * frame once a MixerConn takes over. `send()` optionally forwards every
 * outbound frame to `onSend` so a test can script an auto-`welcome` (or any
 * other scripted server behaviour) without hand-rolling frame bytes twice.
 */
class FakeSocket extends EventEmitter implements DialSocket {
  bufferedAmount = 0;
  readyState = 0; // CONNECTING
  protocol = "";
  readonly sent: Uint8Array[] = [];
  closedWith: { code?: number; reason?: string } | null = null;
  onSend: ((data: Uint8Array) => void) | null = null;
  /** The `Authorization` header this dial was opened with (item 4: proves a fresh token is used per dial). */
  authHeader: string | undefined = undefined;
  /** The `hello.token` this dial actually sent on the wire (item 4). */
  helloToken: string | undefined = undefined;

  send(data: Uint8Array, cb?: (err?: Error) => void): void {
    this.sent.push(data);
    cb?.();
    this.onSend?.(data);
  }

  close(code?: number, reason?: string): void {
    if (this.closedWith) return;
    this.closedWith = { code, reason };
    this.readyState = 3; // CLOSED
    queueMicrotask(() => this.emit("close", code ?? 1000, Buffer.from(reason ?? "")));
  }

  terminate(): void {
    this.close(1006, "terminated");
  }

  /** Test driver: the dial succeeds, `ws-mixer.v1` echoed by default. */
  open(protocol: string = SUBPROTOCOL): void {
    this.readyState = 1; // OPEN
    this.protocol = protocol;
    this.emit("open");
  }

  /** Test driver: the dial fails at the HTTP layer (401/403/404/429/anything else). */
  httpReject(statusCode: number, headers: Record<string, string> = {}): void {
    this.emit("unexpected-response", {}, { statusCode, headers });
  }

  /** Test driver: the dial fails at the transport layer (DNS, ECONNREFUSED, ...). */
  netError(err: Error): void {
    this.emit("error", err);
  }

  /** Test driver: deliver an inbound binary WS message as if it came from the peer. */
  push(frame: Uint8Array): void {
    this.emit("message", frame, true);
  }

  /** Test driver: the server (or the network) closes the socket after the handshake. */
  serverClose(code: number, reason = ""): void {
    this.close(code, reason);
  }
}

/** Builds a `_wsFactory` that records every fake socket it creates and, by default, auto-answers `hello` with `welcome` so tests can focus on the reconnect policy instead of hand-driving the handshake every time. */
function makeFactory(
  sockets: FakeSocket[],
  opts: { autoWelcome?: boolean; welcomeOverrides?: Partial<ControlMessage> } = {},
): WSFactory {
  const autoWelcome = opts.autoWelcome ?? true;
  return (_url, _protocols, options) => {
    const socket = new FakeSocket();
    // Captured regardless of autoWelcome, so every test -- not just the ones
    // that hand-drive the handshake -- can assert a fresh token is used on
    // every dial (item 4).
    socket.authHeader = options.headers.Authorization;
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
      queueMicrotask(() => {
        const welcome = {
          t: "welcome",
          v: 1,
          session: "test-session",
          window: 262144,
          max_streams: 64,
          ping_interval: 30000,
          ping_timeout: 90000,
          ...opts.welcomeOverrides,
        } as ControlMessage;
        socket.push(encodeData(0, encodeControl(welcome)));
      });
    };
    return socket;
  };
}

/**
 * Drains microtasks (queued welcome replies, promise chains) and flushes any
 * zero-delay timers/`process.nextTick` work along the way -- Node's stream
 * `destroy()` machinery schedules its `'error'`/`'close'` emission a couple
 * of nextTick/immediate hops deep, which plain `await Promise.resolve()`
 * chains alone don't reliably drain. `vi.advanceTimersByTimeAsync(0)` (every
 * test here runs under `vi.useFakeTimers()`) flushes both.
 *
 * Note: under fake timers, a timer scheduled with a delay under 1ms is
 * indistinguishable from "already due" and fires inside this zero-advance
 * flush -- so a test must not assert "this timer has not fired yet" for an
 * unpinned `Math.random()`-jittered delay unless it pins `Math.random` first.
 */
async function tick(n = 5): Promise<void> {
  for (let i = 0; i < n; i++) {
    await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
  }
}

/** Waits for a socket to exist at `sockets[index]`, then opens it and lets its handshake complete. */
async function connectSocket(sockets: FakeSocket[], index: number): Promise<FakeSocket> {
  await tick();
  const s = sockets[index]!;
  s.open();
  await tick();
  return s;
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

// --- backoff math (pure function, no client needed) --------------------------

describe("fullJitterDelay", () => {
  it("is always within [0, min(cap, base*2^attempt)]", () => {
    for (let attempt = 0; attempt < 10; attempt++) {
      for (let i = 0; i < 50; i++) {
        const d = fullJitterDelay(attempt, 1000, 60000);
        expect(d).toBeGreaterThanOrEqual(0);
        expect(d).toBeLessThanOrEqual(Math.min(60000, 1000 * 2 ** attempt));
      }
    }
  });

  it("is capped at 60000ms even for large attempt counts", () => {
    for (let i = 0; i < 50; i++) {
      const d = fullJitterDelay(30, 1000, 60000);
      expect(d).toBeLessThanOrEqual(60000);
    }
  });

  it("grows the ceiling exponentially with attempt, before the cap", () => {
    // Not flaky: we assert on the ceiling (a pure function of attempt), not on the jittered sample.
    expect(Math.min(60000, 1000 * 2 ** 0)).toBe(1000);
    expect(Math.min(60000, 1000 * 2 ** 3)).toBe(8000);
    expect(Math.min(60000, 1000 * 2 ** 10)).toBe(60000);
  });
});

describe("MixerClient reconnect.stableAfter validation", () => {
  for (const bad of [NaN, -1, Infinity, -Infinity]) {
    it(`throws a RangeError synchronously for stableAfter: ${bad}`, () => {
      expect(() => new MixerClient("wss://x/tunnel", { token: "t", reconnect: { stableAfter: bad } })).toThrow(
        RangeError,
      );
    });
  }

  it("accepts stableAfter: 0", () => {
    expect(() => new MixerClient("wss://x/tunnel", { token: "t", reconnect: { stableAfter: 0 } })).not.toThrow();
  });
});

// --- end-to-end reconnect policy against a fake transport ---------------------

describe("MixerClient reconnect policy", () => {
  it("does NOT reset the attempt counter on welcome alone: a welcome-then-immediate-close (well before stableAfter) keeps counting", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number; cause: string }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000, stableAfter: 10000 },
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    // First dial fails outright (before any welcome): attempt -> 1.
    await tick();
    sockets[0]!.netError(new Error("ECONNREFUSED"));
    await tick();
    expect(reconnecting).toHaveLength(1);
    expect(reconnecting[0]!.attempt).toBe(1);

    // Let the scheduled backoff fire (advance by exactly its delay, not a
    // blind 60s -- a bigger jump would also fire the new dial's own 10s
    // connectTimeout before we get a chance to open() it) and this time
    // succeed all the way to welcome.
    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    await connectSocket(sockets, 1);
    await started;

    // Immediately (well inside stableAfter's 10s window) drop again: since
    // this conn never proved stable, `attempt` must NOT have reset --
    // 0.3.1's "reset on welcome" rule is gone (Change 2).
    sockets[1]!.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting).toHaveLength(2);
    expect(reconnecting[1]!.attempt).toBe(2);

    await client.close();
  });

  it("close 4012 (GOING_AWAY) reconnects immediately, jitter 0-2000ms only, with a fresh token", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const client = new MixerClient("wss://x/tunnel", { token: provider, _wsFactory: makeFactory(sockets) });
    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(4000 + ErrorCode.GOING_AWAY, "draining: rollout");
    await tick();
    // No 60s wait needed: the retry dial happens well inside a 2s window.
    await vi.advanceTimersByTimeAsync(2000);
    expect(sockets.length).toBe(2);
    // The immediate reconnect re-invoked the provider and used its fresh
    // token, both in the handshake headers and on the wire (item 4).
    expect(sockets[1]!.authHeader).toBe("Bearer token-2");
    await connectSocket(sockets, 1);
    expect(sockets[1]!.helloToken).toBe("token-2");

    await client.close();
  });

  it("close 1001 is treated the same as 4012: immediate reconnect", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });
    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(1001, "going away");
    await tick();
    await vi.advanceTimersByTimeAsync(2000);
    expect(sockets.length).toBe(2);

    await client.close();
  });

  it("close 4013 (KEEPALIVE_TIMEOUT) gets exactly one immediate retry, then normal backoff", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number; cause: string }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
    });
    client.on("reconnecting", (info) => reconnecting.push(info));
    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await connectSocket(sockets, 0);
    await started;

    // First 4013: immediate retry, no backoff delay, so no 'reconnecting' event.
    sockets[0]!.serverClose(4000 + ErrorCode.KEEPALIVE_TIMEOUT, "no pong for 92s");
    await tick();
    expect(sockets.length).toBe(2);
    expect(reconnecting).toHaveLength(0);

    // That single immediate retry itself fails to connect at all: the "one
    // immediate attempt" is used up, so this falls back to normal jittered
    // backoff rather than retrying immediately again.
    sockets[1]!.netError(new Error("ECONNRESET"));
    await tick();
    expect(sockets.length).toBe(2); // no third socket until the backoff timer fires
    expect(reconnecting).toHaveLength(1);
    expect(reconnecting[0]!.cause).toContain("ECONNRESET");
    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    expect(sockets.length).toBe(3);

    await client.close();
  });

  it("close 4013 (KEEPALIVE_TIMEOUT) with maxAttempts:0 gives up on the immediate retry instead of dialing a second socket", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });
    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(4000 + ErrorCode.KEEPALIVE_TIMEOUT, "no pong for 92s");
    await tick();
    await vi.advanceTimersByTimeAsync(120000); // give a buggy implementation every chance to retry

    expect(sockets.length).toBe(1); // no second socket: the immediate retry is subject to the ceiling too
    // Exhaustion (giveUp), not a wire-level fatal close, so exactly one
    // merged `fatal: true` onDisconnect report -- no separate `fatal` event
    // (that's goFatal's job, for a real UNSUPPORTED/UNAUTHORIZED close).
    expect(disconnects).toHaveLength(1);
    expect(disconnects.at(-1)?.fatal).toBe(true);
    expect(disconnects.at(-1)?.message).toContain("max reconnect attempts");
  });

  it("close 4009 (ENHANCE_YOUR_CALM) starts backoff at the cap, not at base", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // pin full-jitter's sample to its ceiling
    const sockets: FakeSocket[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number; cause: string }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
    });
    client.on("reconnecting", (info) => reconnecting.push(info));
    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(4000 + ErrorCode.ENHANCE_YOUR_CALM, "excessive load; back off");
    await tick();
    expect(reconnecting).toHaveLength(1);
    // Math.random pinned to 1: a first-attempt normal backoff would ceiling
    // at base*2^1 = 2000ms; starting "at the cap" instead ceilings at 60000ms.
    expect(reconnecting[0]!.delayMs).toBe(60000);

    await client.close();
  });

  for (const code of [4000 + ErrorCode.UNSUPPORTED, 4000 + ErrorCode.UNAUTHORIZED]) {
    it(`close ${code} is fatal: no reconnect, 'fatal' fires, onDisconnect({fatal:true}), and the 'fatal' event's error code matches the disconnect payload's derived errorCode (not INTERNAL_ERROR)`, async () => {
      vi.useFakeTimers();
      const sockets: FakeSocket[] = [];
      const fatalEvents: WsMixerError[] = [];
      const disconnects: DisconnectPayload[] = [];
      const client = new MixerClient("wss://x/tunnel", {
        token: "t",
        _wsFactory: makeFactory(sockets),
        onDisconnect: (r) => disconnects.push(r),
      });
      client.on("fatal", (e) => fatalEvents.push(e));
      const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
      await connectSocket(sockets, 0);
      await started;

      sockets[0]!.serverClose(code, "version/capability mismatch");
      await tick();
      await vi.advanceTimersByTimeAsync(120000); // give a buggy implementation every chance to retry

      expect(sockets.length).toBe(1); // never retried
      expect(fatalEvents).toHaveLength(1);
      expect(disconnects.at(-1)?.fatal).toBe(true);
      // A bare close (no preceding error{}) still derives errorCode from the
      // wire code -- the 'fatal' event's WsMixerError.code must match it,
      // not fall back to INTERNAL_ERROR just because MixerConn itself never
      // set info.errorCode.
      const derivedErrorCode = code - 4000;
      expect(disconnects.at(-1)?.errorCode).toBe(derivedErrorCode);
      expect(fatalEvents[0]!.code).toBe(derivedErrorCode);
    });
  }

  it("a missing/mismatched subprotocol echo is fatal: errorCode UNSUPPORTED, no wsCode, no httpStatus -- the SDK itself raised this, not a ws-mixer close code or an HTTP rejection", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const fatalEvents: unknown[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open("some-other-protocol");
    await tick();
    await vi.advanceTimersByTimeAsync(120000); // give a buggy implementation every chance to retry

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1); // never retried
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "dial",
      fatal: true,
      errorCode: ErrorCode.UNSUPPORTED,
      errorName: "UNSUPPORTED",
      wsCode: undefined,
      httpStatus: undefined,
    });
  });

  it("HTTP 401 during the initial dial is fatal: connect() rejects, never retries", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await tick();
    sockets[0]!.httpReject(401);
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1);
    expect(fatalEvents).toHaveLength(1);
  });

  it("HTTP 429 with Retry-After is honoured verbatim instead of the usual jitter", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
    });
    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await tick();
    sockets[0]!.httpReject(429, { "retry-after": "5" });
    await tick();

    await vi.advanceTimersByTimeAsync(4999);
    expect(sockets.length).toBe(1); // not yet -- Retry-After: 5s hasn't elapsed
    await vi.advanceTimersByTimeAsync(1);
    expect(sockets.length).toBe(2);

    await connectSocket(sockets, 1);
    await started;
    await client.close();
  });

  it("maxAttempts exhaustion stops reconnecting and rejects connect() if never connected", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1, cap: 10, maxAttempts: 2 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await tick();
    sockets[0]!.netError(new Error("refused")); // initial dial: attempt -> 1
    await tick();
    await vi.advanceTimersByTimeAsync(10);
    sockets[1]!.netError(new Error("refused")); // retry 1: attempt -> 2
    await tick();
    await vi.advanceTimersByTimeAsync(10);
    sockets[2]!.netError(new Error("refused")); // retry 2: attempt(2) >= maxAttempts(2) -> give up
    await tick();

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(3);
    // Exactly one report for this disconnect: the failure that triggered
    // exhaustion and the exhaustion itself are the same event, merged into
    // one `fatal: true` report carrying the exhaustion message (blocker 1) --
    // not two separate onDisconnect calls.
    expect(disconnects).toHaveLength(3);
    expect(disconnects.at(-1)?.fatal).toBe(true);
    expect(disconnects.at(-1)?.message).toContain("max reconnect attempts");

    // Confirm it really stopped: no fourth socket even after a long wait.
    await vi.advanceTimersByTimeAsync(120000);
    expect(sockets.length).toBe(3);
  });

  it("close() during an in-flight dial closes the socket once it resolves and never reconnects", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });

    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await tick();
    const closed = client.close();
    // The dial is still in flight (no open()/reject yet) when close() is called.
    sockets[0]!.open();
    await tick();

    await expect(started).rejects.toBeTruthy();
    await closed;
    expect(sockets[0]!.closedWith).not.toBeNull();
    expect(sockets[0]!.sent).toHaveLength(0); // no hello was ever sent: no MixerConn was created for this dial

    await vi.advanceTimersByTimeAsync(120000);
    expect(sockets.length).toBe(1); // never reconnected
  });

  it("errors every in-flight stream when the connection drops for reconnect", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });
    const streamErrors: Error[] = [];
    client.on("stream", (s) => s.on("error", (e: Error) => streamErrors.push(e)));

    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(encodeOpen(1)); // server opens a stream
    await tick();

    s0.serverClose(1006, "abnormal");
    await tick();

    expect(streamErrors).toHaveLength(1);

    await client.close();
  });

  it("never has more than one socket open at a time across a run of plain backoff reconnects", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 100, cap: 1000 },
    });

    const openCount = (): number => sockets.filter((s) => s.readyState === 1 && !s.closedWith).length;

    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await connectSocket(sockets, 0);
    await started;
    expect(openCount()).toBeLessThanOrEqual(1);

    for (let i = 0; i < 4; i++) {
      sockets.at(-1)!.serverClose(1006, "abnormal");
      await tick();
      expect(openCount()).toBeLessThanOrEqual(1);
      await vi.advanceTimersByTimeAsync(1000);
      expect(openCount()).toBeLessThanOrEqual(1);
      const s = await connectSocket(sockets, sockets.length - 1);
      expect(openCount()).toBeLessThanOrEqual(1);
      void s;
    }

    await client.close();
  });
});

// --- Change 2: the attempt counter (and the once-only budgets it gates) ------
// resets only once a connection has stayed up `stableAfter` ms past welcome,
// never on welcome itself ------------------------------------------------------

describe("MixerClient stability (reconnect.stableAfter)", () => {
  it("welcome-then-immediate-close, repeated: 'reconnecting' delays keep climbing instead of resetting every cycle", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // pin full-jitter's sample to its ceiling for an exact sequence
    const sockets: FakeSocket[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000, stableAfter: 10000 },
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    // Three welcome-then-immediate-close cycles, each well inside the 10s
    // stability window: with Math.random pinned to 1, the ceiling itself is
    // the delay, so this pins the exact climbing sequence up to the cap.
    const expected = [2000, 4000, 8000];
    for (let i = 0; i < expected.length; i++) {
      sockets.at(-1)!.serverClose(1006, "abnormal");
      await tick();
      expect(reconnecting).toHaveLength(i + 1);
      expect(reconnecting[i]!.attempt).toBe(i + 1);
      expect(reconnecting[i]!.delayMs).toBe(expected[i]);
      await vi.advanceTimersByTimeAsync(expected[i]!);
      await connectSocket(sockets, sockets.length - 1);
      await tick();
    }

    await client.close();
  });

  it("does NOT reset the delay before stableAfter elapses, but does once a connection survives it (distinguishes from reset-on-welcome: see the revert-proof test below)", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const sockets: FakeSocket[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000, stableAfter: 10000 },
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    // One unstable cycle first, to bump attempt off zero.
    sockets.at(-1)!.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting[0]!.delayMs).toBe(2000); // base*2^1
    await vi.advanceTimersByTimeAsync(2000);
    await connectSocket(sockets, sockets.length - 1);
    await tick();

    // Drop again almost immediately -- well short of stableAfter (10000ms).
    // A reset-on-welcome implementation (0.3.1's rule) would already have
    // reset `attempt` to 0 the instant that last `welcome` landed, so this
    // delay would be back at 2000ms; the real rule (reset only at
    // stability) instead keeps climbing.
    await vi.advanceTimersByTimeAsync(1000);
    sockets.at(-1)!.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting).toHaveLength(2);
    expect(reconnecting[1]!.attempt).toBe(2);
    expect(reconnecting[1]!.delayMs).toBe(4000); // base*2^2 -- NOT reset
    await vi.advanceTimersByTimeAsync(4000);
    await connectSocket(sockets, sockets.length - 1);
    await tick();

    // This time, let it stay up for the full stability window before dropping.
    await vi.advanceTimersByTimeAsync(10000);
    sockets.at(-1)!.serverClose(1006, "abnormal");
    await tick();

    expect(reconnecting).toHaveLength(3);
    // Back at the first step (base*2^1), not base*2^3 -- stability reset attempt to 0.
    expect(reconnecting[2]!.attempt).toBe(1);
    expect(reconnecting[2]!.delayMs).toBe(2000);

    await vi.advanceTimersByTimeAsync(2000);
    await client.close();
  });

  it("a drain hand-over's retiring conn closing after its replacement's welcome does not clear (or falsely reset) the replacement's own timer -- the replacement still needs its own full stableAfter before resetting", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const sockets: FakeSocket[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000, stableAfter: 10000 },
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    // Bump attempt off zero with one unstable cycle first, so a later reset
    // is actually observable.
    s0.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting[0]!.delayMs).toBe(2000);
    await vi.advanceTimersByTimeAsync(2000);
    const s1 = await connectSocket(sockets, 1);
    await tick();

    // Drain immediately (well before s1 itself would ever prove stable): a
    // parallel reconnect starts (connectOnce's own delayMs>0 branch emits its
    // own 'reconnecting', unrelated to `attempt`), its own welcome arms ITS
    // own timer, and (per connectOnce's success path) s1 is torn down
    // (old.fail()) right away -- s1's close must NOT clear (or, worse,
    // falsely appear to reset) s2's freshly-armed timer.
    s1.push(encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 0 } as ControlMessage)));
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // drain's own jitter window
    await connectSocket(sockets, 2);
    await tick();
    expect(s1.closedWith).not.toBeNull(); // superseded, torn down synchronously

    // Drop s2 almost immediately -- well short of ITS OWN stableAfter. If
    // s1's earlier close (or the drain hand-over itself) had wrongly reset
    // anything, this delay would be back at the first step (2000ms); it must
    // instead still be climbing from the pre-drain attempt count.
    await vi.advanceTimersByTimeAsync(1000);
    sockets[2]!.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting).toHaveLength(3);
    expect(reconnecting[2]!.attempt).toBe(2);
    expect(reconnecting[2]!.delayMs).toBe(4000); // NOT reset
    await vi.advanceTimersByTimeAsync(4000);
    await connectSocket(sockets, 3);
    await tick();

    // NOW let this connection actually stay up for its own full
    // stableAfter: the reset genuinely happens once elapsed time earns it,
    // not merely because "some welcome happened somewhere".
    await vi.advanceTimersByTimeAsync(10000);
    sockets[3]!.serverClose(1006, "abnormal");
    await tick();

    expect(reconnecting).toHaveLength(4);
    expect(reconnecting[3]!.attempt).toBe(1);
    expect(reconnecting[3]!.delayMs).toBe(2000);

    await vi.advanceTimersByTimeAsync(2000);
    await client.close();
  });

  it("4013's one-shot immediate retry does not un-spend itself on the second 4013: four consecutive 4013s before stability go immediate, backoff, backoff, backoff (climbing), then immediate again once stable", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // pin full-jitter's sample to its ceiling for an exact sequence
    const sockets: FakeSocket[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000, stableAfter: 10000 },
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    // 4013 #1: the one-shot immediate retry -- no backoff, no 'reconnecting'.
    sockets[0]!.serverClose(4000 + ErrorCode.KEEPALIVE_TIMEOUT, "no pong");
    await tick();
    expect(sockets.length).toBe(2);
    expect(reconnecting).toHaveLength(0);
    await connectSocket(sockets, 1);
    await tick();

    // 4013 #2, well before stability: the BLOCKER this test exists to catch
    // -- a buggy implementation that un-spends the budget right back to
    // "unused" on this very close (instead of only at stability) would
    // immediately retry AGAIN here, keeping sockets.length at 2 forever
    // instead of ever backing off. It must back off instead, with a
    // climbing delay like any other normal-backoff disconnect.
    sockets[1]!.serverClose(4000 + ErrorCode.KEEPALIVE_TIMEOUT, "no pong again");
    await tick();
    expect(sockets.length).toBe(2); // no third socket yet
    expect(reconnecting).toHaveLength(1);
    expect(reconnecting[0]!.delayMs).toBe(4000); // attempt 2: base*2^2
    await vi.advanceTimersByTimeAsync(4000);
    await connectSocket(sockets, 2);
    await tick();

    // 4013 #3: still no immediate retry (budget stays spent), delay keeps climbing.
    sockets[2]!.serverClose(4000 + ErrorCode.KEEPALIVE_TIMEOUT, "no pong a third time");
    await tick();
    expect(sockets.length).toBe(3);
    expect(reconnecting).toHaveLength(2);
    expect(reconnecting[1]!.delayMs).toBe(8000); // attempt 3: base*2^3
    await vi.advanceTimersByTimeAsync(8000);
    await connectSocket(sockets, 3);
    await tick();

    // This connection survives stability: attempt AND the 4013 budget reset.
    await vi.advanceTimersByTimeAsync(10000);

    // 4013 #4: immediate again, exactly like the very first one.
    sockets[3]!.serverClose(4000 + ErrorCode.KEEPALIVE_TIMEOUT, "no pong once more");
    await tick();
    expect(sockets.length).toBe(5);
    expect(reconnecting).toHaveLength(2); // still no 'reconnecting' for this immediate retry

    await client.close();
  });

  it("maxAttempts counts consecutive reconnects without a stable connection in between: welcome-then-close cycles exhaust it and go fatal", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1, cap: 10, maxAttempts: 3, stableAfter: 10000 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    // Three welcome-then-close cycles, each redialing successfully but never
    // staying up long enough to prove stable -- `attempt` climbs 1, 2, 3
    // across them (maxAttempts:3 hasn't been reached YET after the third:
    // the exhaustion check runs before the increment, so the third close
    // still gets its redial).
    for (let i = 0; i < 3; i++) {
      sockets.at(-1)!.serverClose(1006, "abnormal");
      await tick();
      await vi.advanceTimersByTimeAsync(10);
      await connectSocket(sockets, sockets.length - 1);
      await tick();
    }
    expect(client.currentState()).toBe("connected");

    // The fourth close is the one that finds attempt(3) >= maxAttempts(3):
    // give up instead of redialing again.
    sockets.at(-1)!.serverClose(1006, "abnormal");
    await tick();

    expect(client.currentState()).toBe("closed");
    expect(disconnects.at(-1)?.fatal).toBe(true);
    expect(disconnects.at(-1)?.message).toContain("max reconnect attempts");
  });

  it("close() clears every timer (including the stability timer), a general hygiene check -- not itself a Change-2-specific proof", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { stableAfter: 10000 },
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    await client.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("stableAfter: 0 reproduces the pre-0.4 reset-on-welcome behaviour (a config-level approximation, not a real source revert -- an actual revert of armStabilityTimer's reset timing was separately confirmed to fail 5 tests in this suite)", async () => {
    // Approximates the pre-Change-2 "reset on welcome" behaviour by driving
    // armStabilityTimer's effect immediately instead of waiting for
    // stableAfter, and confirms the climbing-sequence assertion the test
    // above relies on would indeed have failed under the old rule -- i.e.
    // this suite is not accidentally passing for an unrelated reason.
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const sockets: FakeSocket[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      // stableAfter: 0 reproduces "reset on welcome" -- the timer fires on
      // the very next tick after welcome, just like the old unconditional reset did.
      reconnect: { base: 1000, cap: 60000, stableAfter: 0 },
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    for (let i = 0; i < 2; i++) {
      sockets.at(-1)!.serverClose(1006, "abnormal");
      await tick();
      await vi.advanceTimersByTimeAsync(reconnecting.at(-1)!.delayMs);
      await connectSocket(sockets, sockets.length - 1);
      await tick();
    }

    // With stableAfter effectively 0, every cycle resets attempt back to 0
    // before the next close -- delay never climbs past the first step,
    // unlike the real default-stableAfter test above.
    expect(reconnecting[0]!.delayMs).toBe(2000);
    expect(reconnecting[1]!.delayMs).toBe(2000);

    await client.close();
  });
});

// --- drain end-to-end against a fake transport (blocker 3) --------------------

describe("MixerClient drain handling", () => {
  function drainFrame(lastStreamId = 0): Uint8Array {
    return encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: lastStreamId } as ControlMessage));
  }

  it("drain triggers an immediate parallel reconnect, and the retiring conn's own later close does not swallow the next one", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const client = new MixerClient("wss://x/tunnel", { token: provider, _wsFactory: makeFactory(sockets) });

    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    const s0 = await connectSocket(sockets, 0);
    await started;
    assertConnInvariant(client);
    expect(s0.helloToken).toBe("token-1");

    // Server drains: the SDK must start a new connection immediately, in
    // parallel, before the old one closes (WIRE.md section 2.9).
    s0.push(drainFrame());
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // drain's jitter window, random(0, 2000)ms
    expect(sockets.length).toBe(2); // the replacement's dial actually happened
    // The parallel reconnect the drain triggered re-invoked the provider and
    // dialed with the fresh token, not the old connection's (item 4).
    expect(sockets[1]!.authHeader).toBe("Bearer token-2");
    await connectSocket(sockets, 1);
    await tick();
    expect(sockets[1]!.helloToken).toBe("token-2");
    assertConnInvariant(client);
    expect(client.currentState()).toBe("connected");

    // The old (drained) connection is superseded automatically as soon as
    // the replacement's welcome lands (MixerClient.connectOnce's success
    // path fail()s it), so it must already be closed.
    expect(s0.closedWith).not.toBeNull();

    // Now the *new*, active connection drops for an unrelated reason (1006):
    // this must schedule a normal reconnect, not be silently swallowed by a
    // stale drainReconnectScheduled flag left over from the drain cycle
    // above (the bug blocker 3 fixes).
    sockets[1]!.serverClose(1006, "abnormal");
    await tick();
    // Normal full-jitter backoff, attempt 1: random(0, min(cap, base*2^1)) =
    // random(0, 2000)ms with the default base/cap. Advancing exactly that
    // far (not a blind 60s, which would let an un-opened dial's own 10s
    // connectTimeout fire and pile up further retries) is enough for the
    // third dial to happen, and no more.
    await vi.advanceTimersByTimeAsync(2000);
    expect(sockets.length).toBe(3); // the third dial actually happened
    assertConnInvariant(client);

    await connectSocket(sockets, 2);
    await tick();
    assertConnInvariant(client);
    expect(client.currentState()).toBe("connected");

    await client.close();
  });

  it("currentState() never reports connected with conn === null across a drain cycle", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });
    const observed: Array<{ state: string; connIsNull: boolean }> = [];
    const record = (): void => {
      observed.push({ state: client.currentState(), connIsNull: client.currentConn() === null });
    };
    client.on("welcome", record);
    client.on("drain", record);
    client.on("close", record);

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(drainFrame());
    await tick();
    await vi.advanceTimersByTimeAsync(2000);
    await connectSocket(sockets, 1);
    await tick();

    expect(observed.length).toBeGreaterThan(0);
    for (const { state, connIsNull } of observed) {
      if (state === "connected") expect(connIsNull).toBe(false);
    }

    await client.close();
  });

  it("two drain cycles in a row each reconnect immediately without wedging the state machine", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });

    const started = client.start();
    started.catch(() => {});
    let active = await connectSocket(sockets, 0);
    await started;

    for (let cycle = 0; cycle < 2; cycle++) {
      const before = sockets.length;
      active.push(drainFrame());
      await tick();
      await vi.advanceTimersByTimeAsync(2000);
      expect(sockets.length).toBe(before + 1);
      active = await connectSocket(sockets, sockets.length - 1);
      await tick();
      assertConnInvariant(client);
      expect(client.currentState()).toBe("connected");
    }

    await client.close();
  });

  it("a fast reconnect during drain cancels the old conn's in-flight stream with StreamError(CANCEL) and reports no disconnect for it", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    let stream: MixerStream | undefined;
    client.on("stream", (s) => {
      stream = s;
    });
    s0.push(encodeOpen(1));
    await tick();
    expect(stream).toBeDefined();
    let streamErr: unknown;
    stream!.on("error", (e) => {
      streamErr = e;
    });

    // Drain: the SDK reconnects immediately in parallel. Once the
    // replacement's welcome lands, the old (drained) conn is torn down --
    // its still-live stream didn't error, the connection it rode on was
    // replaced, so it must get a stream-scoped CANCEL, not a connection-level
    // error, and the old conn's own teardown must not surface as a disconnect
    // (the client is still connected throughout, via the replacement).
    s0.push(drainFrame());
    await tick();
    await vi.advanceTimersByTimeAsync(2000);
    await connectSocket(sockets, 1);
    await tick();

    expect(client.currentState()).toBe("connected");
    expect(streamErr).toBeInstanceOf(StreamError);
    expect((streamErr as StreamError).code).toBe(ErrorCode.CANCEL);
    expect((streamErr as StreamError).message).toBe("connection drained");
    expect(stream!.resetCode).toBe(ErrorCode.CANCEL);
    expect(disconnects).toHaveLength(0);

    await client.close();
    await client.close();
  });

  it("item 8: a second `drain` on the same connection does not spawn a second parallel reconnect", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });
    const drains: unknown[] = [];
    client.on("drain", (d) => drains.push(d));

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    // Two `drain`s on the same connection, before the first's parallel dial
    // has resolved -- mirrors the Go SDK, which ignores a repeat drain the
    // same way (handleServerDrain's own drainReconnectScheduled check): only
    // ONE parallel reconnect may be in flight per superseded connection, or
    // two could both race to spend the client-level unauthorizedRetryUsed
    // budget (Change 1).
    s0.push(drainFrame());
    s0.push(drainFrame());
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // drain's jitter window

    expect(drains).toHaveLength(2); // the app still hears about both
    expect(sockets.length).toBe(2); // but only one parallel dial started

    await connectSocket(sockets, 1);
    await tick();
    expect(client.currentState()).toBe("connected");

    await client.close();
  });

  it("R1: a `drain` queued behind a blocked onApp handler is still delivered after client.close() already resolved, and does not start a reconnect", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const drains: Array<{ reason: string }> = [];
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => (release = resolve));
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onApp: async () => {
        await gate; // wedges MixerConn's delivery loop
      },
      onDrain: (d) => drains.push(d),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(encodeData(0, encodeControl({ t: "app", body: {} } as never))); // wedges the delivery loop
    s0.push(drainFrame()); // queued behind the wedge
    await tick();
    expect(drains).toHaveLength(0); // still queued, not yet delivered

    const closePromise = client.close(); // starts closing NOW, while the drain is still queued
    await tick();

    release(); // let the wedged onApp handler -- and then the queued drain -- proceed
    await tick();

    expect(drains).toHaveLength(1); // still delivered, after close() already started
    expect(sockets.length).toBe(1); // no reconnect scheduled for the flushed drain: client was already closing
    await closePromise;
  });

  it("R1 regression: a `drain` queued behind a blocked onApp handler, delivered after the conn already died for an unrelated (non-close()) reason, does not spawn a second reconnect and does not leave the latch stuck", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // pin backoff delays to their ceiling
    const sockets: FakeSocket[] = [];
    const drains: Array<{ reason: string }> = [];
    const reconnecting: Array<{ delayMs: number }> = [];
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => (release = resolve));
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
      onApp: async () => {
        await gate; // wedges MixerConn's delivery loop
      },
      onDrain: (d) => drains.push(d),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(encodeData(0, encodeControl({ t: "app", body: {} } as never))); // wedges the delivery loop
    s0.push(drainFrame()); // queued behind the wedge
    await tick();
    expect(drains).toHaveLength(0); // still queued, not yet delivered

    // The conn dies for an UNRELATED reason (a peer error{9}, not client.close())
    // while the drain is still queued behind the wedge: its own 'close'
    // handler runs synchronously right now -- this.conn = null, one ordinary
    // reconnect scheduled -- all before the wedged app handler (and the
    // drain behind it) ever gets to run.
    s0.push(encodeData(0, encodeControl({ t: "error", code: ErrorCode.ENHANCE_YOUR_CALM, message: "too many stream-0 messages" } as never)));
    await tick();
    expect(sockets.length).toBe(1); // no new dial yet -- only the backoff timer was armed
    expect(reconnecting).toHaveLength(1); // the one ordinary reconnect from the close

    release(); // let the wedged app handler -- and then the queued drain -- proceed
    await tick();

    expect(drains).toHaveLength(1); // still delivered (Handler delivery: still owed)
    expect(reconnecting).toHaveLength(1); // NOT a second, parallel reconnect from the flushed drain

    // Fire exactly the one scheduled backoff: exactly ONE further socket is dialled.
    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    expect(sockets.length).toBe(2);

    await connectSocket(sockets, 1);
    await tick();
    expect(client.currentState()).toBe("connected");

    // The latch isn't stuck: a later ordinary close still schedules a reconnect.
    sockets[1]!.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting).toHaveLength(2);

    await client.close();
  });

  it("BUG 3: a fatal close on the retiring conn while the drain's parallel dial is still mid-handshake fails that dial too -- the client never flips back to 'connected'", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const fatalEvents: WsMixerError[] = [];
    // autoWelcome:false so the test controls exactly when (or whether) the
    // parallel dial's welcome lands, to land it squarely mid-handshake.
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets, { autoWelcome: false }) });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    pushWelcome(sockets[0]!);
    await tick();
    await started;
    expect(client.currentState()).toBe("connected");

    // Drain: the parallel dial starts (sockets[1]) but is deliberately left
    // mid-handshake -- dialed and hello sent, no welcome yet.
    sockets[0]!.push(drainFrame());
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // drain's jitter window
    expect(sockets.length).toBe(2);
    sockets[1]!.open();
    await tick(); // hello sent on sockets[1]; still no welcome -- genuinely mid-handshake

    // The retiring conn (sockets[0], still `this.conn` until the replacement
    // welcomes) now closes with a fatal code.
    sockets[0]!.serverClose(4000 + ErrorCode.UNSUPPORTED, "version mismatch");
    await tick();

    expect(fatalEvents).toHaveLength(1);
    expect(client.currentState()).toBe("closed");
    // The abandoned parallel dial must actually be closed, not left dangling.
    expect(sockets[1]!.closedWith).not.toBeNull();

    // Even if a welcome somehow still arrived for it (a slow reply racing the
    // abort), it must not resurrect the client back to "connected".
    pushWelcome(sockets[1]!);
    await tick();
    expect(client.currentState()).toBe("closed");
    expect(fatalEvents).toHaveLength(1); // still exactly one fatal report

    await client.close();
  });

  it("BUG 3 variant: the drain's parallel dial's welcome lands just BEFORE the retiring conn's fatal close is processed -- connectOnce's own `attempt.ok` guard (not the fatal branch's dialingConn cleanup) is what stops it from flipping the client back to 'connected'", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const fatalEvents: WsMixerError[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets, { autoWelcome: false }) });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    pushWelcome(sockets[0]!);
    await tick();
    await started;
    expect(client.currentState()).toBe("connected");

    sockets[0]!.push(drainFrame());
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // drain's jitter window
    expect(sockets.length).toBe(2);
    sockets[1]!.open();
    await tick(); // hello sent on sockets[1]; still mid-handshake

    // The parallel dial's welcome and the retiring conn's fatal close arrive
    // back-to-back, with no `tick()` between them: the welcome's handshake
    // resolution (dialAndHandshakeOnce's `attempt.ok`) and the fatal branch's
    // own `dialingConn.fail()` cleanup then race in the same microtask
    // window -- exactly the case connectOnce's own `attempt.ok` guard, not
    // that cleanup, has to catch.
    pushWelcome(sockets[1]!);
    sockets[0]!.serverClose(4000 + ErrorCode.UNSUPPORTED, "version mismatch");
    await tick();

    expect(fatalEvents).toHaveLength(1);
    expect(client.currentState()).toBe("closed");
    expect(sockets[1]!.closedWith).not.toBeNull(); // the promoted-then-abandoned dial is actually closed, not left dangling

    await client.close();
  });

  it("a fatal UNAUTHORIZED (4011) close on the retiring conn while the drain's parallel dial is still mid-handshake does not re-dial with a fresh token: the token-provider retry budget is left unspent and no third socket is opened", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const fatalEvents: WsMixerError[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const client = new MixerClient("wss://x/tunnel", { token: provider, _wsFactory: makeFactory(sockets, { autoWelcome: false }) });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    pushWelcome(sockets[0]!);
    await tick();
    await started;
    expect(client.currentState()).toBe("connected");
    expect(calls).toBe(1);

    // Drain: the parallel dial starts (sockets[1], a fresh token-2) but is
    // deliberately left mid-handshake.
    sockets[0]!.push(drainFrame());
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // drain's jitter window
    expect(sockets.length).toBe(2);
    expect(calls).toBe(2);
    sockets[1]!.open();
    await tick(); // hello sent on sockets[1]; still mid-handshake

    // The retiring conn (sockets[0]) now closes fatally with UNAUTHORIZED
    // (4011). Before the fix, failing the mid-handshake dial with that same
    // UNAUTHORIZED error made connectOnce's own unauthorized-retry loop treat
    // it as a fresh rejection and re-dial with yet another token, even though
    // the client had already gone fatal.
    sockets[0]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "token revoked");
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // nothing scheduled, but drain the timer queue just in case

    expect(fatalEvents).toHaveLength(1);
    expect(client.currentState()).toBe("closed");
    expect(calls).toBe(2); // the provider is NOT called a third time
    expect(sockets.length).toBe(2); // no third socket is dialed
    expect(sockets[1]!.closedWith).not.toBeNull(); // the abandoned mid-handshake dial is still actually closed

    await client.close();
  });
});

describe("MixerClient drain respects maxAttempts", () => {
  function drainFrame(lastStreamId = 0): Uint8Array {
    return encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: lastStreamId } as ControlMessage));
  }

  it("maxAttempts:0: a drain does not disconnect until the server's own deadline closes with 4012, reported once as fatal", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const fatalErrors: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalErrors.push(e));

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(drainFrame());
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // the jitter window a normal drain reconnect would have used

    // No parallel reconnect dial happened, and the conn is left alone to run
    // to its own deadline (WIRE.md section 2.9): maxAttempts:0 means
    // this SDK never reconnects, but a drain by itself isn't a disconnect.
    expect(sockets.length).toBe(1);
    expect(client.currentState()).toBe("connected");
    expect(disconnects).toHaveLength(0);
    expect(fatalErrors).toHaveLength(0);

    // The server's own deadline arrives and it closes with 4012 -- that's
    // the one, fatal report.
    s0.serverClose(4000 + ErrorCode.GOING_AWAY, "draining: rollout");
    await tick();
    expect(client.currentState()).toBe("closed");
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "connected", fatal: true, message: "drained; reconnect disabled" });
    expect(fatalErrors).toHaveLength(1);
  });

  it("maxAttempts:0: an in-flight stream finishes normally before the drain's eventual fatal report", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    let stream: MixerStream | undefined;
    client.on("stream", (s) => {
      stream = s;
    });
    s0.push(encodeOpen(1));
    await tick();
    expect(stream).toBeDefined();
    let streamEnded = false;
    let streamErr: unknown;
    stream!.on("end", () => {
      streamEnded = true;
    });
    stream!.on("error", (e) => {
      streamErr = e;
    });
    stream!.resume(); // flowing mode, so push(null) below actually emits 'end'

    // Drain arrives with reconnect disabled: the conn stays up and the
    // in-flight stream is left to finish on its own, not cancelled/reset.
    s0.push(drainFrame());
    await tick();
    s0.push(encodeClose(1));
    await tick();

    expect(streamEnded).toBe(true);
    expect(streamErr).toBeUndefined();
    expect(client.currentState()).toBe("connected");
    expect(disconnects).toHaveLength(0);

    // Only the server's own deadline close (4012) produces the one, fatal
    // disconnect -- after the stream already finished normally.
    s0.serverClose(4000 + ErrorCode.GOING_AWAY, "draining: rollout");
    await tick();
    expect(client.currentState()).toBe("closed");
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "connected", fatal: true, message: "drained; reconnect disabled" });
  });

  it("maxAttempts:0: client.close() after a drain is an ordinary graceful close, not the fatal drained report", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const fatalErrors: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalErrors.push(e));

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(drainFrame());
    await tick();

    // App-initiated close() wins over the pending drain deadline: this must
    // be reported (if at all) as an ordinary graceful close, never the fatal
    // "drained; reconnect disabled" report, and 'fatal' must never fire.
    await client.close();
    await tick();

    expect(client.currentState()).toBe("closed");
    expect(fatalErrors).toHaveLength(0);
    if (disconnects.length > 0) {
      expect(disconnects).toHaveLength(1);
      expect(disconnects[0]!.fatal).toBe(false);
      expect(disconnects[0]!.message).not.toBe("drained; reconnect disabled");
    }
  });
});

describe("MixerClient item 1: a pre-welcome drain does not desync the reconnect state machine", () => {
  it("fails the dialing conn without setting retiringConn/drainReconnectScheduled; a later 1006 still schedules a dial", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 100, cap: 1000 },
    });
    const started = client.start();
    started.catch(() => {}); // asserted later via `.rejects`/`await`; avoid a spurious unhandledRejection in between
    await tick();
    sockets[0]!.open();
    await tick();

    // A server that sends `drain` before ever completing the handshake with
    // `welcome`: MixerConn's item-1 guard must turn this into a
    // PROTOCOL_ERROR failure of the dialing connection, not a real drain --
    // so it must never reach MixerClient's `drain` handler (the only place
    // that sets retiringConn/drainReconnectScheduled).
    sockets[0]!.push(encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 0 } as ControlMessage)));
    await tick();

    const priv = client as unknown as { retiringConn: unknown; drainReconnectScheduled: boolean };
    expect(priv.retiringConn).toBeNull();
    expect(priv.drainReconnectScheduled).toBe(false);

    // The failed dial's own retry lands normally; let it succeed for real.
    await vi.advanceTimersByTimeAsync(1000);
    expect(sockets.length).toBe(2);
    sockets[1]!.open();
    await tick();
    pushWelcome(sockets[1]!);
    await tick();
    await started;

    // Now the active connection drops with a bare 1006: if the pre-welcome
    // drain had wrongly left drainReconnectScheduled set, this would be
    // silently swallowed instead of scheduling a third dial.
    sockets[1]!.serverClose(1006, "abnormal");
    await tick();
    await vi.advanceTimersByTimeAsync(1000);
    expect(sockets.length).toBe(3);

    await client.close();
  });
});

describe("MixerClient item 6: reconnectImmediately respects maxAttempts", () => {
  // End-to-end (a full 4012 flap loop where every replacement also
  // completes a fresh welcome) can't observe the ceiling directly: `attempt`
  // only resets once a connection has stayed up `stableAfter` ms past
  // welcome (Change 2) -- a flap loop that always reconnects successfully
  // but never stays up that long still accumulates attempts, so testing the
  // counting/ceiling logic directly on the private method (as conn.test.ts
  // already does for MixerConn internals like sendPing/outstandingPings) is
  // still the precise, non-flaky way to pin this contract down without
  // depending on stableAfter's timing.
  it("increments `attempt` on every call and gives up once maxAttempts is reached", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ attempt: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { maxAttempts: 2 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));
    const ctx = { phase: "connected" as const, message: "flap" };
    const priv = client as unknown as {
      reconnectImmediately: (c: typeof ctx, cause: string) => void;
      attempt: number;
    };

    expect(priv.attempt).toBe(0);
    priv.reconnectImmediately(ctx, "flap 1");
    expect(priv.attempt).toBe(1);
    expect(reconnecting).toHaveLength(1);
    expect(reconnecting[0]!.attempt).toBe(1);

    priv.reconnectImmediately(ctx, "flap 2");
    expect(priv.attempt).toBe(2);

    // attempt(2) >= maxAttempts(2): the third call must give up instead of
    // dialing again -- one merged report, fatal:true, exhaustion message.
    priv.reconnectImmediately(ctx, "flap 3");
    expect(client.currentState()).toBe("closed");
    expect(disconnects).toHaveLength(3);
    expect(disconnects.at(-1)?.fatal).toBe(true);

    // Flush the two dials flap 1/flap 2 already kicked off (their own
    // connectTimeout eventually fires against a socket nobody ever open()s)
    // so nothing dangles past this test -- both see `this.closing` already
    // set by flap 3's giveUp() and stand down instead of retrying.
    await vi.advanceTimersByTimeAsync(15000);
  });
});

// --- v0.2 token provider + 401 refresh-retry (CLIENT-SDK.md's "Token provider"/"Rejected token" rows) --------
describe("MixerClient v0.2: token provider", () => {
  it("calls the provider fresh on every dial, across multiple reconnects", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 10, cap: 100 },
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;
    expect(calls).toBe(1);
    expect(sockets[0]!.authHeader).toBe("Bearer token-1");
    expect(sockets[0]!.helloToken).toBe("token-1");

    for (let i = 1; i <= 2; i++) {
      sockets.at(-1)!.serverClose(1006, "abnormal");
      await tick();
      await vi.advanceTimersByTimeAsync(100);
      await connectSocket(sockets, sockets.length - 1);
      await tick();
    }
    expect(calls).toBe(3);
    expect(sockets.length).toBe(3);
    // Every reconnect actually carried the freshly-resolved token, on the
    // wire, not just to the provider (item 4).
    expect(sockets[1]!.authHeader).toBe("Bearer token-2");
    expect(sockets[1]!.helloToken).toBe("token-2");
    expect(sockets[2]!.authHeader).toBe("Bearer token-3");
    expect(sockets[2]!.helloToken).toBe("token-3");

    await client.close();
  });

  it("HTTP 401 calls the provider again and retries the dial immediately (no backoff), and succeeds", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const reconnecting: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: provider, _wsFactory: makeFactory(sockets) });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401);
    await tick(); // the retry dial happens immediately, no timer to advance

    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);
    expect(reconnecting).toHaveLength(0); // no backoff: this isn't scheduleReconnect
    // The retry dial actually carries the refreshed token-2, not the stale
    // token-1 that just got the 401 (item 4).
    expect(sockets[1]!.authHeader).toBe("Bearer token-2");

    await connectSocket(sockets, 1);
    expect(sockets[1]!.helloToken).toBe("token-2");
    await started;
    await client.close();
  });

  it("HTTP 401 twice is fatal, with httpStatus 401 on the disconnect reason", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const fatalEvents: unknown[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401);
    await tick();
    sockets[1]!.httpReject(401);
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(calls).toBe(2);
    expect(sockets.length).toBe(2); // no third dial
    expect(fatalEvents).toHaveLength(1);
    // An HTTP upgrade rejection is never a ws-mixer wire error -- no
    // errorCode/errorName, even for a 401 (D-2026-09-20-09).
    expect(disconnects.at(-1)).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true, errorCode: undefined, errorName: undefined });
  });

  it("a static string token is fatal on the first 401, no retry, httpStatus 401", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "static-token",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401);
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1);
    expect(disconnects.at(-1)).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true, errorCode: undefined, errorName: undefined });
  });

  it("a provider that rejects is fatal, with the thrown error surfaced as message and cause", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const boom = new Error("refresh failed: token endpoint 500");
    const provider = async () => {
      throw boom;
    };
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(0); // never even opened a socket
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: true, message: boom.message, cause: boom });
  });

  it("a provider that rejects on the 401 refresh-retry is fatal, with that rejection surfaced", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const boom = new Error("refresh token expired");
    let calls = 0;
    const provider = () => {
      calls++;
      if (calls === 1) return "first-token";
      throw boom;
    };
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401);
    await tick();

    await expect(started).rejects.toBeTruthy();
    expect(calls).toBe(2);
    expect(sockets.length).toBe(1); // the retry never got as far as a second socket
    expect(disconnects.at(-1)).toMatchObject({ phase: "dial", fatal: true, message: boom.message, cause: boom });
  });

  for (const status of [403, 404]) {
    it(`HTTP ${status} is fatal always, even with a token provider present, httpStatus ${status}`, async () => {
      vi.useFakeTimers();
      const sockets: FakeSocket[] = [];
      let calls = 0;
      const provider = () => {
        calls++;
        return "t";
      };
      const disconnects: DisconnectPayload[] = [];
      const client = new MixerClient("wss://x/tunnel", {
        token: provider,
        _wsFactory: makeFactory(sockets),
        onDisconnect: (r) => disconnects.push(r),
      });

      const started = client.start();
      started.catch(() => {});
      await tick();
      sockets[0]!.httpReject(status);
      await tick();
      await vi.advanceTimersByTimeAsync(120000);

      await expect(started).rejects.toBeTruthy();
      expect(sockets.length).toBe(1); // no retry: only 401 is retry-eligible
      expect(calls).toBe(1);
      expect(disconnects.at(-1)).toMatchObject({ phase: "dial", httpStatus: status, fatal: true });
      // An HTTP upgrade rejection is never a ws-mixer wire error -- 403 is
      // grouped with 401 for retry/fatal purposes only (WIRE.md section
      // 2.9), and 404 never was -- neither gets an errorCode/errorName
      // (D-2026-09-20-09).
      expect(disconnects.at(-1)?.errorCode).toBeUndefined();
      expect(disconnects.at(-1)?.errorName).toBeUndefined();
    });
  }

  for (const status of [500, 503]) {
    it(`HTTP ${status} during the dial is non-fatal: retried with normal backoff, httpStatus ${status}, no errorCode`, async () => {
      vi.useFakeTimers();
      const sockets: FakeSocket[] = [];
      const disconnects: DisconnectPayload[] = [];
      const reconnecting: Array<{ attempt: number; delayMs: number; cause: string }> = [];
      const client = new MixerClient("wss://x/tunnel", {
        token: "t",
        _wsFactory: makeFactory(sockets),
        reconnect: { base: 1000, cap: 60000 },
        onDisconnect: (r) => disconnects.push(r),
      });
      client.on("reconnecting", (info) => reconnecting.push(info));

      const started = client.start();
      started.catch(() => {});
      await tick();
      sockets[0]!.httpReject(status);
      await tick();

      expect(sockets.length).toBe(1); // not yet -- still waiting out the backoff delay
      expect(disconnects.at(-1)).toMatchObject({ phase: "dial", httpStatus: status, fatal: false, errorCode: undefined, errorName: undefined });
      expect(reconnecting).toHaveLength(1);

      // Fire exactly the scheduled backoff, not a blind long jump: the retry
      // dial's own connectTimeout would otherwise also elapse and cascade
      // into further retries before we get a chance to open() it.
      await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
      expect(sockets.length).toBe(2); // retried instead of giving up

      await connectSocket(sockets, 1);
      await started;
      await client.close();
    });
  }

  it("phase is 'dial' for a dial-time failure, 'handshake' for a post-dial handshake failure, 'connected' for a post-welcome close", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});

    // dial phase: a bare network error before the socket ever opens.
    await tick();
    sockets[0]!.netError(new Error("ECONNREFUSED"));
    await tick();
    expect(disconnects.at(-1)).toMatchObject({ phase: "dial", fatal: false });

    // handshake phase: the dial succeeds (open), but no `welcome` ever
    // arrives before the hello timeout -- MixerConn's own handshake failure.
    await vi.advanceTimersByTimeAsync(100);
    sockets[1]!.open();
    await tick();
    await vi.advanceTimersByTimeAsync(15000); // DEFAULT_HELLO_TIMEOUT_MS
    await tick();
    expect(disconnects.at(-1)).toMatchObject({ phase: "handshake", fatal: false });

    // connected phase: a real welcome lands, then the socket drops. (This
    // factory has autoWelcome disabled, so the welcome has to be pushed by
    // hand, same as the dial/handshake sockets above.)
    await vi.advanceTimersByTimeAsync(1000);
    await tick();
    const s2 = sockets[2]!;
    s2.open();
    await tick();
    s2.push(
      encodeData(
        0,
        encodeControl({
          t: "welcome",
          v: 1,
          session: "s2",
          window: 262144,
          max_streams: 64,
          ping_interval: 30000,
          ping_timeout: 90000,
        } as ControlMessage),
      ),
    );
    await tick();
    await started;
    s2.serverClose(1006, "abnormal");
    await tick();
    expect(disconnects.at(-1)).toMatchObject({ phase: "connected", fatal: false });

    await client.close();
  });

  it("maxAttempts interplay (item 4): an already-exhausted ceiling does NOT block the pre-welcome refresh-retry -- it still redials and can still connect", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // pin full-jitter to its ceiling -- an unpinned sub-1ms draw is indistinguishable from "already due" under fake timers (see tick()'s note)
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return "t";
    };
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100, maxAttempts: 1 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    // Burn the one allowed attempt on an ordinary network failure first.
    await tick();
    sockets[0]!.netError(new Error("ECONNRESET"));
    await tick();
    expect(calls).toBe(1);
    await vi.advanceTimersByTimeAsync(100);
    expect(sockets.length).toBe(2); // the one retry attempt fires...

    // ...and now attempt (1) >= maxAttempts (1) -- but the refresh-retry
    // below is NOT one of the ordinary reconnect-attempt paths gated by
    // maxAttempts (CLIENT-SDK.md's "Rejected token" row is
    // unconditional for a provider, and it does not itself increment
    // `attempt`): a 401 on this dial still gets its one immediate retry.
    sockets[1]!.httpReject(401);
    await tick();
    expect(calls).toBe(3); // the refresh-retry's own fresh provider call
    expect(sockets.length).toBe(3); // ...and its own fresh dial

    // That retry succeeds: connects normally, start() resolves (not rejects).
    sockets[2]!.open();
    await tick();
    pushWelcome(sockets[2]!);
    await tick();
    await started;
    expect(client.currentState()).toBe("connected");
    // The one report is the earlier ECONNRESET (non-fatal, normal backoff);
    // neither the 401 nor its silent refresh-retry produces a second one.
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: false, message: "ECONNRESET" });

    await client.close();
    await client.close();
  });
});

// --- Change 1/1b: ONE retry budget, shared by both pre-welcome rejection -----
// shapes (HTTP 401 on the upgrade, and handshake-phase UNAUTHORIZED/4011,
// with or without a preceding error{}) -- provider-only, one retry, and a
// second rejection in EITHER form is fatal ------------------------------------

describe("MixerClient change 1/1b: unified pre-welcome auth-rejection retry", () => {
  it("provider + bare close 4011 before welcome: one retry, no backoff, no disconnect reported for the first rejection, then connects", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "token rejected");
    await tick(); // the retry dial happens immediately, no timer to advance

    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);
    expect(reconnecting).toHaveLength(0); // no backoff
    expect(disconnects).toHaveLength(0); // no report for the first rejection
    expect(sockets[1]!.authHeader).toBe("Bearer token-2");

    sockets[1]!.open();
    await tick();
    pushWelcome(sockets[1]!);
    await tick();
    await started;

    expect(client.currentState()).toBe("connected");
    expect(disconnects).toHaveLength(0);

    await client.close();
  });

  it("provider + bare close 4011 before welcome: a second rejection is fatal, start() rejects, provider called exactly twice", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "token rejected");
    await tick();
    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);

    sockets[1]!.open();
    await tick();
    sockets[1]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "token rejected again");
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(calls).toBe(2); // no third provider call
    expect(sockets.length).toBe(2); // no third dial
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "handshake", wsCode: 4000 + ErrorCode.UNAUTHORIZED, fatal: true });
  });

  it("static token + bare close 4011 before welcome: fatal at once, exactly one dial", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "static-token",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "token rejected");
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1); // no retry
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.UNAUTHORIZED,
      errorCode: ErrorCode.UNAUTHORIZED,
      errorName: "UNAUTHORIZED",
      fatal: true,
    });
  });

  it("provider + error{11 UNAUTHORIZED} before welcome: one retry, provider called twice, no disconnect reported for the first rejection", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.push(
      encodeData(0, encodeControl({ t: "error", code: ErrorCode.UNAUTHORIZED, message: "token invalid" } as ControlMessage)),
    );
    await tick();

    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);
    expect(reconnecting).toHaveLength(0);
    expect(disconnects).toHaveLength(0);

    sockets[1]!.open();
    await tick();
    pushWelcome(sockets[1]!);
    await tick();
    await started;
    expect(client.currentState()).toBe("connected");
    expect(disconnects).toHaveLength(0);

    await client.close();
  });

  it("provider + error{11 UNAUTHORIZED} before welcome: a second rejection is fatal", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.push(
      encodeData(0, encodeControl({ t: "error", code: ErrorCode.UNAUTHORIZED, message: "token invalid" } as ControlMessage)),
    );
    await tick();
    sockets[1]!.open();
    await tick();
    sockets[1]!.push(
      encodeData(0, encodeControl({ t: "error", code: ErrorCode.UNAUTHORIZED, message: "still invalid" } as ControlMessage)),
    );
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "handshake", wsCode: 4000 + ErrorCode.UNAUTHORIZED, fatal: true });
  });

  it("provider + HTTP 401 then handshake-phase 4011: the second rejection (a different form) is still fatal -- one shared budget", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401); // dial-phase rejection: uses the one retry
    await tick(); // the retry redials immediately -- a fresh DialSocket via the factory

    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);
    sockets[1]!.open();
    await tick();
    sockets[1]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "handshake rejected too");
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(calls).toBe(2); // no third provider call -- the budget was already spent by the 401
    expect(sockets.length).toBe(2);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "handshake", wsCode: 4000 + ErrorCode.UNAUTHORIZED, fatal: true });
  });

  it("provider + handshake-phase 4011 then HTTP 401: fatal after the second, in the opposite order", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "handshake rejected");
    await tick(); // uses the one retry
    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);

    sockets[1]!.httpReject(401); // the retry dial itself gets a 401: budget already spent -> fatal
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true });
  });
});

// --- Change 1 addendum: the refresh-retry budget is a CLIENT-level flag, -----
// re-armed only at stability -- never on every redial, so a server that ------
// welcomes-then-closes can't make the client hit the token endpoint forever --

describe("MixerClient change 1 addendum: refresh-retry budget re-arms only at stability", () => {
  it("rejected -> refresh -> welcome -> close before stableAfter -> redial rejected: fatal, with 3 total provider calls (2 for the refresh-retry cycle, 1 more for the later un-stable redial that also gets rejected, with no further refresh)", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100, stableAfter: 10000 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "rejected");
    await tick(); // burns the one retry: calls -> 2, a second socket dials

    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);
    sockets[1]!.open();
    await tick();
    pushWelcome(sockets[1]!);
    await tick();
    await started; // resolves here: the client HAS connected once
    expect(client.currentState()).toBe("connected");

    // Drops well before stability: the retry budget must stay spent.
    await vi.advanceTimersByTimeAsync(2000);
    sockets[1]!.serverClose(1006, "abnormal");
    await tick();
    await vi.advanceTimersByTimeAsync(100); // normal backoff redial (base 10/cap 100: delay <= 20ms)
    expect(sockets.length).toBe(3);
    sockets[2]!.open();
    await tick();
    sockets[2]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "rejected again");
    await tick();

    // No further retry for this new rejection: the budget never re-armed
    // (this conn never reached stability either) -- fatal at once, no third
    // provider call. (start() already resolved above and stays resolved --
    // a later fatal disconnect only rejects start() if the client never
    // connected once; it's the 'fatal' event/onDisconnect that carry this.)
    expect(calls).toBe(3); // one provider call for the third dial only, no fourth
    expect(sockets.length).toBe(3);
    expect(client.currentState()).toBe("closed");
    expect(disconnects).toHaveLength(2); // the earlier non-fatal 1006 + this fatal one
    expect(disconnects[0]).toMatchObject({ phase: "connected", wsCode: 1006, fatal: false });
    expect(disconnects.at(-1)).toMatchObject({ phase: "handshake", fatal: true, wsCode: 4000 + ErrorCode.UNAUTHORIZED });
  });

  it("...but if the connection survives stableAfter, the next rejection gets a refresh-retry again", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100, stableAfter: 10000 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "rejected");
    await tick(); // burns the one retry
    expect(calls).toBe(2);
    sockets[1]!.open();
    await tick();
    pushWelcome(sockets[1]!);
    await tick();
    await started;

    // This time, stay up past stability before dropping.
    await vi.advanceTimersByTimeAsync(10000);
    sockets[1]!.serverClose(1006, "abnormal");
    await tick();
    await vi.advanceTimersByTimeAsync(100); // normal backoff redial (base 10/cap 100: delay <= 20ms)
    expect(sockets.length).toBe(3);
    sockets[2]!.open();
    await tick();
    sockets[2]!.serverClose(4000 + ErrorCode.UNAUTHORIZED, "rejected once more");
    await tick();

    // The budget re-armed at stability: this rejection gets its own retry.
    expect(calls).toBe(4); // dial 3's own token + the retry's fresh token
    expect(sockets.length).toBe(4);
    expect(disconnects).toHaveLength(1); // the earlier non-fatal 1006 only -- no report for the refresh-retry's own rejection
    expect(disconnects[0]).toMatchObject({ phase: "connected", wsCode: 1006, fatal: false });

    sockets[3]!.open();
    await tick();
    pushWelcome(sockets[3]!);
    await tick();
    expect(client.currentState()).toBe("connected");

    await client.close();
  });
});

describe("MixerClient change 1 item 4: the refresh-retry is unconditional on maxAttempts/reconnect-disabled", () => {
  it("provider + maxAttempts:0 + HTTP 401 on the INITIAL connect: still gets the one refresh-retry (2 provider calls, 2 dials), and the retry succeeding completes the initial connection", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401);
    await tick(); // the retry redials immediately, no backoff

    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);
    expect(disconnects).toHaveLength(0); // no report for the first rejection

    await connectSocket(sockets, 1);
    await started;
    expect(client.currentState()).toBe("connected");
    expect(disconnects).toHaveLength(0);

    await client.close();
  });

  it("provider + maxAttempts:0 + a second HTTP 401 (the retry's own dial also rejected): fatal, exactly like maxAttempts unset", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401);
    await tick();
    expect(calls).toBe(2);
    sockets[1]!.httpReject(401);
    await tick();

    await expect(started).rejects.toBeTruthy();
    expect(calls).toBe(2); // no third provider call
    expect(sockets.length).toBe(2);
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true });
  });

  it("provider + HTTP 401 then HTTP 503 on the retry dial: NOT forced fatal -- the retry's own failure takes the ordinary recoverable path (non-fatal, normal backoff), and the spent budget stays spent (no further refresh before stability)", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    let calls = 0;
    const provider = () => {
      calls++;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100, stableAfter: 10000 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401); // uses the one retry
    await tick();
    expect(calls).toBe(2);
    expect(sockets.length).toBe(2);

    // The retry's own dial fails for an ordinary, non-auth reason: this is
    // NOT "a second rejection" in the sense the spec means (a second token
    // rejection) -- it's an unrelated transient failure, so it must take the
    // normal recoverable path instead of being forced fatal.
    sockets[1]!.httpReject(503);
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", httpStatus: 503, fatal: false, errorCode: undefined });
    expect(reconnecting).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    expect(sockets.length).toBe(3);

    // That normal-backoff redial itself now gets rejected with a 401 too,
    // still well before any connection has ever gone stable: the budget
    // stayed spent (the 503 above never re-armed it), so this is fatal at
    // once, with no further refresh-retry.
    sockets[2]!.httpReject(401);
    await tick();

    expect(calls).toBe(3); // no fourth provider call for another refresh
    expect(sockets.length).toBe(3);
    await expect(started).rejects.toBeTruthy();
    expect(disconnects).toHaveLength(2);
    expect(disconnects[1]).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true });
  });
});

// --- TokenUnavailableError: an explicit-opt-in marker for a temporary -------
// failure to OBTAIN a token (network still down, auth server briefly
// unreachable), treated like a failed dial instead of the fatal-by-default
// verdict every other provider throw/reject still gets. Detection is
// `instanceof` only (and follows `cause`, bounded + cycle-safe) -- never a
// duck-typed property/method, so an unrelated library's error can never
// accidentally turn a genuinely fatal provider failure into an endless
// retry loop.
describe("MixerClient TokenUnavailableError", () => {
  it("a provider that throws TokenUnavailableError on a reconnect is non-fatal: normal backoff, cause surfaced, then reconnects", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // pin full-jitter to its ceiling, so delays are exact
    const sockets: FakeSocket[] = [];
    const boom = new TokenUnavailableError("auth server briefly unreachable");
    let calls = 0;
    const provider = () => {
      calls++;
      if (calls === 2) throw boom;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;
    expect(calls).toBe(1);

    // Post-welcome drop: an ordinary reconnect trigger, unrelated to the
    // provider -- schedules the first backoff (attempt 1).
    sockets[0]!.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting).toHaveLength(1);
    expect(reconnecting[0]!.delayMs).toBe(2000); // base*2^1, pinned to the ceiling

    // That backoff's own redial is where the provider throws
    // TokenUnavailableError: no socket for the failed provider call, exactly
    // one non-fatal report with the thrown error verbatim as `cause`, and a
    // normal (climbing) backoff timer -- not fatal, not an immediate retry.
    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    await tick();
    expect(calls).toBe(2);
    expect(sockets.length).toBe(1);
    expect(disconnects).toHaveLength(2);
    expect(disconnects[1]).toMatchObject({ phase: "dial", fatal: false, cause: boom });
    expect(disconnects[1]!.cause).toBe(boom);
    expect(reconnecting).toHaveLength(2);
    expect(reconnecting[1]!.delayMs).toBe(4000); // base*2^2, pinned

    // The next attempt calls the provider again and connects.
    await vi.advanceTimersByTimeAsync(reconnecting[1]!.delayMs);
    expect(calls).toBe(3);
    await connectSocket(sockets, 1);
    expect(sockets.length).toBe(2);

    await client.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("repeated marked failures climb the backoff delay each time, and exhaust maxAttempts fatally like any other failure", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // pin full-jitter to its ceiling, so delays are exact
    const sockets: FakeSocket[] = [];
    const boom = new TokenUnavailableError("token endpoint still down");
    let calls = 0;
    const provider = () => {
      calls++;
      if (calls === 1) return "token-1";
      throw boom;
    };
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000, maxAttempts: 2 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting).toHaveLength(1);
    expect(reconnecting[0]!.delayMs).toBe(2000);

    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    await tick();
    expect(calls).toBe(2); // provider call #2 threw -- non-fatal, climbs to attempt 2's ceiling
    expect(sockets.length).toBe(1); // no socket for the failed provider call
    expect(reconnecting).toHaveLength(2);
    expect(reconnecting[1]!.delayMs).toBe(4000);

    await vi.advanceTimersByTimeAsync(reconnecting[1]!.delayMs);
    await tick();
    // Provider call #3 also threw, and maxAttempts (2) is now exhausted: the
    // marked failure itself stayed non-fatal (dialAndHandshakeOnce's own
    // verdict) -- it's exhaustion (giveUp), same as any other failure, that
    // makes this fatal.
    expect(calls).toBe(3);
    expect(sockets.length).toBe(1);
    await started; // already resolved at the first successful connect above -- exhaustion doesn't un-resolve it
    expect(disconnects).toHaveLength(3); // 1006 (connected), the marked failure, the exhaustion
    expect(disconnects.at(-1)).toMatchObject({ phase: "dial", fatal: true, cause: boom });
    expect(disconnects.at(-1)?.message).toContain("max reconnect attempts");

    expect(vi.getTimerCount()).toBe(0);
  });

  it("a TokenUnavailableError reachable only via a wrapping Error's `cause` is still treated as marked", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const sockets: FakeSocket[] = [];
    const wrapped = new Error("refresh failed", { cause: new TokenUnavailableError("token endpoint down") });
    let calls = 0;
    const provider = () => {
      calls++;
      if (calls === 2) throw wrapped;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(1006, "abnormal");
    await tick();
    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    await tick();

    expect(sockets.length).toBe(1); // non-fatal: no immediate fatal close
    expect(disconnects).toHaveLength(2);
    // `cause` is the wrapping error, verbatim -- exactly like an unmarked
    // provider throw surfaces its own error, not the unwrapped inner one.
    expect(disconnects[1]).toMatchObject({ phase: "dial", fatal: false, cause: wrapped });

    await client.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a deep/cyclic `cause` chain that never reaches a TokenUnavailableError does not hang, and is fatal", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const e1 = new Error("a");
    const e2 = new Error("b", { cause: e1 });
    (e1 as Error & { cause?: unknown }).cause = e2; // cycle: e1 -> e2 -> e1 -> ...
    const provider = async () => {
      throw e1;
    };
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: true, cause: e1 });
  });

  it("an error with a retryable-looking property/method (not an `instanceof TokenUnavailableError`) is still fatal -- no duck-typing", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const boom = Object.assign(new Error("token endpoint on fire"), { retryable: true, Retryable: () => true });
    const provider = async () => {
      throw boom;
    };
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(0);
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: true, message: boom.message, cause: boom });
  });

  it("first connect: a provider that throws TokenUnavailableError does not reject start() -- same outcome as a first-dial network error", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const boom = new TokenUnavailableError("network still down");
    let calls = 0;
    const provider = () => {
      calls++;
      if (calls === 1) throw boom;
      return "token-2";
    };
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ delayMs: number }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    let settled = false;
    started.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );

    await tick();
    expect(sockets.length).toBe(0); // never even opened a socket, same as an unmarked provider throw
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: false, cause: boom });
    expect(reconnecting).toHaveLength(1);
    expect(settled).toBe(false); // start() has NOT rejected -- unlike an unmarked throw, which is fatal

    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    await connectSocket(sockets, 0);
    await started;
    expect(settled).toBe(true);
    expect(calls).toBe(2);

    await client.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("refresh-retry path: the provider's own call throws TokenUnavailableError -- non-fatal backoff, budget stays spent, and the next un-stable rejection is fatal with no further refresh", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const sockets: FakeSocket[] = [];
    const boom = new TokenUnavailableError("token endpoint briefly unreachable");
    let calls = 0;
    const provider = () => {
      calls++;
      if (calls === 2) throw boom;
      return `token-${calls}`;
    };
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ delayMs: number }> = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: provider,
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100, stableAfter: 10000 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(401); // uses the one retry
    await tick();

    // The retry's own provider call threw TokenUnavailableError: no second
    // socket (resolveToken throws before dialWebSocket), non-fatal, normal
    // backoff -- NOT forced fatal by the refresh-retry rule, and no
    // disconnect reported for the 401 itself (only the final outcome is).
    expect(calls).toBe(2);
    expect(sockets.length).toBe(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: false, cause: boom });
    expect(reconnecting).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    expect(calls).toBe(3);
    expect(sockets.length).toBe(2);

    // That redial itself now gets rejected with a 401 too, still well
    // before any connection has ever gone stable: the budget stayed spent
    // (the marked failure above never re-armed it), so this is fatal at
    // once, with no further refresh-retry.
    sockets[1]!.httpReject(401);
    await tick();

    expect(calls).toBe(3); // no fourth provider call for another refresh
    expect(sockets.length).toBe(2);
    await expect(started).rejects.toBeTruthy();
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(2);
    expect(disconnects[1]).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true });

    await client.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a throwing `cause` getter on the provider's error never escapes as an unhandled rejection: exactly one fatal report, start() rejects", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const e = new Error("wedge via cause");
    Object.defineProperty(e, "cause", {
      get() {
        throw new Error("cause getter boom");
      },
    });
    const provider = async () => {
      throw e;
    };
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandledRejections.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const client = new MixerClient("wss://x/tunnel", {
        token: provider,
        _wsFactory: makeFactory(sockets),
        onDisconnect: (r) => disconnects.push(r),
      });
      client.on("fatal", (ev) => fatalEvents.push(ev));

      const started = client.start();
      started.catch(() => {});
      await tick();
      await vi.advanceTimersByTimeAsync(120000);

      // Check the synchronous-by-now observables BEFORE awaiting `started`'s
      // rejection: on a regression (the throw escaping uncaught) `started`
      // never settles, and awaiting it first would just hang for the full
      // per-test timeout instead of failing fast with the real reason.
      expect(unhandledRejections).toHaveLength(0);
      expect(fatalEvents).toHaveLength(1);
      expect(disconnects).toHaveLength(1);
      expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: true });
      expect(disconnects[0]!.cause).toBe(e);
      await expect(started).rejects.toBeTruthy();
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("a throwing `message` getter on the provider's error never escapes as an unhandled rejection: exactly one fatal report, start() rejects", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const e = new Error("placeholder");
    Object.defineProperty(e, "message", {
      get() {
        throw new Error("message getter boom");
      },
    });
    const provider = async () => {
      throw e;
    };
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandledRejections.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      const client = new MixerClient("wss://x/tunnel", {
        token: provider,
        _wsFactory: makeFactory(sockets),
        onDisconnect: (r) => disconnects.push(r),
      });
      client.on("fatal", (ev) => fatalEvents.push(ev));

      const started = client.start();
      started.catch(() => {});
      await tick();
      await vi.advanceTimersByTimeAsync(120000);

      // Same ordering rationale as the `cause`-getter test above: check the
      // synchronous-by-now observables before awaiting `started`, so a
      // regression fails fast instead of hanging for the full test timeout.
      expect(unhandledRejections).toHaveLength(0);
      expect(fatalEvents).toHaveLength(1);
      expect(disconnects).toHaveLength(1);
      expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: true });
      expect(disconnects[0]!.cause).toBe(e);
      await expect(started).rejects.toBeTruthy();
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });
});

describe("MixerClient item 7: loud onDisconnect for a protocol-bug close (4001/4003/4004)", () => {
  for (const [errorCode, name] of [
    [ErrorCode.PROTOCOL_ERROR, "PROTOCOL_ERROR"],
    [ErrorCode.FLOW_CONTROL_ERROR, "FLOW_CONTROL_ERROR"],
    [ErrorCode.FRAME_SIZE_ERROR, "FRAME_SIZE_ERROR"],
  ] as const) {
    it(`onDisconnect carries protocolError:true with code/name for a ${name} close`, async () => {
      vi.useFakeTimers();
      const sockets: FakeSocket[] = [];
      const disconnects: Array<Record<string, unknown>> = [];
      const client = new MixerClient("wss://x/tunnel", {
        token: "t",
        _wsFactory: makeFactory(sockets),
        onDisconnect: (r) => disconnects.push(r as unknown as Record<string, unknown>),
      });

      const started = client.start();
      started.catch(() => {});
      await connectSocket(sockets, 0);
      await started;

      sockets[0]!.serverClose(4000 + errorCode, "you violated the protocol");
      await tick();

      expect(disconnects).toHaveLength(1);
      expect(disconnects[0]).toMatchObject({
        wsCode: 4000 + errorCode,
        protocolError: true,
        code: errorCode,
        name,
      });

      await client.close();
    });
  }

  it("onDisconnect does not carry protocolError for an ordinary close", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: Array<Record<string, unknown>> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r as unknown as Record<string, unknown>),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(1006, "abnormal");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]!.protocolError).toBeFalsy();

    await client.close();
  });
});

// --- blocker 1: exactly one DisconnectReason per disconnect, even when ------
// exhaustion follows the failure that caused it ------------------------------

describe("MixerClient blocker 1: exactly one DisconnectReason per disconnect", () => {
  it("maxAttempts:0 + ECONNREFUSED on the initial dial reports exactly once, fatal:true", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.netError(new Error("ECONNREFUSED"));
    await tick();

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1); // never retried: maxAttempts is already 0
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: true });
    expect(disconnects[0]!.message).toContain("max reconnect attempts");
  });

  it("a handshake failure immediately exhausted (maxAttempts:0) reports exactly once, fatal:true", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    await vi.advanceTimersByTimeAsync(15000); // DEFAULT_HELLO_TIMEOUT_MS: no welcome ever arrives
    await tick();

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1); // never retried
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "handshake", fatal: true });
    expect(disconnects[0]!.message).toContain("max reconnect attempts");
  });

  it("HTTP 429 immediately exhausted (maxAttempts:0) reports exactly once, fatal:true, httpStatus 429", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.httpReject(429, { "retry-after": "5" });
    await tick();

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1); // never retried
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", httpStatus: 429, fatal: true });
    expect(disconnects[0]!.message).toContain("max reconnect attempts");
  });

  it("a normal non-fatal failure followed by a successful reconnect reports exactly once, fatal:false, then onConnect", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const connects: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
      onConnect: (w) => connects.push(w),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.netError(new Error("ECONNRESET"));
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: false });
    expect(connects).toHaveLength(0); // not connected yet

    await vi.advanceTimersByTimeAsync(100);
    await connectSocket(sockets, 1);
    await started;

    // The retry succeeded: still exactly the one report from the failure
    // above, now followed by onConnect -- no second onDisconnect for the
    // successful reconnect itself.
    expect(disconnects).toHaveLength(1);
    expect(connects).toHaveLength(1);

    await client.close();
  });

  // nit 5: giveUp keeps wsCode/errorCode when the disconnect that exhausted
  // maxAttempts was itself a WS close, not just the exhaustion message.
  it("a connected-phase close that exhausts maxAttempts (maxAttempts:0) keeps wsCode on the merged report", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { maxAttempts: 0 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    // welcome already reset attempt to 0; with maxAttempts:0 this very first
    // post-welcome close is immediately exhausted.
    sockets[0]!.serverClose(1006, "abnormal");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "connected", wsCode: 1006, fatal: true });
    expect(disconnects[0]!.message).toContain("max reconnect attempts");
  });
});

// --- giveUp (maxAttempts exhaustion) must mirror goFatal: no orphaned ------
// live conn/timers left running past "closed" -----------------------------

describe("MixerClient giveUp mirrors goFatal: no orphaned conn/timers after exhaustion", () => {
  it("maxAttempts:1, welcome -> unstable redial -> welcome -> drain's parallel dial fails -> exhaustion closes the still-live conn and leaves zero timers", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 10, cap: 100, maxAttempts: 1, stableAfter: 10000 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    // An unstable cycle bumps attempt 0 -> 1 (still under maxAttempts:1's
    // ceiling, so this redial is allowed).
    sockets[0]!.serverClose(1006, "abnormal");
    await tick();
    await vi.advanceTimersByTimeAsync(20);
    const s1 = await connectSocket(sockets, 1);
    await tick();
    expect(client.currentState()).toBe("connected");

    // Drain, well before s1 ever proves stable: a parallel reconnect starts
    // (retiringConn === conn === s1's MixerConn, since drain never touches
    // `this.conn` itself). That parallel dial then fails for an ordinary
    // (non-auth, non-fatal) reason.
    s1.push(encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 0 } as ControlMessage)));
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // drain's own jitter window
    expect(sockets.length).toBe(3);
    sockets[2]!.netError(new Error("ECONNRESET"));
    await tick();

    // attempt is already 1 (== maxAttempts): this dial failure's own
    // scheduleReconnect finds the ceiling already reached and gives up
    // immediately, without ever incrementing further.
    expect(client.currentState()).toBe("closed");
    expect(disconnects.at(-1)?.fatal).toBe(true);
    expect(disconnects.at(-1)?.message).toContain("max reconnect attempts");

    // The mirror-goFatal fix: s1 (still live, welcomed, and both `conn` AND
    // `retiringConn` at the moment of exhaustion) must actually be closed --
    // not left running with its ping/watchdog timers, nor its own
    // stability timer left orphaned to fire 10s later against a closed
    // client.
    expect(s1.closedWith).not.toBeNull();
    expect(vi.getTimerCount()).toBe(0);
  });
});

// --- item 3: per-dial-attempt failure context, not shared mutable fields ----

describe("MixerClient item 3: a concurrent old-conn close can't clobber a new dial's report", () => {
  it("old conn closes 4012 mid-dial while the new dial 401s: each report keeps its own phase/httpStatus", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.push(
      encodeData(
        0,
        encodeControl({
          t: "welcome",
          v: 1,
          session: "s0",
          window: 262144,
          max_streams: 64,
          ping_interval: 30000,
          ping_timeout: 90000,
        } as ControlMessage),
      ),
    );
    await tick();
    await started;

    // Drain starts a parallel dial (retiringConn = the old conn); let it
    // reach a second socket without resolving it yet.
    sockets[0]!.push(encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 0 } as ControlMessage)));
    await tick();
    await vi.advanceTimersByTimeAsync(2000); // drain's jitter window
    expect(sockets.length).toBe(2);

    // While that new dial is still in flight, the *old* (retiring) conn
    // closes on its own with 4012 -- a real event, independent of the new
    // dial's own outcome.
    sockets[0]!.serverClose(4000 + ErrorCode.GOING_AWAY, "draining: rollout");
    await tick();

    // Now the new dial fails with a fatal HTTP 401 (static token: no
    // refresh-retry). With shared mutable failure-context fields, the old
    // conn's close above (phase "connected") could clobber what this
    // report is about to say; per-attempt context (blocker 3's fix) means
    // it can't.
    sockets[1]!.httpReject(401);
    await tick();

    expect(disconnects).toHaveLength(2);
    expect(disconnects[0]).toMatchObject({ phase: "connected", wsCode: 4000 + ErrorCode.GOING_AWAY, fatal: false });
    expect(disconnects[1]).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true });
  });
});

// --- item 2: unexpected-response drains and destroys the request/response --

describe("MixerClient item 2: unexpected-response cleans up req/res instead of leaking them", () => {
  it("resumes and destroys the response, destroys the request, and terminates the socket", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });

    const started = client.start();
    started.catch(() => {});
    await tick();

    const terminateSpy = vi.spyOn(sockets[0]!, "terminate");
    const req = { destroy: vi.fn() };
    const res = { statusCode: 401, headers: {}, resume: vi.fn(), destroy: vi.fn() };
    // `ws` skips its own abortHandshake cleanup because a listener is
    // attached (dialWebSocket's own `ws.once("unexpected-response", ...)`),
    // so the SDK must drain/destroy req/res itself or they leak.
    sockets[0]!.emit("unexpected-response", req, res);
    await tick();

    expect(res.resume).toHaveBeenCalledTimes(1);
    expect(req.destroy).toHaveBeenCalledTimes(1);
    expect(res.destroy).toHaveBeenCalledTimes(1);
    expect(terminateSpy).toHaveBeenCalledTimes(1);

    await expect(started).rejects.toBeTruthy();
  });

  it("a missing res (and req) never throws out of ws's emit, and still settles the dial for a retry", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number; cause: string }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await tick();

    // `ws`'s real event always carries both, but a defensive transport/mock
    // might not -- emitting must not throw synchronously out of dispatch.
    expect(() => sockets[0]!.emit("unexpected-response", undefined, undefined)).not.toThrow();
    await tick();

    // Never fatal, treated like any other unrecognized dial failure: no
    // errorCode/httpStatus, and it doesn't give up -- the dial promise
    // settled (rejected) rather than leaving connectOnce hanging forever.
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "dial", fatal: false, httpStatus: undefined, errorCode: undefined });
    expect(reconnecting).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(reconnecting[0]!.delayMs);
    expect(sockets.length).toBe(2); // retried instead of hanging

    await connectSocket(sockets, 1);
    await started;
    await client.close();
  });
});

// --- closeReason: the peer's WS close-frame reason, received from the peer -
// only, never this side's own outgoing reason (CLIENT-SDK.md's closeReason
// row) --------------------------------------------------------------------

describe("MixerClient closeReason", () => {
  it("connected phase: peer closes 4009 with a reason and no error{} carries that reason as closeReason", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(4000 + ErrorCode.ENHANCE_YOUR_CALM, "excessive load; back off");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      wsCode: 4000 + ErrorCode.ENHANCE_YOUR_CALM,
      closeReason: "excessive load; back off",
    });

    await client.close();
  });

  it("connected phase: peer sends error{code:9} then closes -- errorCode/name/message as today, closeReason absent (this side closes on error{} without reading the close frame that follows)", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(
      encodeData(
        0,
        encodeControl({ t: "error", code: ErrorCode.ENHANCE_YOUR_CALM, message: "too many stream-0 messages" } as ControlMessage),
      ),
    );
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      wsCode: 4000 + ErrorCode.ENHANCE_YOUR_CALM,
      errorCode: ErrorCode.ENHANCE_YOUR_CALM,
      errorName: "ENHANCE_YOUR_CALM",
      message: "too many stream-0 messages",
    });
    // Never this side's own outgoing error{} message/close reason echoed
    // back as if it were something the peer told us.
    expect(disconnects[0]!.closeReason).toBeUndefined();

    await client.close();
  });

  it("the error{}+close disconnect above is reported synchronously -- no timer needs to elapse to observe it", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(
      encodeData(
        0,
        encodeControl({ t: "error", code: ErrorCode.GOING_AWAY, message: "shutting down" } as ControlMessage),
      ),
    );
    // No `await tick()`, no `vi.advanceTimersByTimeAsync(...)` -- teardownConn
    // emits 'close' in the same synchronous call as dispatching the error{}
    // control frame, so this must already be populated.
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ wsCode: 4000 + ErrorCode.GOING_AWAY, errorCode: ErrorCode.GOING_AWAY });

    await client.close();
  });

  it("abnormal closure (1006, no close frame) leaves closeReason absent", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(1006, "");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]!.wsCode).toBe(1006);
    expect(disconnects[0]!.closeReason).toBeUndefined();

    await client.close();
  });

  it("a self-initiated client.close() never reports the fake socket's own echoed reason as closeReason", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    await client.close();
    await tick();

    expect(disconnects).toHaveLength(1);
    // FakeSocket.close() always echoes back whatever this side passed to
    // ws.close() (its own "client closing" reason) -- teardownConn's own
    // 'close' emission already happened, synchronously, before that echo, so
    // it must never surface as closeReason.
    expect(disconnects[0]!.closeReason).toBeUndefined();
  });

  it("handshake phase: a bare close (no error{}) after the upgrade but before welcome is phase 'handshake', not a timeout", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();

    // The peer closes right after the 101 upgrade, well within the hello
    // timeout, with no ws-mixer error{} ever sent.
    sockets[0]!.serverClose(4000 + ErrorCode.ENHANCE_YOUR_CALM, "rejecting: over the session cap");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.ENHANCE_YOUR_CALM,
      closeReason: "rejecting: over the session cap",
      fatal: false,
    });
    expect(disconnects[0]!.message).not.toMatch(/no welcome within/);

    await client.close();
  });
});

// --- handshake-phase fatal classification: a bare close carrying a --------
// ws-mixer wire code must classify (and go fatal for 4010) the same way a
// connected-phase one already does, instead of trusting onSocketClose's
// generic INTERNAL_ERROR placeholder ----------------------------------------

describe("MixerClient handshake-phase close classification", () => {
  it("bare close 4010 (UNSUPPORTED) before welcome is fatal: no reconnect scheduled, start() rejects", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.serverClose(4000 + ErrorCode.UNSUPPORTED, "version mismatch");
    await tick();
    await vi.advanceTimersByTimeAsync(120000); // give a buggy implementation every chance to retry

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1); // never retried
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.UNSUPPORTED,
      errorCode: ErrorCode.UNSUPPORTED,
      errorName: "UNSUPPORTED",
      fatal: true,
    });
  });

  // 4011 (UNAUTHORIZED) itself moved to "MixerClient change 1/1b: unified
  // pre-welcome auth-rejection retry" above: a token PROVIDER now gets one
  // refresh-retry for this shape too (Change 1); this is the STATIC-token
  // variant that stays fatal on the very first rejection (Change 1b) --
  // see that describe block for the provider variants (0.3.1 pinned this as
  // always-fatal, with no distinction between a provider and a static token,
  // which is no longer the case).

  it("bare close 4009 (ENHANCE_YOUR_CALM) before welcome is still non-fatal, normal reconnect", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.serverClose(4000 + ErrorCode.ENHANCE_YOUR_CALM, "too many attempts");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.ENHANCE_YOUR_CALM,
      errorCode: ErrorCode.ENHANCE_YOUR_CALM,
      errorName: "ENHANCE_YOUR_CALM",
      fatal: false,
    });

    await client.close();
  });
});

// --- locally-generated wsCode for a handshake failure this side raised -----
// itself (no close frame observed): CLIENT-SDK.md's "Handshake-phase close"
// row -- the welcome timeout carries wsCode 4001, no closeReason -----------

describe("MixerClient handshake failures raised locally carry their own wsCode", () => {
  it("the hello/welcome timeout reports wsCode 4001 (PROTOCOL_ERROR) and no closeReason", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    await vi.advanceTimersByTimeAsync(10000); // DEFAULT_HELLO_TIMEOUT_MS
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.PROTOCOL_ERROR,
      errorCode: ErrorCode.PROTOCOL_ERROR,
      fatal: false,
    });
    expect(disconnects[0]!.closeReason).toBeUndefined();

    await client.close();
  });
});

// --- the normative pre-welcome rejection: error{code,message}+close before -
// welcome (OVERVIEW.md section 3.4's Authenticate hook,
// spec/fixtures/sequences/auth_failure.json) -- NOT a protocol violation,
// surfaces with the peer's own code, and the client sends no reply --------

describe("MixerClient pre-welcome error{} rejection (auth_failure.json shape)", () => {
  it("error{11 UNAUTHORIZED} before welcome, STATIC token: fatal, wsCode 4011, the server's message, no reply sent, no reconnect, start() rejects", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const fatalEvents: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "stale-or-revoked-token",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("fatal", (e) => fatalEvents.push(e));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.push(
      encodeData(
        0,
        encodeControl({
          t: "error",
          code: ErrorCode.UNAUTHORIZED,
          message: "token invalid: signature verification failed",
        } as ControlMessage),
      ),
    );
    await tick();
    await vi.advanceTimersByTimeAsync(120000); // give a buggy implementation every chance to retry

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1); // never retried: static token, no refresh possible
    expect(fatalEvents).toHaveLength(1);
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.UNAUTHORIZED,
      errorCode: ErrorCode.UNAUTHORIZED,
      errorName: "UNAUTHORIZED",
      message: "token invalid: signature verification failed",
      fatal: true,
    });
    expect(disconnects[0]!.closeReason).toBeUndefined();

    // WIRE.md section 2.7: a peer that receives `error` MUST NOT reply with
    // its own -- assert directly on what actually went out over the wire.
    const errorReplies = sockets[0]!.sent
      .map((frame) => {
        try {
          const decoded = decodeFrame(frame);
          if (decoded.streamId !== 0 || decoded.type !== FrameType.DATA) return null;
          return JSON.parse(Buffer.from(decoded.payload).toString("utf8")) as { t: string };
        } catch {
          return null;
        }
      })
      .filter((m): m is { t: string } => !!m && m.t === "error");
    expect(errorReplies).toHaveLength(0);
  });

  it("error{10 UNSUPPORTED} before welcome: fatal", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.push(
      encodeData(0, encodeControl({ t: "error", code: ErrorCode.UNSUPPORTED, message: "unsupported version" } as ControlMessage)),
    );
    await tick();
    await vi.advanceTimersByTimeAsync(120000);

    await expect(started).rejects.toBeTruthy();
    expect(sockets.length).toBe(1); // never retried
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.UNSUPPORTED,
      errorCode: ErrorCode.UNSUPPORTED,
      errorName: "UNSUPPORTED",
      fatal: true,
    });
    expect(disconnects[0]!.closeReason).toBeUndefined();
  });

  it("error{9 ENHANCE_YOUR_CALM} before welcome: non-fatal, normal backoff, wsCode 4009", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number; cause: string }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    sockets[0]!.push(
      encodeData(0, encodeControl({ t: "error", code: ErrorCode.ENHANCE_YOUR_CALM, message: "too many attempts" } as ControlMessage)),
    );
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.ENHANCE_YOUR_CALM,
      errorCode: ErrorCode.ENHANCE_YOUR_CALM,
      errorName: "ENHANCE_YOUR_CALM",
      fatal: false,
    });
    expect(disconnects[0]!.closeReason).toBeUndefined();
    expect(reconnecting).toHaveLength(1); // normal backoff was actually scheduled

    await client.close();
  });
});

// --- Change 3: a failure after the 101 and before welcome is ALWAYS phase ---
// "handshake", reported deterministically regardless of whether 'error' or
// 'close' happens to arrive first (or 'close' never arrives at all) --------

describe("MixerClient change 3: handshake-phase 'error'/'close' converge on one deterministic report", () => {
  it("'error' then 'close'(1006) delivered ~2ms later (a later macrotask, matching real ws 8.21's own behaviour): one report, wsCode 1006, no errorCode, non-fatal, reconnect scheduled", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: unknown[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();

    sockets[0]!.emit("error", new Error("read ECONNRESET"));
    await tick();
    expect(disconnects).toHaveLength(0); // onSocketError never reports by itself, only records the message
    // Real `ws` never delivers 'close' in the same microtask as a preceding
    // 'error' -- a protocol-level failure it detects delivers 'error'
    // immediately followed by 'close' a macrotask or more later. Schedule it
    // the same way here (a real timer, not a synchronous emit) instead.
    setTimeout(() => sockets[0]!.emit("close", 1006, Buffer.from("")), 2);
    await vi.advanceTimersByTimeAsync(2);
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "handshake", wsCode: 1006, fatal: false, message: "read ECONNRESET" });
    expect(disconnects[0]!.errorCode).toBeUndefined();
    expect(disconnects[0]!.errorName).toBeUndefined();
    expect(disconnects[0]!.message).not.toMatch(/no welcome within/);
    expect(reconnecting).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(reconnecting[0]! ? (reconnecting[0] as { delayMs: number }).delayMs : 0);
    await client.close();
  });

  it("'close'(1006) then 'error' (a hypothetical ordering real ws never actually produces, but the code stays order-independent): still the same one report", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();

    sockets[0]!.emit("close", 1006, Buffer.from(""));
    sockets[0]!.emit("error", new Error("read ECONNRESET")); // arrives after 'close' already reported and closed -- a no-op
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "handshake", wsCode: 1006, fatal: false });

    await vi.advanceTimersByTimeAsync(100);
    await client.close();
  });

  it("only 'close'(1006), no 'error' at all: unaffected, still one report", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();

    sockets[0]!.emit("close", 1006, Buffer.from(""));
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "handshake", wsCode: 1006, fatal: false });

    await vi.advanceTimersByTimeAsync(100);
    await client.close();
  });

  it("only 'error', with no 'close' ever following: onSocketError never finalizes anything itself -- the ordinary hello/welcome timeout is what eventually reports it (wsCode 4001), not a second mechanism", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      reconnect: { base: 10, cap: 100 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();

    sockets[0]!.emit("error", new Error("boom, no close ever follows"));
    await tick();
    // Nothing reported yet: onSocketError only recorded the message, it
    // never rejects/finalizes the handshake by itself.
    expect(disconnects).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(10000); // DEFAULT_HELLO_TIMEOUT_MS
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "handshake",
      wsCode: 4000 + ErrorCode.PROTOCOL_ERROR,
      errorCode: ErrorCode.PROTOCOL_ERROR,
      fatal: false,
    });
    expect(disconnects[0]!.message).toMatch(/no welcome within/);

    await client.close();
  });

  it("a connect() cancelled by client.close() mid-handshake is not reported as a handshake failure (unchanged)", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    // hello sent, no welcome yet: MixerConn exists as dialingConn only.

    await client.close();
    await tick();

    await expect(started).rejects.toBeTruthy();
    expect(disconnects).toHaveLength(0);
  });

  it("connected phase: a bare close with no reason yields message 'socket closed with code N', not an empty string (the emitted 'close' payload's message now always gets this fallback, in both phases, not just the handshake-phase pendingHandshakeErrorMessage one)", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(4000 + ErrorCode.APPLICATION_CLOSE, "");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "connected",
      wsCode: 4000 + ErrorCode.APPLICATION_CLOSE,
      message: `socket closed with code ${4000 + ErrorCode.APPLICATION_CLOSE}`,
    });
    expect(disconnects[0]!.closeReason).toBeUndefined();

    await client.close();
  });
});

describe("MixerClient close({ message })", () => {
  it("performs error{code:14,message} + WS close 4014 (message truncated to 123 bytes), no drain, no reconnect", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    const longMessage = "x".repeat(200);
    await client.close({ message: longMessage });

    const errorFrames = s0.sent
      .map((frame) => {
        try {
          const decoded = decodeFrame(frame);
          if (decoded.streamId !== 0 || decoded.type !== FrameType.DATA) return null;
          return JSON.parse(Buffer.from(decoded.payload).toString("utf8")) as { t: string; code?: number; message?: string };
        } catch {
          return null;
        }
      })
      .filter((m): m is { t: string; code?: number; message?: string } => !!m && m.t === "error");
    expect(errorFrames).toHaveLength(1);
    expect(errorFrames[0]!.code).toBe(ErrorCode.APPLICATION_CLOSE);
    expect(errorFrames[0]!.message).toBe(longMessage); // error{} carries the message untruncated

    expect(s0.closedWith?.code).toBe(4000 + ErrorCode.APPLICATION_CLOSE);
    expect(s0.closedWith?.reason).toBe(longMessage.slice(0, 123)); // WS close reason IS truncated (ascii here, so char count == byte count)

    expect(client.currentState()).toBe("closed");
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]!.fatal).toBe(false);
    expect(disconnects[0]!.wsCode).toBe(4000 + ErrorCode.APPLICATION_CLOSE);
    expect(disconnects[0]!.errorCode).toBe(ErrorCode.APPLICATION_CLOSE);
    expect(disconnects[0]!.errorName).toBe("APPLICATION_CLOSE");

    await vi.advanceTimersByTimeAsync(120000);
    expect(sockets.length).toBe(1); // never reconnected
  });

  it("truncates a multi-byte reason to <=123 UTF-8 bytes on a character boundary (no lone surrogate / U+FFFD)", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    // "é" (U+00E9) is 2 UTF-8 bytes: 100 of them is 200 bytes, well past the
    // 123-byte close-reason limit, and not evenly divisible into it (123 is
    // odd) -- truncateUtf8's character-boundary walk has to actually back
    // off from a mid-character byte, not just slice(0, 123).
    const multiByteMessage = "é".repeat(100);
    await client.close({ message: multiByteMessage });

    const reason = s0.closedWith?.reason ?? "";
    const reasonBytes = new TextEncoder().encode(reason);
    expect(reasonBytes.length).toBeLessThanOrEqual(123);
    expect(reason).not.toContain("�"); // no lossy replacement character: a clean character-boundary cut
    expect(reason).toBe("é".repeat(Math.floor(reasonBytes.length / 2))); // every "é" in the result is whole
  });

  it("defaults to today's graceful drain-then-close when no message is given", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const client = new MixerClient("wss://x/tunnel", { token: "t", _wsFactory: makeFactory(sockets) });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    await client.close();
    await tick();

    // Unchanged from before opts.message existed: NO_ERROR, WS close 1000.
    expect(s0.closedWith?.code).toBe(1000);
  });

  it("throws a TypeError synchronously for a leftover `code` key (with or without message), before touching the connection", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;
    const sentBefore = s0.sent.length;

    expect(() => client.close({ code: 14 } as any)).toThrow(TypeError);
    expect(() => client.close({ code: 14 } as any)).toThrow(/D-2026-09-25-01/);
    expect(() => client.close({ code: 14 } as any)).toThrow(/message/);
    expect(() => client.close({ code: 0, message: "x" } as any)).toThrow(TypeError);
    await tick();

    // No side effects: still connected, nothing sent, socket open, no report.
    expect(client.currentState()).toBe("connected");
    expect(s0.sent.length).toBe(sentBefore);
    expect(s0.closedWith).toBeNull();
    expect(disconnects).toHaveLength(0);

    await client.close({ message: "bye" });
    expect(s0.closedWith?.code).toBe(4000 + ErrorCode.APPLICATION_CLOSE);
  });

  it("close({message}) while the handshake is still in flight (mid-dial): the app's message reaches the wire; no report and no redial, matching the pre-existing no-args close() behaviour in this window", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets, { autoWelcome: false }),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await tick();
    sockets[0]!.open();
    await tick();
    // hello sent, no welcome yet -- MixerConn exists as dialingConn, not yet promoted to conn.

    await client.close({ message: "APP-BYE" });
    await tick();

    const errorFrames = sockets[0]!.sent
      .map((frame) => {
        try {
          const decoded = decodeFrame(frame);
          if (decoded.streamId !== 0 || decoded.type !== FrameType.DATA) return null;
          return JSON.parse(Buffer.from(decoded.payload).toString("utf8")) as { t: string; code?: number; message?: string };
        } catch {
          return null;
        }
      })
      .filter((m): m is { t: string; code?: number; message?: string } => !!m && m.t === "error");
    // The app's message, not the hard-coded NO_ERROR/"client closing"
    // dialingConn.fail() used before this fix.
    expect(errorFrames).toHaveLength(1);
    expect(errorFrames[0]!.code).toBe(ErrorCode.APPLICATION_CLOSE);
    expect(errorFrames[0]!.message).toBe("APP-BYE");
    expect(sockets[0]!.closedWith?.code).toBe(4000 + ErrorCode.APPLICATION_CLOSE);

    // A dialingConn never became `conn` (connectOnce's catch and wireConn's
    // own 'close' handler both treat it as "already handled elsewhere" and
    // stay silent once `this.closing` is set) -- this is pre-existing
    // behaviour, identical to the no-args close() mid-handshake case (see
    // the sibling "close() during an in-flight dial" test above); opts.message
    // does not change it, only what actually reaches the wire.
    expect(disconnects).toHaveLength(0);

    expect(client.currentState()).toBe("closed");
    await vi.advanceTimersByTimeAsync(120000);
    expect(sockets.length).toBe(1); // never reconnected
  });

  it("close({message}) mid-backoff: stops reconnecting and clears every timer, but -- like plain close() in the same window -- reports nothing (no connection was ever established for this cycle)", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000, stableAfter: 10000 },
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    // Drop and let it enter "backoff" (a pending reconnect timer, plus the
    // (already-cleared, since the conn ended) stability timer) before close().
    sockets[0]!.serverClose(1006, "abnormal");
    await tick();
    expect(client.currentState()).toBe("backoff");
    // The 1006 above already produced its own one non-fatal report.
    expect(disconnects).toHaveLength(1);

    await client.close({ message: "operator shutdown" });
    expect(client.currentState()).toBe("closed");
    expect(vi.getTimerCount()).toBe(0);
    // No live connection existed to send error{}/close 4014 over --
    // close({message}) mid-backoff degrades to the same "closes, stays closed,
    // reports nothing" shape as plain close() in this window (there is no
    // second report for the backoff itself ending).
    expect(disconnects).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(120000);
    expect(sockets.length).toBe(1); // never reconnected
  });

  it("close({message: \"\"}) still takes the application-close path -- error{code:14,message:\"\"} + WS close 4014, not the graceful default", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    await client.close({ message: "" });

    const errorFrames = s0.sent
      .map((frame) => {
        try {
          const decoded = decodeFrame(frame);
          if (decoded.streamId !== 0 || decoded.type !== FrameType.DATA) return null;
          return JSON.parse(Buffer.from(decoded.payload).toString("utf8")) as { t: string; code?: number; message?: string };
        } catch {
          return null;
        }
      })
      .filter((m): m is { t: string; code?: number; message?: string } => !!m && m.t === "error");
    expect(errorFrames).toHaveLength(1);
    expect(errorFrames[0]!.code).toBe(ErrorCode.APPLICATION_CLOSE);
    expect(errorFrames[0]!.message).toBe("");

    expect(s0.closedWith?.code).toBe(4000 + ErrorCode.APPLICATION_CLOSE);

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]!.fatal).toBe(false);
    expect(disconnects[0]!.wsCode).toBe(4000 + ErrorCode.APPLICATION_CLOSE);
    expect(disconnects[0]!.errorCode).toBe(ErrorCode.APPLICATION_CLOSE);
    expect(disconnects[0]!.errorName).toBe("APPLICATION_CLOSE");
  });

  it("close({}) -- no message key at all -- takes the graceful drain-then-close path, WS close 1000, no error frame", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    await client.close({});
    await tick();

    const errorFrames = s0.sent
      .map((frame) => {
        try {
          const decoded = decodeFrame(frame);
          if (decoded.streamId !== 0 || decoded.type !== FrameType.DATA) return null;
          return JSON.parse(Buffer.from(decoded.payload).toString("utf8")) as { t: string; code?: number; message?: string };
        } catch {
          return null;
        }
      })
      .filter((m): m is { t: string; code?: number; message?: string } => !!m && m.t === "error");
    // No APPLICATION_CLOSE error frame -- the graceful path's own error{NO_ERROR}
    // (drain-then-close) is expected and unrelated to this fix.
    expect(errorFrames.some((f) => f.code === ErrorCode.APPLICATION_CLOSE)).toBe(false);

    // Unchanged from before opts.message existed: NO_ERROR, WS close 1000.
    expect(s0.closedWith?.code).toBe(1000);
  });
});

// --- 0x0e APPLICATION_CLOSE: connection-level, non-fatal, starts at cap ---

describe("MixerClient 0x0e APPLICATION_CLOSE", () => {
  it("error{code:14,message} then close 4014 is named APPLICATION_CLOSE, non-fatal, and starts backoff at the cap (not at base) -- WIRE.md section 2.9: 4014 always follows welcome, so plain backoff would never climb", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // pin full-jitter's sample to its ceiling
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number; cause: string }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    const s0 = await connectSocket(sockets, 0);
    await started;

    s0.push(
      encodeData(
        0,
        encodeControl({ t: "error", code: ErrorCode.APPLICATION_CLOSE, message: "application says goodbye" } as ControlMessage),
      ),
    );
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      wsCode: 4000 + ErrorCode.APPLICATION_CLOSE,
      errorCode: ErrorCode.APPLICATION_CLOSE,
      errorName: "APPLICATION_CLOSE",
      message: "application says goodbye",
      fatal: false,
    });
    expect(disconnects[0]!.protocolError).toBeFalsy();
    expect(reconnecting).toHaveLength(1);
    // Math.random pinned to 1: a first-attempt normal backoff would ceiling
    // at base*2^1 = 2000ms; starting "at the cap" instead ceilings at 60000ms.
    expect(reconnecting[0]!.delayMs).toBe(60000);

    await vi.advanceTimersByTimeAsync(60000);
    await connectSocket(sockets, 1);
    await client.close();
  });

  it("a bare close 4014 (no error{}) also derives errorName APPLICATION_CLOSE from the wire code and starts backoff at the cap", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const reconnecting: Array<{ attempt: number; delayMs: number; cause: string }> = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      reconnect: { base: 1000, cap: 60000 },
      onDisconnect: (r) => disconnects.push(r),
    });
    client.on("reconnecting", (info) => reconnecting.push(info));

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(4000 + ErrorCode.APPLICATION_CLOSE, "goodbye");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      wsCode: 4000 + ErrorCode.APPLICATION_CLOSE,
      errorCode: ErrorCode.APPLICATION_CLOSE,
      errorName: "APPLICATION_CLOSE",
      closeReason: "goodbye",
      fatal: false,
    });
    expect(reconnecting).toHaveLength(1);
    expect(reconnecting[0]!.delayMs).toBe(60000);

    await client.close();
  });

  it("connected phase: a bare close with an unrecognised ws-mixer code (4777) still derives errorCode 777, errorName INTERNAL_ERROR", async () => {
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    const disconnects: DisconnectPayload[] = [];
    const client = new MixerClient("wss://x/tunnel", {
      token: "t",
      _wsFactory: makeFactory(sockets),
      onDisconnect: (r) => disconnects.push(r),
    });

    const started = client.start();
    started.catch(() => {});
    await connectSocket(sockets, 0);
    await started;

    sockets[0]!.serverClose(4777, "unknown code");
    await tick();

    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      phase: "connected",
      wsCode: 4777,
      errorCode: 777,
      errorName: "INTERNAL_ERROR",
    });

    await client.close();
  });

  it("connected phase: a bare close in 1000/1001/1006/1009/1011 (not a ws-mixer code) carries no errorCode/errorName", async () => {
    vi.useFakeTimers();
    for (const wsCode of [1000, 1001, 1006, 1009, 1011]) {
      const sockets: FakeSocket[] = [];
      const disconnects: DisconnectPayload[] = [];
      const client = new MixerClient("wss://x/tunnel", {
        token: "t",
        _wsFactory: makeFactory(sockets),
        onDisconnect: (r) => disconnects.push(r),
      });

      const started = client.start();
      started.catch(() => {});
      await connectSocket(sockets, 0);
      await started;

      sockets[0]!.serverClose(wsCode, "");
      await tick();

      expect(disconnects).toHaveLength(1);
      expect(disconnects[0]).toMatchObject({ phase: "connected", errorCode: undefined, errorName: undefined });

      await client.close();
    }
  });
});
