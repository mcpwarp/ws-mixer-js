/**
 * Reconnect/backoff policy from docs/research/2026-08-26-control-channel-and-connection-lifecycle.md
 * and OVERVIEW.md section 2.9's reconnect table, exercised end-to-end
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
import { ErrorCode, StreamError } from "../src/errors.js";
import type { MixerStream } from "../src/stream.js";

/** Asserts the invariant blocker 3 requires: `currentState() === "connected"` never coincides with `currentConn() === null`. */
function assertConnInvariant(client: MixerClient): void {
  if (client.currentState() === "connected") {
    expect(client.currentConn()).not.toBeNull();
  }
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

// --- end-to-end reconnect policy against a fake transport ---------------------

describe("MixerClient reconnect policy", () => {
  it("resets the attempt counter only on welcome, never on a bare dial success", async () => {
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

    // A second, independent failure must start counting from attempt 1 again.
    sockets[1]!.serverClose(1006, "abnormal");
    await tick();
    expect(reconnecting).toHaveLength(2);
    expect(reconnecting[1]!.attempt).toBe(1);

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
    it(`close ${code} is fatal: no reconnect, 'fatal' fires, onDisconnect({fatal:true})`, async () => {
      vi.useFakeTimers();
      const sockets: FakeSocket[] = [];
      const fatalEvents: unknown[] = [];
      const disconnects: Array<{ fatal: boolean }> = [];
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
    });
  }

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
    // parallel, before the old one closes (OVERVIEW.md section 2.9).
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
    // to its own deadline (OVERVIEW.md section 2.9): maxAttempts:0 means
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
  // completes a fresh welcome) can't observe the ceiling directly: welcome
  // resets `attempt` to 0 on every success (OVERVIEW.md section 2.9: "reset
  // backoff counter HERE, nowhere else"), so a flap loop that always
  // reconnects successfully never accumulates attempts in the first place --
  // by design, only a flap loop that never gets to `welcome` should be
  // bounded. That is exactly reconnectImmediately's own job: it must count
  // every immediate-reconnect *attempt* against the ceiling itself, so a
  // caller who wires it up when replacements keep failing before welcome
  // (a real "stuck" 4012 loop) is protected regardless of how failure gets
  // there. Testing the counting/ceiling logic directly on the private
  // method (as conn.test.ts already does for MixerConn internals like
  // sendPing/outstandingPings) is the precise, non-flaky way to pin that
  // contract down.
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

// --- v0.2 token provider + 401 refresh-retry (OVERVIEW.md section 4.0) --------

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
    expect(disconnects.at(-1)).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true });
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
    expect(disconnects.at(-1)).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true });
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
      // 403 is grouped with 401 (UNAUTHORIZED); 404 is fatal but isn't a
      // ws-mixer wire error, so it gets no errorCode.
      expect(disconnects.at(-1)?.errorCode).toBe(status === 403 ? ErrorCode.UNAUTHORIZED : undefined);
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
      expect(disconnects.at(-1)).toMatchObject({ phase: "dial", httpStatus: status, fatal: false, errorCode: undefined });
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

  it("maxAttempts interplay: the 401 refresh-retry is itself skipped once the ceiling is already reached", async () => {
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

    // ...and now attempt (1) >= maxAttempts (1): a 401 on this dial must not
    // spend a second attempt on the refresh-retry -- it's fatal immediately.
    sockets[1]!.httpReject(401);
    await tick();

    await expect(started).rejects.toBeTruthy();
    expect(calls).toBe(2); // no third provider call for a refresh-retry
    expect(sockets.length).toBe(2); // no third dial
    expect(disconnects.at(-1)).toMatchObject({ phase: "dial", httpStatus: 401, fatal: true });
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
