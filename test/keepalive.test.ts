/**
 * Keepalive: ping/pong bookkeeping and the dead-peer watchdog (mirrors
 * ping_pong_then_dead_peer_timeout.json's shape). Uses real timers, not
 * vitest's fake timers -- FakeWS's `send()` resolves callbacks via a real
 * `queueMicrotask`, and this SDK's writer loop awaits those callbacks, so a
 * fake-timer clock never actually lets that microtask chain settle.
 *
 * The wire's ping_interval/ping_timeout floors (5000ms / 2x) are enforced
 * unconditionally by control.ts's `parseWelcome`, on every `welcome` that
 * actually round-trips through JSON on the wire -- there is no way to send a
 * sub-floor `welcome` and have it decode. So this suite drives the
 * handshake's private `applyWelcome()` directly (bypassing the wire
 * encode/decode step, the same way the existing `conn.test.ts` reaches into
 * `sendPing`/`outstandingPings`) with `ConnOptions._timing` (test-only)
 * relaxing *that* method's own floor check, letting the whole suite run with
 * real, but tiny, delays -- well under a second total.
 */
import { describe, expect, it } from "vitest";
import { FakeWS } from "./helpers/fake-ws.js";
import { MixerConn } from "../src/conn.js";
import { decodeFrame, encodeData } from "../src/frame.js";
import { encodeControl } from "../src/control.js";

const PING_INTERVAL = 20;
const PING_TIMEOUT = 40; // exactly 2x, the wire's normal minimum ratio

async function handshaken(ws: FakeWS) {
  const conn = new MixerConn(ws, {
    token: "t",
    agent: { sdk: "x", sdk_version: "0" },
    _timing: { minPingInterval: 5 },
  });
  const p = conn.handshake();
  await new Promise((r) => setImmediate(r));
  (conn as unknown as { applyWelcome: (w: unknown) => void }).applyWelcome({
    t: "welcome",
    v: 1,
    session: "s",
    window: 262144,
    max_streams: 64,
    ping_interval: PING_INTERVAL,
    ping_timeout: PING_TIMEOUT,
  });
  await p;
  return conn;
}

describe("keepalive", () => {
  it("answers a received ping with pong immediately", async () => {
    const ws = new FakeWS();
    const conn = await handshaken(ws);
    conn.on("error", () => {});
    ws.sent.length = 0;
    ws.receive(encodeData(0, encodeControl({ t: "ping", id: 7, ts: 123 } as never)));
    await new Promise((r) => setImmediate(r));
    const msg = JSON.parse(Buffer.from(decodeFrame(ws.sent[0]!).payload).toString());
    expect(msg).toMatchObject({ t: "pong", id: 7, ts: 123 });
  });

  it("tracks RTT on a received pong for our own ping", async () => {
    const ws = new FakeWS();
    const conn = await handshaken(ws);
    conn.on("error", () => {});
    const pongEvent = new Promise((resolve) => conn.on("pong", resolve));
    // First ping is jittered by random(0, ping_interval); poll briefly for it.
    let pingFrame: Uint8Array | undefined;
    const deadline = Date.now() + 500;
    while (!pingFrame && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 2));
      pingFrame = ws.sent.find((f) => decodeFrame(f).streamId === 0 && JSON.parse(Buffer.from(decodeFrame(f).payload).toString()).t === "ping");
    }
    expect(pingFrame, "expected a ping to have been sent").toBeDefined();
    const ping = JSON.parse(Buffer.from(decodeFrame(pingFrame!).payload).toString()) as { id: number; ts: number };
    ws.receive(encodeData(0, encodeControl({ t: "pong", id: ping.id, ts: ping.ts } as never)));
    const info = await pongEvent;
    expect((info as { id: number }).id).toBe(ping.id);
  });

  it("pong for an id never sent is a connection PROTOCOL_ERROR", async () => {
    const ws = new FakeWS();
    const conn = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    ws.receive(encodeData(0, encodeControl({ t: "pong", id: 999 } as never)));
    const close = await closeP;
    expect(close.wsCode).toBe(4001);
  });

  it("fires KEEPALIVE_TIMEOUT (close 4013) after ping_timeout with no pong", async () => {
    const ws = new FakeWS();
    const conn = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    const close = await closeP;
    expect(close.wsCode).toBe(4013);
  });
});
