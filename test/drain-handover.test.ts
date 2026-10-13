/**
 * Drain hand-over (WIRE.md section 2.9): after `drain`, the client dials a
 * replacement in parallel; once it welcomes, new streams go there while the
 * drained connection finishes its in-flight streams, then closes itself
 * (empty table, or the drain deadline). Its end is reported once, non-fatal.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MixerClient, type DisconnectPayload } from "../src/client.js";
import { encodeControl, type ControlMessage } from "../src/control.js";
import { ErrorCode } from "../src/errors.js";
import { FrameType, decodeFrame, encodeClose, encodeData, encodeOpen, encodeReset } from "../src/frame.js";
import type { MixerStream } from "../src/stream.js";
import { FakeSocket, connectSocket, makeFactory, tick } from "./helpers/fake-socket.js";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | null) => (b === null ? null : new TextDecoder().decode(b));
const control = (m: Record<string, unknown>) => encodeData(0, encodeControl(m as unknown as ControlMessage));

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

/** Stream-1 frames a socket sent, as "DATA:<text>" / "CLOSE" / "RESET". */
function stream1Frames(s: FakeSocket): string[] {
  const out: string[] = [];
  for (const raw of s.sent) {
    const f = decodeFrame(raw);
    if (f.streamId !== 1) continue;
    if (f.type === FrameType.DATA) out.push(`DATA:${dec(f.payload)}`);
    else if (f.type === FrameType.CLOSE) out.push("CLOSE");
    else if (f.type === FrameType.RESET) out.push("RESET");
  }
  return out;
}

async function setup() {
  vi.useFakeTimers();
  // Pins the drain reconnect jitter (random(0, 2000)) and the first-ping jitter.
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  const sockets: FakeSocket[] = [];
  const disconnects: DisconnectPayload[] = [];
  const streams: MixerStream[] = [];
  const client = new MixerClient("wss://x", {
    token: "t",
    webSocket: makeFactory(sockets),
    onDisconnect: (r) => disconnects.push(r),
    onStream: (s) => {
      streams.push(s);
    },
  });
  const started = client.start();
  const a = await connectSocket(sockets, 0);
  await started;
  const connA = client.currentConn()!;

  // One in-flight request on A: OPEN + body + CLOSE, not yet answered.
  a.push(encodeOpen(1));
  a.push(encodeData(1, enc("req")));
  a.push(encodeClose(1));
  await tick();
  expect(streams).toHaveLength(1);

  a.push(control({ t: "drain", reason: "rollout", last_stream_id: 1, deadline_ms: 25000 }));
  await tick();
  await vi.advanceTimersByTimeAsync(1000); // the pinned 0-2000 ms jitter
  expect(sockets).toHaveLength(2);
  const b = await connectSocket(sockets, 1);
  return { client, sockets, disconnects, streams, a, b, connA, stream: streams[0]! };
}

describe("drain hand-over", () => {
  it("an in-flight stream survives the replacement's welcome and finishes on the old connection, which then closes (1000), reported once", async () => {
    const { client, sockets, disconnects, a, connA, stream } = await setup();

    expect(client.currentConn()).not.toBe(connA);
    expect(client.currentState()).toBe("connected");
    expect(a.closedWith).toBeNull();
    expect(stream.getState()).toBe("half_closed_remote");
    expect(dec(await stream.read())).toBe("req");
    expect(await stream.read()).toBeNull();

    await stream.write("resp");
    await stream.closeWrite();
    await tick();

    expect(stream1Frames(a)).toEqual(["DATA:resp", "CLOSE"]);
    expect(a.closedWith?.code).toBe(1000);
    await expect(stream.closed).resolves.toEqual({ kind: "finished" });
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "connected", fatal: false, wsCode: 1000 });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(sockets).toHaveLength(2);
    expect(client.currentState()).toBe("connected");

    await client.close();
  });

  it("new streams after the hand-over land on the replacement, never on the old connection", async () => {
    const { client, streams, a, b, stream } = await setup();

    b.push(encodeOpen(1));
    b.push(encodeData(1, enc("second")));
    await tick();
    expect(streams).toHaveLength(2);
    const second = streams[1]!;
    expect(second).not.toBe(stream);
    expect(dec(await second.read())).toBe("second");
    await second.write("for-b");
    await tick();

    expect(stream1Frames(b)).toEqual(["DATA:for-b"]);
    expect(stream1Frames(a)).toEqual([]);

    const closing = client.close();
    await vi.advanceTimersByTimeAsync(5000); // graceful close waits out its grace period for B's open stream
    await closing;
  });

  it("client-side deadline: a stream never finished is cancelled at deadline_ms + 1 s, the old connection closes, reported once, non-fatal", async () => {
    const { client, sockets, disconnects, a, stream } = await setup();

    await vi.advanceTimersByTimeAsync(25_000);
    expect(a.closedWith).toBeNull();
    await vi.advanceTimersByTimeAsync(1_000);
    await tick();

    expect(a.closedWith).not.toBeNull();
    await expect(stream.closed).resolves.toEqual({ kind: "reset", code: ErrorCode.CANCEL, name: "CANCEL", message: "connection drained", byPeer: false });
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "connected", fatal: false });
    expect(sockets).toHaveLength(2);
    expect(client.currentState()).toBe("connected");

    await client.close();
  });

  it("server deadline first: RESET(CANCEL) + error{GOING_AWAY} + close 4012 on the old connection, reported once with wsCode 4012, no extra reconnect", async () => {
    const { client, sockets, disconnects, a, stream } = await setup();

    a.push(encodeReset(1, ErrorCode.CANCEL, "drain deadline"));
    a.push(control({ t: "error", code: ErrorCode.GOING_AWAY, message: "drain deadline passed" }));
    a.serverClose(4012, "drain deadline passed");
    await tick();

    await expect(stream.closed).resolves.toMatchObject({ kind: "reset", code: ErrorCode.CANCEL, byPeer: true });
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ phase: "connected", fatal: false, wsCode: 4012 });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(sockets).toHaveLength(2);
    expect(client.currentState()).toBe("connected");

    await client.close();
  });

  it("close() during the hand-over closes both connections on one graceful path, without another reconnect", async () => {
    const { client, sockets, disconnects, a, b, stream } = await setup();

    await client.close();
    await tick();

    expect(a.closedWith).not.toBeNull();
    expect(b.closedWith).not.toBeNull();
    await expect(stream.closed).resolves.toMatchObject({ kind: "connection_lost" });
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({ fatal: false });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(sockets).toHaveLength(2);
    expect(client.currentState()).toBe("closed");
  });
});
