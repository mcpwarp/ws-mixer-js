/**
 * MixerConn unit tests against the FakeWS transport: handshake, dispatch of
 * OPEN/DATA/WINDOW/CLOSE/RESET, credit-violation -> connection error, and
 * the round-robin DATA writer's exact interleaving (WIRE.md section 2.6
 * rule 3).
 */
import { once } from "node:events";
import { describe, expect, it } from "vitest";
import { FakeWS } from "./helpers/fake-ws.js";
import { MixerConn } from "../src/conn.js";
import { encodeControl } from "../src/control.js";
import { decodeFrame, encodeClose, encodeData, encodeOpen, encodeReset, encodeWindow, FrameType } from "../src/frame.js";
import { ConnError, ErrorCode, StreamError, WsMixerError } from "../src/errors.js";
import { MixerStream } from "../src/stream.js";

function agent() {
  return { sdk: "ws-mixer-js-test", sdk_version: "0.0.0" };
}

async function handshaken(ws: FakeWS, opts?: Partial<{ window: number; maxStreams: number }>) {
  const conn = new MixerConn(ws, { token: "t", agent: agent(), window: opts?.window, maxStreams: opts?.maxStreams });
  const handshakePromise = conn.handshake();
  await new Promise((r) => setImmediate(r));
  // First control frame sent must be `hello`.
  const helloFrame = decodeFrame(ws.sent[0]!);
  const hello = JSON.parse(Buffer.from(helloFrame.payload).toString()) as { t: string };
  expect(hello.t).toBe("hello");

  ws.receive(
    encodeData(
      0,
      encodeControl({
        t: "welcome",
        v: 1,
        session: "01TEST",
        window: opts?.window ?? 262144,
        max_streams: opts?.maxStreams ?? 64,
        ping_interval: 30000,
        ping_timeout: 90000,
      } as never),
    ),
  );
  const welcome = await handshakePromise;
  return { conn, welcome };
}

describe("MixerConn handshake", () => {
  it("sends hello then resolves on welcome", async () => {
    const ws = new FakeWS();
    const { welcome } = await handshaken(ws);
    expect(welcome.session).toBe("01TEST");
  });

  it("rejects (fatal) on version mismatch", async () => {
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: agent() });
    const p = conn.handshake();
    await new Promise((r) => setImmediate(r));
    ws.receive(encodeData(0, encodeControl({ t: "welcome", v: 2 } as never)));
    await expect(p).rejects.toMatchObject({ code: 10 }); // UNSUPPORTED
  });

  it("times out if welcome never arrives", async () => {
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: agent(), helloTimeoutMs: 20 });
    conn.on("error", () => {});
    conn.on("close", () => {});
    await expect(conn.handshake()).rejects.toThrow(/no welcome within/);
  });
});

describe("MixerConn stream dispatch", () => {
  it("OPEN creates a stream and emits it", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    expect(stream.id).toBe(1);
    expect(stream.getState()).toBe("open");
  });

  it("DATA for a never-opened stream is a connection error", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    ws.receive(encodeData(3, new Uint8Array([1, 2, 3])));
    const close = await closeP;
    expect(close.wsCode).toBe(4001); // PROTOCOL_ERROR
  });

  it("DATA exceeding recv credit is connection-fatal FLOW_CONTROL_ERROR (4003)", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { window: 16384 });
    conn.on("error", () => {});
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    stream.on("error", () => {}); // fail() destroys every live stream; a Duplex thrown 'error' with no listener would crash the test
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    ws.receive(encodeData(1, new Uint8Array(20480)));
    const close = await closeP;
    expect(close.wsCode).toBe(4003);
  });

  it("late WINDOW after CLOSE is tolerated, not an error", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    stream.on("error", () => {});
    stream.closeWrite();
    ws.receive(encodeClose(1)); // peer closes too -> fully closed
    await new Promise((r) => setImmediate(r));
    let errored = false;
    conn.on("error", () => (errored = true));
    conn.on("close", () => (errored = true));
    ws.receive(encodeWindow(1, 5));
    await new Promise((r) => setImmediate(r));
    expect(errored).toBe(false);
  });

  it("request/response half-close reaches stream state closed (mirrors request_response_half_close.json)", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    ws.receive(encodeData(1, Buffer.from("GET / HTTP/1.1")));
    expect(stream.getState()).toBe("open");
    ws.receive(encodeClose(1));
    expect(stream.getState()).toBe("half_closed_remote");
    stream.end(Buffer.from("HTTP/1.1 200 OK"));
    await new Promise((r) => setImmediate(r));
    expect(stream.getState()).toBe("closed");
  });
});

describe("MixerConn writer: control priority + round robin", () => {
  it("drains the control queue before any DATA chunk", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    ws.sent.length = 0;
    stream.write(Buffer.from("data"));
    conn.sendApp({ x: 1 });
    await new Promise((r) => setTimeout(r, 10));
    // control (app on stream 0) must be written before the DATA chunk queued after it,
    // even though the DATA write was enqueued first.
    const appIndex = ws.sent.findIndex((f) => decodeFrame(f).streamId === 0);
    const dataIndex = ws.sent.findIndex((f) => decodeFrame(f).streamId === 1);
    expect(appIndex).toBeLessThan(dataIndex);
  });

  it("round-robins one chunk per ready stream instead of draining one stream first", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    const streams: MixerStream[] = [];
    conn.on("stream", (s) => streams.push(s));
    ws.receive(encodeOpen(1));
    ws.receive(encodeOpen(3));
    await new Promise((r) => setImmediate(r));
    ws.sent.length = 0;

    const big = Buffer.alloc(16384 * 2, "a"); // two full chunks per stream
    streams[0]!.write(big);
    streams[1]!.write(big);
    await new Promise((r) => setTimeout(r, 20));

    const order = ws.sent.filter((f) => decodeFrame(f).streamId !== 0).map((f) => decodeFrame(f).streamId);
    // Expect alternation (1,3,1,3), not (1,1,3,3): round robin, not FIFO drain.
    expect(order).toEqual([1, 3, 1, 3]);
  });
});

describe("MixerConn ping watermark (item 3)", () => {
  // Mirrors ws-mixer-go/wsmixer/dispatch.go's handlePong: nextPingID/lowestUnacked
  // watermark instead of an unbounded "seen" set.

  it("pong for an id that was never sent (>= nextPingID) is PROTOCOL_ERROR fatal", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    // No ping has ever been sent by this conn (keepalive timer hasn't fired
    // yet), so pong id 0 is "never sent" -> nextPingID is still 0.
    ws.receive(encodeData(0, encodeControl({ t: "pong", id: 0 } as never)));
    const close = await closeP;
    expect(close.wsCode).toBe(4001); // PROTOCOL_ERROR
  });

  it("a duplicate pong (already acked) is tolerated, not an error", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { window: 262144 });
    let errored = false;
    conn.on("error", () => (errored = true));
    conn.on("close", () => (errored = true));

    // Drive one ping/pong round trip so id 0 becomes "sent, then acked".
    (conn as unknown as { sendPing: () => void }).sendPing();
    await new Promise((r) => setImmediate(r));
    const pingFrame = ws.sent.find((f) => {
      const frame = decodeFrame(f);
      if (frame.streamId !== 0) return false;
      const msg = JSON.parse(Buffer.from(frame.payload).toString()) as { t: string };
      return msg.t === "ping";
    });
    expect(pingFrame).toBeDefined();
    ws.receive(encodeData(0, encodeControl({ t: "pong", id: 0 } as never)));
    await new Promise((r) => setImmediate(r));
    expect(errored).toBe(false);

    // Same id again: already acked (below lowestUnacked) -> duplicate, tolerated.
    ws.receive(encodeData(0, encodeControl({ t: "pong", id: 0 } as never)));
    await new Promise((r) => setImmediate(r));
    expect(errored).toBe(false);
    expect(conn.stats().duplicatePongs).toBe(1);
  });

  it("a late pong for an id already pruned by the watchdog is tolerated, not an error", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    let errored = false;
    conn.on("error", () => (errored = true));
    conn.on("close", () => (errored = true));

    (conn as unknown as { sendPing: () => void }).sendPing(); // id 0, outstanding
    (conn as unknown as { sendPing: () => void }).sendPing(); // id 1, outstanding
    // Simulate the watchdog pruning id 0 as stale (past ping_timeout) without
    // touching lowestUnacked, then id 0's pong finally arrives late.
    (conn as unknown as { outstandingPings: Map<number, number> }).outstandingPings.delete(0);
    ws.receive(encodeData(0, encodeControl({ t: "pong", id: 0 } as never)));
    await new Promise((r) => setImmediate(r));
    expect(errored).toBe(false);
    expect(conn.stats().duplicatePongs).toBe(1);

    // The still-outstanding id 1 must still ack normally afterwards.
    const pongP = new Promise((resolve) => conn.on("pong", resolve));
    ws.receive(encodeData(0, encodeControl({ t: "pong", id: 1 } as never)));
    await pongP;
  });
});

describe("MixerConn drain", () => {
  it("emits drain and marks the connection draining", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const drainP = new Promise((resolve) => conn.on("drain", resolve));
    ws.receive(
      encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 5 } as never)),
    );
    const msg = await drainP;
    expect((msg as { reason: string }).reason).toBe("rollout");
    expect(conn.isDraining()).toBe(true);
  });

  it("OPEN above last_stream_id after drain is connection-fatal PROTOCOL_ERROR, not a stream-scoped refusal (2026-08-27 decision log)", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 8 });
    conn.on("error", () => {});
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    await streamP;
    ws.receive(encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 1 } as never)));
    await new Promise((r) => setImmediate(r));

    let sawStream3 = false;
    conn.on("stream", () => (sawStream3 = true));
    const closeP = new Promise<{ wsCode: number; errorCode?: number; message: string }>((resolve) => conn.on("close", resolve as never));
    ws.receive(encodeOpen(3));
    const close = await closeP;

    expect(close.wsCode).toBe(4001); // PROTOCOL_ERROR
    expect(close.errorCode).toBe(ErrorCode.PROTOCOL_ERROR);
    expect(close.message).toContain("3"); // names the offending stream id
    expect(close.message).toContain("1"); // and the drain's last_stream_id
    expect(sawStream3).toBe(false);
    expect(conn.stats().drainViolations).toBe(1);
  });
});

describe("MixerConn ordered async delivery (item 2)", () => {
  it("stream/app/drain handlers fire in wire order off one queue, even when a handler is async", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    const order: string[] = [];
    conn.on("stream", async (s: MixerStream) => {
      await new Promise((r) => setTimeout(r, 5));
      order.push(`stream:${s.id}`);
    });
    conn.on("app", (b: Record<string, unknown>) => {
      order.push(`app:${JSON.stringify(b)}`);
    });
    conn.on("drain", (d: { reason: string }) => {
      order.push(`drain:${d.reason}`);
    });

    ws.receive(encodeOpen(1));
    ws.receive(encodeData(0, encodeControl({ t: "app", body: { x: 1 } } as never)));
    ws.receive(encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 1 } as never)));
    await new Promise((r) => setTimeout(r, 30));

    expect(order).toEqual(["stream:1", 'app:{"x":1}', "drain:rollout"]);
  });

  it("a slow 'stream' handler does not stall frame parsing: DATA on another stream is still buffered", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => (release = resolve));
    conn.on("stream", async (s: MixerStream) => {
      if (s.id === 1) await gate; // wedges the delivery loop until released
    });

    ws.receive(encodeOpen(1));
    ws.receive(encodeOpen(3));
    ws.receive(encodeData(3, Buffer.from("hello")));
    await new Promise((r) => setImmediate(r));

    // stream 3's own 'stream' event is stuck behind stream 1's in the
    // delivery queue, but the frame dispatch/credit accounting for its DATA
    // already happened synchronously off the read path.
    const stream3 = (conn as unknown as { streams: Map<number, MixerStream> }).streams.get(3);
    expect(stream3, "stream 3 must already exist even though its event hasn't been delivered").toBeDefined();
    expect(stream3!.getRecvWindow()).toBeLessThan(262144);

    release();
    await new Promise((r) => setTimeout(r, 10));
  });

  it("overflowing the bounded delivery queue fails the connection with ENHANCE_YOUR_CALM", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 64 }); // capacity = max(128, 64+64) = 128
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    conn.on("stream", () => new Promise(() => {})); // never resolves: wedges the loop on the very first event

    for (let id = 1; id <= 127; id += 2) ws.receive(encodeOpen(id)); // 64 OPENs, fills to maxStreams
    // 80 app messages: comfortably past the 128-item capacity even accounting
    // for the delivery loop having already dequeued its first (wedged) item.
    for (let i = 0; i < 80; i++) ws.receive(encodeData(0, encodeControl({ t: "app", body: { i } } as never)));

    const close = await closeP;
    expect(close.wsCode).toBe(4009); // ENHANCE_YOUR_CALM
  });

  it("conn.once('stream') fires once, is awaited in order, and detaches", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    const order: string[] = [];
    conn.once("stream", async (s: MixerStream) => {
      await new Promise((r) => setTimeout(r, 5));
      order.push(`once:${s.id}`);
    });
    conn.on("stream", (s: MixerStream) => order.push(`on:${s.id}`));

    ws.receive(encodeOpen(1));
    ws.receive(encodeOpen(3));
    await new Promise((r) => setTimeout(r, 30));

    expect(order).toEqual(["once:1", "on:1", "on:3"]);
    expect(conn.listenerCount("stream")).toBe(1);
  });

  it("events.once(conn, 'stream') resolves with the first stream and leaves no listener behind", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    const errorListeners = conn.listenerCount("error");
    const first = once(conn, "stream") as Promise<[MixerStream]>;
    expect(conn.listenerCount("stream")).toBe(1);

    ws.receive(encodeOpen(1));
    const [stream] = await first;
    expect(stream.id).toBe(1);
    expect(conn.listenerCount("stream")).toBe(0);
    expect(conn.listenerCount("error")).toBe(errorListeners);

    ws.receive(encodeOpen(3));
    await new Promise((r) => setImmediate(r));
    expect(conn.listenerCount("stream")).toBe(0);
  });
});

describe("MixerConn stream-0 flood limit (item 4)", () => {
  it("more than burst (100) stream-0 messages fails the connection with ENHANCE_YOUR_CALM", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    for (let i = 0; i < 101; i++) {
      ws.receive(encodeData(0, encodeControl({ t: "app", body: { i } } as never)));
    }
    const close = await closeP;
    expect(close.wsCode).toBe(4009);
  });
});

describe("MixerConn minor conformance (item 6)", () => {
  it("ping received before welcome completes is PROTOCOL_ERROR", async () => {
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: agent() });
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    conn.handshake().catch(() => {});
    await new Promise((r) => setImmediate(r));
    ws.receive(encodeData(0, encodeControl({ t: "ping", id: 1 } as never)));
    const close = await closeP;
    expect(close.wsCode).toBe(4001);
  });

  it("pong jumps ahead of an already-queued control message", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    ws.sent.length = 0;
    conn.sendApp({ x: 1 });
    ws.receive(encodeData(0, encodeControl({ t: "ping", id: 5, ts: 1 } as never)));
    await new Promise((r) => setTimeout(r, 10));
    const first = JSON.parse(Buffer.from(decodeFrame(ws.sent[0]!).payload).toString()) as { t: string };
    expect(first.t).toBe("pong");
  });

  it("drain received before welcome completes is PROTOCOL_ERROR (item 1)", async () => {
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: agent() });
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number }>((resolve) => conn.on("close", resolve as never));
    conn.handshake().catch(() => {});
    await new Promise((r) => setImmediate(r));
    ws.receive(encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 0 } as never)));
    const close = await closeP;
    expect(close.wsCode).toBe(4001);
  });

  it("error{} received before welcome completes surfaces with the peer's own code, not a local PROTOCOL_ERROR (auth_failure.json shape)", async () => {
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: agent() });
    conn.on("error", () => {});
    conn.on("fatal", () => {});
    const closeP = new Promise<{ wsCode: number; errorCode?: number; message: string }>((resolve) =>
      conn.on("close", resolve as never),
    );
    conn.handshake().catch(() => {});
    await new Promise((r) => setImmediate(r));
    ws.receive(
      encodeData(
        0,
        encodeControl({ t: "error", code: ErrorCode.UNAUTHORIZED, message: "token invalid: signature verification failed" } as never),
      ),
    );
    const close = await closeP;
    // WIRE.md section 2.7/2.10 step 12: the server MAY reject `hello` with
    // error{code,message}+close before ever sending `welcome`
    // (spec/fixtures/sequences/auth_failure.json) -- this is not a protocol
    // violation on the peer's part, so it must surface with the peer's own
    // code/message, not a masking local PROTOCOL_ERROR/4001.
    expect(close.wsCode).toBe(4000 + ErrorCode.UNAUTHORIZED);
    expect(close.errorCode).toBe(ErrorCode.UNAUTHORIZED);
    expect(close.message).toBe("token invalid: signature verification failed");
    // WIRE.md section 2.7: a peer that receives `error` MUST NOT reply with
    // its own.
    const errorReplies = ws.sent.filter((f) => {
      let frame;
      try {
        frame = decodeFrame(f);
      } catch {
        return false;
      }
      if (frame.streamId !== 0 || frame.type !== FrameType.DATA) return false;
      try {
        return (JSON.parse(Buffer.from(frame.payload).toString()) as { t: string }).t === "error";
      } catch {
        return false;
      }
    });
    expect(errorReplies).toHaveLength(0);
  });
});

describe("MixerConn.sendApp() write-completion", () => {
  it("resolves only once ws.send's callback confirms the frame was written, not on enqueue", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    ws.sent.length = 0;
    let resolved = false;
    const p = conn.sendApp({ x: 1 }).then(() => {
      resolved = true;
    });
    // The writer picks the frame off the queue and calls ws.send() on the
    // next microtask, but FakeWS.send's callback only fires on a *further*
    // microtask -- so right after that first tick, the frame has been
    // written but sendApp() must not have resolved yet.
    await Promise.resolve();
    expect(ws.sent.length).toBe(1);
    expect(resolved).toBe(false);
    await p;
    expect(resolved).toBe(true);
  });

  it("rejects with the connection's terminal error if the conn fails before the write completes", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    ws.sent.length = 0;
    ws.failNextSend(new Error("simulated socket failure"));
    await expect(conn.sendApp({ x: 1 })).rejects.toThrow();
  });

  it("rejects a still-queued sendApp() if the connection fails before it's written at all", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    ws.sent.length = 0;
    // Queue two control frames back-to-back; failing the first write aborts
    // the writer loop (runWriter's control-loop `return` on failure), and
    // (as a real failing ws.send would) the socket then closes -- which is
    // what must reject the second, still-queued item instead of hanging it
    // forever (rejectOutstanding now sweeps the control queue too).
    ws.failNextSend(new Error("simulated socket failure"));
    // Caught immediately (as an ordinary error value) so the intervening
    // await below doesn't leave either promise "unhandled" in the meantime.
    const first = conn.sendApp({ x: 1 }).catch((e: Error) => e);
    const second = conn.sendApp({ x: 2 }).catch((e: Error) => e);
    await new Promise((r) => setImmediate(r));
    ws.emit("close", 1006, Buffer.from("simulated socket failure"));
    expect(await first).toBeInstanceOf(Error);
    expect(await second).toBeInstanceOf(Error);
  });
});

describe("MixerConn ordered async delivery: handler errors (blocker 2)", () => {
  it("a throwing 'stream' handler does not stop delivery of a later event; no unhandled rejection", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    const delivered: number[] = [];
    const handlerErrors: Array<{ event: string; error: Error }> = [];
    conn.on("handlerError", (info) => handlerErrors.push(info));
    conn.on("stream", (s: MixerStream) => {
      if (s.id === 1) throw new Error("boom (sync)");
      delivered.push(s.id);
    });

    ws.receive(encodeOpen(1));
    ws.receive(encodeOpen(3));
    await new Promise((r) => setTimeout(r, 10));

    expect(delivered).toEqual([3]);
    expect(handlerErrors).toHaveLength(1);
    expect(handlerErrors[0]!.error.message).toBe("boom (sync)");
    expect(conn.stats().handlerErrors).toBe(1);
  });

  it("a rejecting async 'stream' handler is caught, not an unhandled rejection", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    const delivered: number[] = [];
    conn.on("stream", async (s: MixerStream) => {
      if (s.id === 1) {
        await new Promise((r) => setTimeout(r, 1));
        throw new Error("boom (async)");
      }
      delivered.push(s.id);
    });

    ws.receive(encodeOpen(1));
    ws.receive(encodeOpen(3));
    await new Promise((r) => setTimeout(r, 20));

    expect(delivered).toEqual([3]);
    expect(conn.stats().handlerErrors).toBe(1);
  });

  it("a throwing once('stream') handler is caught the same way, and still detaches", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    const delivered: number[] = [];
    const handlerErrors: Array<{ event: string; error: Error }> = [];
    conn.on("handlerError", (info) => handlerErrors.push(info));
    let calls = 0;
    conn.once("stream", () => {
      calls++;
      throw new Error("boom (once)");
    });
    conn.on("stream", (s: MixerStream) => delivered.push(s.id));

    ws.receive(encodeOpen(1));
    ws.receive(encodeOpen(3));
    await new Promise((r) => setTimeout(r, 10));

    expect(calls).toBe(1);
    expect(delivered).toEqual([1, 3]);
    expect(handlerErrors).toHaveLength(1);
    expect(handlerErrors[0]!.error.message).toBe("boom (once)");
    expect(conn.stats().handlerErrors).toBe(1);
    expect(conn.listenerCount("stream")).toBe(1);
  });
});

describe("MixerConn ordered async delivery: flush after close (R1)", () => {
  it("app A (handler pending) + app B queued + error{} mid-flight -> both delivered, in order, B after close; a further enqueue after close is refused", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const order: string[] = [];
    let closed = false;
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => (release = resolve));
    conn.on("close", () => {
      closed = true;
    });
    conn.on("error", () => {}); // guard: error{} -> handlePeerError -> teardownConn's guarded 'error' emit
    conn.on("app", async (b: Record<string, unknown>) => {
      const i = b.i as number;
      order.push(`start:${i}`);
      if (i === 0) await gate; // A suspends here, still pending when error{} arrives
      order.push(`end:${i}`);
    });

    ws.receive(encodeData(0, encodeControl({ t: "app", body: { i: 0 } } as never))); // A: wedges the loop on gate
    ws.receive(encodeData(0, encodeControl({ t: "app", body: { i: 1 } } as never))); // B: queued behind A
    ws.receive(encodeData(0, encodeControl({ t: "error", code: ErrorCode.INTERNAL_ERROR, message: "boom" } as never))); // ends the connection while A is still in flight
    // A further event arriving strictly after teardown must never be
    // enqueued at all (enqueueDelivery refuses once this.closed) -- it
    // should never show up in `order`, even after the flush below runs.
    ws.receive(encodeData(0, encodeControl({ t: "app", body: { i: 99 } } as never)));

    await new Promise((r) => setImmediate(r));
    expect(closed).toBe(true); // the connection already tore down...
    expect(order).toEqual(["start:0"]); // ...but B (queued before the close) has NOT been dropped -- it's still owed

    release();
    await new Promise((r) => setTimeout(r, 10));

    expect(order).toEqual(["start:0", "end:0", "start:1", "end:1"]); // B delivered after close, in wire order; i:99 never appears
  });

  it("a queued stream OPEN is still delivered after close, and the stream it hands over is already destroyed with an error (no read/write ever hangs)", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    const delivered: MixerStream[] = [];
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => (release = resolve));
    conn.on("app", async () => {
      await gate; // wedges the loop on the app event queued ahead of the stream OPEN
    });
    conn.on("stream", (s: MixerStream) => {
      delivered.push(s);
    });
    conn.on("error", () => {});

    ws.receive(encodeData(0, encodeControl({ t: "app", body: {} } as never))); // wedges the loop
    ws.receive(encodeOpen(1)); // OPEN: registered in MixerConn's stream table synchronously, its 'stream' event queued behind the wedge
    ws.receive(encodeData(0, encodeControl({ t: "error", code: ErrorCode.INTERNAL_ERROR, message: "boom" } as never))); // ends the connection -- rejectOutstanding tears stream 1 down NOW, before its 'stream' event is ever delivered

    await new Promise((r) => setImmediate(r));
    expect(delivered).toHaveLength(0); // the stream event is still queued, not dropped

    release();
    await new Promise((r) => setTimeout(r, 10));

    expect(delivered).toHaveLength(1);
    const stream = delivered[0]!;
    // Handed to the app already dead: destroyed, with the connection's error
    // -- never a clean end, and no read/write on it can ever hang.
    expect(stream.destroyed).toBe(true);
    expect(stream.errored).toBeTruthy();
  });

  it("a handler that throws mid-flush does not stop the rest of the backlog from being delivered, and produces no unhandled rejection", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const order: string[] = [];
    const handlerErrors: Array<{ event: string; error: Error }> = [];
    conn.on("handlerError", (info) => handlerErrors.push(info));
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => (release = resolve));
    conn.on("app", async (b: Record<string, unknown>) => {
      const i = b.i as number;
      if (i === 0) {
        await gate;
        throw new Error("boom mid-flush");
      }
      order.push(`app:${i}`);
    });
    conn.on("error", () => {});

    const unhandled: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      ws.receive(encodeData(0, encodeControl({ t: "app", body: { i: 0 } } as never)));
      ws.receive(encodeData(0, encodeControl({ t: "app", body: { i: 1 } } as never)));
      ws.receive(encodeData(0, encodeControl({ t: "error", code: ErrorCode.INTERNAL_ERROR, message: "boom" } as never)));
      await new Promise((r) => setImmediate(r));

      release();
      await new Promise((r) => setTimeout(r, 10));

      expect(order).toEqual(["app:1"]); // event 1 still delivered despite event 0's handler throwing
      expect(handlerErrors).toHaveLength(1);
      expect(handlerErrors[0]!.error.message).toBe("boom mid-flush");
      expect(unhandled).toHaveLength(0);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });
});

describe("MixerConn abnormal connection death errors open streams, never re-touches an already-closed one (R2)", () => {
  it("a stream that already fully, cleanly closed both directions before the connection later dies abnormally keeps its clean end -- retired, only destroyed by the teardown", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws, { maxStreams: 4 });
    let stream: MixerStream | undefined;
    let sawError = false;
    conn.on("stream", (s: MixerStream) => {
      stream = s;
      s.on("error", () => {
        sawError = true;
      });
    });
    ws.receive(encodeOpen(1));
    await new Promise((r) => setImmediate(r));
    expect(stream).toBeDefined();

    // Both directions close cleanly: the peer's CLOSE, then this side's own.
    ws.receive(encodeClose(1));
    stream!.end(); // -> closeWrite(): CLOSE sent, state "closed", retired
    await new Promise((r) => setImmediate(r));
    expect(stream!.getState()).toBe("closed");
    expect(stream!.errored).toBeFalsy(); // ended cleanly, not with an error

    // Now the WHOLE connection dies abnormally (1006, no close frame).
    ws.emit("close", 1006, Buffer.from(""));
    await new Promise((r) => setImmediate(r));

    // Retired, but not yet destroyed (nothing read it to 'end'): the teardown
    // still reaches it, only to destroy it -- its clean end stands, no error.
    expect(sawError).toBe(false);
    expect(stream!.errored).toBeFalsy();
    expect(stream!.destroyed).toBe(true);
  });

  it("a stream CLOSE'd both ways with unread data outlives the connection for its reader; a write issued after the teardown still fails STREAM_CLOSED, once 'end' has fired", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    stream.on("error", () => {});
    ws.receive(encodeData(1, Buffer.from("hello")));
    ws.receive(encodeClose(1));
    stream.closeWrite();
    ws.emit("close", 1006, Buffer.from(""));
    expect(stream.destroyed).toBe(false);

    const events: string[] = [];
    stream.write(Buffer.from("late"), (e) => events.push(`write-cb:${e ? (e as StreamError).codeName : "ok"}`));
    await new Promise((r) => setImmediate(r));
    expect(events).toEqual([]);

    const received: Buffer[] = [];
    stream.on("data", (c: Buffer) => received.push(c));
    stream.on("end", () => events.push("end"));
    const closed = new Promise<void>((resolve) => stream.once("close", () => resolve()));
    await closed;
    expect(Buffer.concat(received).toString()).toBe("hello");
    expect([...events].sort()).toEqual(["end", "write-cb:STREAM_CLOSED"]);
    expect(stream.resetCode).toBeUndefined();
  });
});

describe("MixerConn retired streams are tracked weakly", () => {
  type Retired = Map<number, MixerStream | WeakRef<MixerStream>>;
  const retiredOf = (conn: MixerConn) => (conn as unknown as { retired: Retired }).retired;
  const dead = Object.assign(Object.create(WeakRef.prototype) as WeakRef<MixerStream>, { deref: () => undefined });
  const alive = (entry: MixerStream | WeakRef<MixerStream>) => (entry instanceof WeakRef ? entry.deref() : entry);

  /** One permanent 'stream' listener handing each stream to whoever waits for it. */
  function nextStream(conn: MixerConn): () => Promise<MixerStream> {
    let waiting: ((s: MixerStream) => void) | null = null;
    conn.on("stream", (s: MixerStream) => {
      const resolve = waiting;
      waiting = null;
      resolve?.(s);
    });
    return () =>
      new Promise<MixerStream>((resolve) => {
        waiting = resolve;
      });
  }

  /** Peer CLOSE with an empty body, app end()s, nobody ever reads: retired, never 'end', never destroyed. */
  async function retireUnread(ws: FakeWS, next: () => Promise<MixerStream>, id: number): Promise<void> {
    const streamP = next();
    ws.receive(encodeOpen(id));
    const stream = await streamP;
    stream.on("error", () => {});
    ws.receive(encodeClose(id));
    stream.end(Buffer.from("resp"));
    await new Promise((r) => setImmediate(r));
  }

  it("holds each never-read retired stream by WeakRef, and prunes dead refs on insert, on 'close' and at teardown", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const streams: MixerStream[] = [];
    conn.on("stream", (s: MixerStream) => streams.push(s));
    const next = nextStream(conn);
    for (const id of [1, 3, 5]) await retireUnread(ws, next, id);
    const retired = retiredOf(conn);
    expect([...retired.keys()]).toEqual([1, 3, 5]);
    for (const [i, ref] of [...retired.values()].entries()) {
      expect(ref).toBeInstanceOf(WeakRef);
      expect(alive(ref)).toBe(streams[i]);
      expect(streams[i]!.destroyed).toBe(false);
    }

    retired.set(1001, dead);
    await retireUnread(ws, next, 7);
    expect([...retired.keys()]).toEqual([1, 3, 5, 7]);

    retired.set(1003, dead);
    const closed = new Promise<void>((resolve) => streams[0]!.once("close", () => resolve()));
    streams[0]!.resume();
    await closed;
    expect([...retired.keys()]).toEqual([3, 5, 7]);

    retired.set(1005, dead);
    ws.emit("close", 1006, Buffer.from(""));
    await new Promise((r) => setImmediate(r));
    expect(retired.size).toBe(0);
    expect(streams.every((s) => s.destroyed)).toBe(true);
  });

  it("holds a retired stream strongly while it owes a write callback: held before retirement, or held after it (upgraded from its WeakRef)", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const next = nextStream(conn);
    const retired = retiredOf(conn);

    // No held callback: weak.
    await retireUnread(ws, next, 1);
    expect(retired.get(1)).toBeInstanceOf(WeakRef);

    // Held before retirement (peer CLOSE, the socket send fails, then closeWrite()): strong from the start.
    const heldBeforeP = next();
    ws.receive(encodeOpen(3));
    const heldBefore = await heldBeforeP;
    heldBefore.on("error", () => {});
    ws.receive(encodeClose(3));
    ws.failNextSend(new Error("simulated socket failure"));
    const beforeErrs: Array<Error | null | undefined> = [];
    heldBefore.write(Buffer.from("x"), (e) => beforeErrs.push(e));
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    heldBefore.closeWrite();
    expect(heldBefore.getState()).toBe("closed");
    expect(retired.get(3)).toBe(heldBefore);

    // Held after retirement (a write after closeWrite()): the WeakRef is upgraded.
    const heldAfterP = next();
    ws.receive(encodeOpen(5));
    const heldAfter = await heldAfterP;
    heldAfter.on("error", () => {});
    ws.receive(encodeClose(5));
    heldAfter.closeWrite();
    expect(retired.get(5)).toBeInstanceOf(WeakRef);
    const afterErrs: Array<Error | null | undefined> = [];
    heldAfter.write(Buffer.from("late"), (e) => afterErrs.push(e));
    await new Promise((r) => setImmediate(r));
    expect(retired.get(5)).toBe(heldAfter);
    expect(retired.get(1)).toBeInstanceOf(WeakRef);
    expect(beforeErrs).toHaveLength(0);
    expect(afterErrs).toHaveLength(0);

    ws.emit("close", 1006, Buffer.from(""));
    await new Promise((r) => setImmediate(r));
    expect(retired.size).toBe(0);
    expect(beforeErrs).toHaveLength(1);
    expect(beforeErrs[0]).toBeInstanceOf(ConnError);
    expect(afterErrs).toHaveLength(1);
    expect((afterErrs[0] as StreamError).codeName).toBe("STREAM_CLOSED");
  });

  it.skipIf(typeof globalThis.gc !== "function")(
    "a retired stream the app dropped is collected; one it still holds is still torn down by the connection's death (needs --expose-gc)",
    async () => {
      const ws = new FakeWS();
      const { conn } = await handshaken(ws);
      conn.on("error", () => {});
      const next = nextStream(conn);
      for (let id = 1; id < 41; id += 2) await retireUnread(ws, next, id);

      // Kept: peer CLOSE, a write whose socket send fails, closeWrite() -> retired with the callback held until 'end', which never comes unread.
      const keptP = next();
      ws.receive(encodeOpen(41));
      const kept = await keptP;
      kept.on("error", () => {});
      ws.receive(encodeClose(41));
      ws.failNextSend(new Error("simulated socket failure"));
      const cbErrs: Array<Error | null | undefined> = [];
      kept.write(Buffer.from("x"), (e) => cbErrs.push(e));
      await new Promise((r) => setImmediate(r));
      await new Promise((r) => setImmediate(r));
      kept.closeWrite();
      await new Promise((r) => setImmediate(r));
      expect(kept.getState()).toBe("closed");
      expect(cbErrs).toHaveLength(0);

      const retired = retiredOf(conn);
      expect(retired.size).toBe(21);
      for (let i = 0; i < 5; i++) {
        await new Promise((r) => setImmediate(r));
        globalThis.gc!();
      }
      expect([...retired].filter(([, entry]) => alive(entry) !== undefined).map(([id]) => id)).toEqual([41]);

      const closed = new Promise<void>((resolve) => kept.once("close", () => resolve()));
      ws.emit("close", 1006, Buffer.from(""));
      await closed;
      expect(retired.size).toBe(0);
      expect(cbErrs).toHaveLength(1);
      expect(cbErrs[0]).toBeInstanceOf(ConnError);
      expect(kept.destroyed).toBe(true);
    },
  );

  it.skipIf(typeof globalThis.gc !== "function")(
    "a retired stream the app dropped while it owes a write callback is not collected: the connection's death still settles the callback and fires 'close' (needs --expose-gc)",
    async () => {
      const ws = new FakeWS();
      const { conn } = await handshaken(ws);
      conn.on("error", () => {});
      const next = nextStream(conn);
      const log: string[] = [];

      // All stream refs live only inside this function's frame, gone once it returns.
      await (async () => {
        // Control: dropped with nothing owed -- must be collected, proving the drop is real.
        await retireUnread(ws, next, 1);

        // Held before retirement: peer CLOSE, the socket send fails, then closeWrite().
        const beforeP = next();
        ws.receive(encodeOpen(3));
        const before = await beforeP;
        before.on("error", () => {});
        before.on("close", () => log.push("close:3"));
        ws.receive(encodeClose(3));
        ws.failNextSend(new Error("simulated socket failure"));
        before.write(Buffer.from("x"), (e) => log.push(`cb:3:${e instanceof ConnError ? "ConnError" : String(e)}`));
        await new Promise((r) => setImmediate(r));
        await new Promise((r) => setImmediate(r));
        before.closeWrite();

        // Held after retirement: a write after closeWrite().
        const afterP = next();
        ws.receive(encodeOpen(5));
        const after = await afterP;
        after.on("error", () => {});
        after.on("close", () => log.push("close:5"));
        ws.receive(encodeClose(5));
        after.closeWrite();
        after.write(Buffer.from("late"), (e) => log.push(`cb:5:${(e as StreamError | undefined)?.codeName ?? String(e)}`));
        await new Promise((r) => setImmediate(r));
      })();

      const retired = retiredOf(conn);
      expect([...retired.keys()]).toEqual([1, 3, 5]);
      for (let i = 0; i < 5; i++) {
        await new Promise((r) => setImmediate(r));
        globalThis.gc!();
      }
      expect(alive(retired.get(1)!)).toBeUndefined();
      for (const id of [3, 5]) {
        expect(alive(retired.get(id)!)?.id).toBe(id);
        expect(retired.get(id)).not.toBeInstanceOf(WeakRef);
      }
      expect(log).toEqual([]);

      ws.emit("close", 1006, Buffer.from(""));
      await new Promise((r) => setTimeout(r, 20));
      expect(retired.size).toBe(0);
      expect([...log].sort()).toEqual(["cb:3:ConnError", "cb:5:STREAM_CLOSED", "close:3", "close:5"]);
    },
  );
});

describe("MixerConn peer error{} handling (item 4)", () => {
  it("records the error and closes with 4000+code immediately, without waiting for the peer", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number; errorCode?: number }>((resolve) => conn.on("close", resolve as never));
    ws.receive(encodeData(0, encodeControl({ t: "error", code: ErrorCode.FLOW_CONTROL_ERROR, message: "stream 41: over credit" } as never)));
    const close = await closeP;
    expect(close.wsCode).toBe(4000 + ErrorCode.FLOW_CONTROL_ERROR);
    expect(close.errorCode).toBe(ErrorCode.FLOW_CONTROL_ERROR);
    // Must not reply with its own error{} (WIRE.md section 2.7): no
    // stream-0 DATA frame carrying {"t":"error"} was ever sent back.
    const sentErrorReplies = ws.sent.filter((f) => {
      let frame;
      try {
        frame = decodeFrame(f);
      } catch {
        return false;
      }
      if (frame.streamId !== 0 || frame.type !== FrameType.DATA) return false;
      try {
        return (JSON.parse(Buffer.from(frame.payload).toString()) as { t: string }).t === "error";
      } catch {
        return false;
      }
    });
    expect(sentErrorReplies).toHaveLength(0);
  });
});

describe("MixerConn unknown drain.reason (item 5)", () => {
  it("normalizes an unknown reason to maintenance and counts it", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const drainP = new Promise<{ reason: string }>((resolve) => conn.on("drain", resolve as never));
    ws.receive(encodeData(0, encodeControl({ t: "drain", reason: "some_future_reason", last_stream_id: 0 } as never)));
    const msg = await drainP;
    expect(msg.reason).toBe("maintenance");
    expect(conn.stats().unknownDrainReasons).toBe(1);
  });

  it("leaves a known reason untouched and does not count it", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const drainP = new Promise<{ reason: string }>((resolve) => conn.on("drain", resolve as never));
    ws.receive(encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: 0 } as never)));
    const msg = await drainP;
    expect(msg.reason).toBe("rollout");
    expect(conn.stats().unknownDrainReasons).toBe(0);
  });
});

describe("MixerConn stats(): protocolViolations, bytesIn, bytesOut (item 9)", () => {
  it("counts a ConnError-triggered failure as a protocol violation", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<void>((resolve) => conn.on("close", () => resolve()));
    ws.receive(encodeData(3, new Uint8Array([1, 2, 3]))); // never-opened stream: PROTOCOL_ERROR
    await closeP;
    expect(conn.stats().protocolViolations).toBe(1);
  });

  it("tracks cumulative bytesIn/bytesOut", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const before = conn.stats();
    expect(before.bytesIn).toBeGreaterThan(0); // welcome was already received during handshake
    expect(before.bytesOut).toBeGreaterThan(0); // hello was already sent during handshake
    ws.sent.length = 0;
    await conn.sendApp({ x: 1 });
    const after = conn.stats();
    expect(after.bytesOut - before.bytesOut).toBe(ws.sent[0]!.length);
  });
});

describe("MixerConn.handleDispatchError (item 10)", () => {
  it("a StreamError for a stream with no live entry still sends RESET (mirrors Go's resetStream)", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    ws.sent.length = 0;
    (conn as unknown as { handleDispatchError: (e: unknown) => void }).handleDispatchError(
      new StreamError(ErrorCode.STREAM_CLOSED, 99, "gone"),
    );
    await new Promise((r) => setImmediate(r));
    const resetFrame = ws.sent.find((f) => {
      const frame = decodeFrame(f);
      return frame.type === FrameType.RESET && frame.streamId === 99;
    });
    expect(resetFrame, "expected a RESET(99) even with no live MixerStream").toBeDefined();
    expect(decodeFrame(resetFrame!).payload[3]).toBe(ErrorCode.STREAM_CLOSED);
  });

  it("a StreamError for a still-live stream uses abort(): RESET sent, resetCode set, and 'reset' emitted (item 3)", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    ws.receive(encodeClose(1)); // half_closed_remote; the id stays live
    await new Promise((r) => setImmediate(r));
    const resetP = new Promise<{ code: number; message: string }>((resolve) => stream.on("reset", resolve));
    ws.sent.length = 0;

    // DATA after the peer's own CLOSE: an SDK-detected StreamError(STREAM_CLOSED),
    // not application code -- must go through abort(), not the app-facing reset().
    ws.receive(encodeData(1, new Uint8Array([1])));
    const info = await resetP;

    expect(info.code).toBe(ErrorCode.STREAM_CLOSED);
    expect(stream.resetCode).toBe(ErrorCode.STREAM_CLOSED);
    expect(stream.getState()).toBe("closed");
    const resetFrame = ws.sent.find((f) => decodeFrame(f).type === FrameType.RESET && decodeFrame(f).streamId === 1);
    expect(resetFrame, "expected the connection to have sent RESET(1)").toBeDefined();
    expect(decodeFrame(resetFrame!).payload[3]).toBe(ErrorCode.STREAM_CLOSED);
  });
});
describe("MixerConn wire close-code clamping (item B)", () => {
  const HUGE_CODE = 0x10000001; // >= 0x1000_0000: a legal application/RESET code, never a legal WS close code

  it("peer error{code:0x10000001} in connected phase: the WS close frame is clamped to 4002, not a throw/terminate(1006)", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number; errorCode?: number; message: string }>((resolve) =>
      conn.on("close", resolve as never),
    );
    ws.receive(encodeData(0, encodeControl({ t: "error", code: HUGE_CODE, message: "layer-above reason" } as never)));
    const close = await closeP;
    // Reported wsCode/errorCode stay the real, unclamped semantic values --
    // only the bytes actually sent on the wire are clamped.
    expect(close.wsCode).toBe(4000 + HUGE_CODE);
    expect(close.errorCode).toBe(HUGE_CODE);
    expect(close.message).toBe("layer-above reason");
    expect(ws.closedWith).not.toBeNull();
    expect(ws.closedWith!.code).toBe(4000 + ErrorCode.INTERNAL_ERROR);
  });

  it("peer error{code:0x10000001} before welcome: same clamped wire close, no throw", async () => {
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: agent() });
    conn.on("error", () => {});
    conn.on("fatal", () => {});
    const closeP = new Promise<{ wsCode: number; errorCode?: number }>((resolve) => conn.on("close", resolve as never));
    conn.handshake().catch(() => {});
    await new Promise((r) => setImmediate(r));
    ws.receive(encodeData(0, encodeControl({ t: "error", code: HUGE_CODE, message: "layer-above reason" } as never)));
    const close = await closeP;
    expect(close.wsCode).toBe(4000 + HUGE_CODE);
    expect(close.errorCode).toBe(HUGE_CODE);
    expect(ws.closedWith).not.toBeNull();
    expect(ws.closedWith!.code).toBe(4000 + ErrorCode.INTERNAL_ERROR);
  });

  it("MixerConn.fail() with such a code likewise clamps the wire close", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const closeP = new Promise<{ wsCode: number; errorCode?: number }>((resolve) => conn.on("close", resolve as never));
    conn.fail(new WsMixerError(HUGE_CODE, "locally detected"));
    const close = await closeP;
    expect(close.wsCode).toBe(4000 + HUGE_CODE);
    expect(close.errorCode).toBe(HUGE_CODE);
    expect(ws.closedWith).not.toBeNull();
    expect(ws.closedWith!.code).toBe(4000 + ErrorCode.INTERNAL_ERROR);
  });
});

describe("MixerConn incoming RESET message length", () => {
  it("an over-long RESET message (a sender-side SHOULD, WIRE.md section 2.3) is clamped to 256 UTF-8 bytes on a character boundary, not a protocol error", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    let connClosed = false;
    conn.on("close", () => (connClosed = true));
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    stream.on("error", () => {});
    const resetP = new Promise<{ code: number; message: string }>((resolve) => stream.on("reset", resolve));

    const long = "x" + "é".repeat(200); // 401 UTF-8 bytes; byte 256 falls mid-"é"
    ws.receive(encodeReset(1, ErrorCode.CANCEL, long));
    const info = await resetP;

    expect(info.code).toBe(ErrorCode.CANCEL);
    expect(info.message).toBe("x" + "é".repeat(127));
    expect(Buffer.byteLength(info.message)).toBe(255);
    expect((stream.errored as StreamError).message).toBe(info.message);
    await new Promise((r) => setImmediate(r));
    expect(connClosed).toBe(false);
    expect(conn.stats().protocolViolations).toBe(0);
  });
});

describe("MixerConn stream teardown: queued DATA and error classes", () => {
  it("DATA still queued in the outbox when its stream is retired by a peer RESET is rejected with that RESET's error -- never dropped (a hung write callback) and never written", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    stream.on("error", () => {});

    // Stall the writer on a control frame so the stream's chunk stays in the outbox.
    const held: Array<() => void> = [];
    const realSend = ws.send.bind(ws);
    ws.send = (data: Uint8Array, cb?: (err?: Error) => void) => {
      ws.sent.push(data.slice());
      held.push(() => cb?.());
    };
    void conn.sendApp({ stall: true });
    await new Promise((r) => setImmediate(r));
    const writeErrP = new Promise<Error | null | undefined>((resolve) => stream.write(Buffer.from("queued"), (e) => resolve(e)));
    await new Promise((r) => setImmediate(r));

    ws.receive(encodeReset(1, ErrorCode.CANCEL, "peer gave up"));
    const writeErr = await writeErrP;
    expect(writeErr).toBeInstanceOf(StreamError);
    expect((writeErr as StreamError).code).toBe(ErrorCode.CANCEL);

    ws.send = realSend;
    for (const release of held) release();
    await new Promise((r) => setImmediate(r));
    const data1 = ws.sent.map((f) => decodeFrame(f)).filter((f) => f.type === FrameType.DATA && f.streamId === 1);
    expect(data1).toHaveLength(0);
  });

  it("a raw socket send failure reaches the write callback as a ConnError, with the socket's own error as `cause`", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    stream.on("error", () => {});
    const raw = new Error("WebSocket is not open: readyState 2 (CLOSING)");
    ws.failNextSend(raw);
    const writeErr = await new Promise<Error | null | undefined>((resolve) => stream.write(Buffer.from("x"), (e) => resolve(e)));
    expect(writeErr).toBeInstanceOf(ConnError);
    expect((writeErr as ConnError).code).toBe(ErrorCode.INTERNAL_ERROR);
    expect(writeErr!.message).toBe(raw.message);
    expect(writeErr!.cause).toBe(raw);
  });

  it("an abnormal 1006 close tears a live stream down with a ConnError that keeps wsCode and the socket-close error as `cause`", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;
    const errP = new Promise<Error>((resolve) => stream.on("error", resolve));
    ws.emit("close", 1006, Buffer.from(""));
    const err = await errP;
    expect(err).toBeInstanceOf(ConnError);
    expect((err as ConnError).wsCode).toBe(1006);
    expect(err.cause).toBeInstanceOf(WsMixerError);
    expect(err.cause).not.toBeInstanceOf(ConnError);
    expect(stream.errored).toBe(err);
  });

  it("drain hand-over: DATA queued behind another stream's in-flight write fails with its own stream's CANCEL, not the connection's NO_ERROR", async () => {
    const ws = new FakeWS();
    const { conn } = await handshaken(ws);
    conn.on("error", () => {});
    const streams: MixerStream[] = [];
    conn.on("stream", (s: MixerStream) => {
      s.on("error", () => {});
      streams.push(s);
    });
    ws.receive(encodeOpen(1));
    ws.receive(encodeOpen(3));
    await new Promise((r) => setImmediate(r));
    const [s1, s3] = streams as [MixerStream, MixerStream];

    // Hold every socket send: s3's chunk stays in flight, s1's queues behind it in the outbox.
    const held: Array<(err?: Error) => void> = [];
    ws.send = (data: Uint8Array, cb?: (err?: Error) => void) => {
      ws.sent.push(data.slice());
      held.push((err) => cb?.(err));
    };
    const s3ErrP = new Promise<Error | null | undefined>((resolve) => s3.write(Buffer.from("s3"), (e) => resolve(e)));
    await new Promise((r) => setImmediate(r));
    const s1ErrP = new Promise<Error | null | undefined>((resolve) => s1.write(Buffer.from("s1"), (e) => resolve(e)));
    await new Promise((r) => setImmediate(r));
    expect(held).toHaveLength(1);

    conn.fail(new WsMixerError(ErrorCode.NO_ERROR, "superseded"), (id) => new StreamError(ErrorCode.CANCEL, id, "connection drained"));
    // The old socket dies under s3's in-flight write.
    for (const release of held) release(new Error("socket closed"));

    for (const [s, errP] of [
      [s1, s1ErrP],
      [s3, s3ErrP],
    ] as const) {
      const err = await errP;
      expect(err).toBeInstanceOf(StreamError);
      expect((err as StreamError).code).toBe(ErrorCode.CANCEL);
      expect(err!.message).toBe("connection drained");
      expect(s.errored).toBeInstanceOf(StreamError);
      expect((s.errored as StreamError).code).toBe(ErrorCode.CANCEL);
      expect(s.errored!.message).toBe("connection drained");
    }
    const data1 = ws.sent.map((f) => decodeFrame(f)).filter((f) => f.type === FrameType.DATA && f.streamId === 1);
    expect(data1).toHaveLength(0);
  });
});
