/**
 * MixerStream against a recording StreamHost (no connection): state
 * machine, credit accounting, half-close/RESET semantics, the `closed`
 * promise and connection teardown (terminate).
 */
import { describe, expect, it } from "vitest";
import { MixerStream, type StreamHost } from "../src/stream.js";
import { ConnError, ErrorCode, StreamError } from "../src/errors.js";
import { FrameType, decodeFrame, resetCode, resetMessage, windowIncrement, type Frame } from "../src/frame.js";
import { toWebStreams } from "../src/webstreams.js";

class RecordingHost implements StreamHost {
  /** Every frame in the order the stream handed it over (DATA via sendData, the rest via sendControlFrame). */
  readonly frames: Array<{ type: "DATA"; streamId: number; payload: Uint8Array } | Frame> = [];
  readonly retired: number[] = [];
  /** When set, sendData waits for release() instead of resolving right away. */
  hold = false;
  private held: Array<() => void> = [];

  sendData(streamId: number, chunk: Uint8Array): Promise<void> {
    this.frames.push({ type: "DATA", streamId, payload: chunk.slice() });
    if (!this.hold) return Promise.resolve();
    return new Promise((resolve) => this.held.push(resolve));
  }
  sendControlFrame(frame: Uint8Array): void {
    this.frames.push(decodeFrame(frame));
  }
  retireStream(streamId: number): void {
    this.retired.push(streamId);
  }
  release(): void {
    this.hold = false;
    for (const r of this.held.splice(0)) r();
  }
  data(): Uint8Array[] {
    return this.frames.filter((f) => f.type === "DATA").map((f) => f.payload);
  }
  ofType(t: number): Frame[] {
    return this.frames.filter((f): f is Frame => typeof f.type === "number" && f.type === t);
  }
  kinds(): string[] {
    return this.frames.map((f) => (f.type === "DATA" ? "DATA" : (["OPEN", "DATA", "WINDOW", "CLOSE", "RESET"][f.type] ?? "?")));
  }
}

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | null) => (b === null ? null : new TextDecoder().decode(b));
const tick = () => new Promise<void>((r) => setImmediate(r));

function make(recvWindow = 16384, sendWindow = 262144) {
  const host = new RecordingHost();
  const stream = new MixerStream(1, host, recvWindow, sendWindow);
  return { host, stream };
}

describe("MixerStream state machine", () => {
  it("starts open; closeWrite() -> half_closed_local; then peer CLOSE -> closed, retired, closed=finished", async () => {
    const { host, stream } = make();
    expect(stream.getState()).toBe("open");
    await stream.closeWrite();
    expect(stream.getState()).toBe("half_closed_local");
    expect(host.kinds()).toEqual(["CLOSE"]);
    stream.handleClose();
    expect(stream.getState()).toBe("closed");
    expect(host.retired).toEqual([1]);
    await expect(stream.closed).resolves.toEqual({ kind: "finished" });
  });

  it("peer CLOSE then closeWrite() -> closed, retired once, finished", async () => {
    const { host, stream } = make();
    stream.handleClose();
    expect(stream.getState()).toBe("half_closed_remote");
    await stream.closeWrite();
    expect(stream.getState()).toBe("closed");
    expect(host.retired).toEqual([1]);
    await expect(stream.closed).resolves.toEqual({ kind: "finished" });
  });

  it("closeWrite() is idempotent: one CLOSE frame, same promise", async () => {
    const { host, stream } = make();
    const a = stream.closeWrite();
    const b = stream.closeWrite();
    expect(a).toBe(b);
    await a;
    expect(host.ofType(FrameType.CLOSE)).toHaveLength(1);
  });

  it("duplicate CLOSE from the peer is a STREAM_CLOSED stream error", () => {
    const { stream } = make();
    stream.handleClose();
    expect(() => stream.handleClose()).toThrow(StreamError);
  });

  it("DATA after the peer's CLOSE is a STREAM_CLOSED stream error", () => {
    const { stream } = make();
    stream.handleClose();
    try {
      stream.handleData(enc("x"));
      expect.unreachable();
    } catch (e) {
      expect(e).toBeInstanceOf(StreamError);
      expect((e as StreamError).code).toBe(ErrorCode.STREAM_CLOSED);
    }
  });
});

describe("MixerStream reading and credit", () => {
  it("returns credit at half the window as the app reads, not on receipt", async () => {
    const { host, stream } = make(16384);
    stream.handleData(new Uint8Array(6000));
    stream.handleData(new Uint8Array(6000));
    expect(host.ofType(FrameType.WINDOW)).toHaveLength(0);
    expect(stream.getRecvWindow()).toBe(16384 - 12000);
    await stream.read();
    expect(host.ofType(FrameType.WINDOW)).toHaveLength(0); // 6000 < 8192
    await stream.read();
    const windows = host.ofType(FrameType.WINDOW);
    expect(windows).toHaveLength(1);
    expect(windowIncrement(windows[0]!)).toBe(12000);
    expect(stream.getRecvWindow()).toBe(16384);
  });

  it("DATA exceeding recv credit throws ConnError(FLOW_CONTROL_ERROR)", () => {
    const { stream } = make(16384);
    try {
      stream.handleData(new Uint8Array(16385));
      expect.unreachable();
    } catch (e) {
      expect(e).toBeInstanceOf(ConnError);
      expect((e as ConnError).code).toBe(ErrorCode.FLOW_CONTROL_ERROR);
    }
  });

  it("keeps its own copy of each DATA payload", async () => {
    const { stream } = make();
    const payload = enc("abc");
    stream.handleData(payload);
    payload.fill(0);
    expect(dec(await stream.read())).toBe("abc");
  });

  it("a pending read is woken by DATA; concurrent reads are served FIFO", async () => {
    const { stream } = make();
    const r1 = stream.read();
    const r2 = stream.read();
    stream.handleData(enc("one"));
    stream.handleData(enc("two"));
    expect(dec(await r1)).toBe("one");
    expect(dec(await r2)).toBe("two");
  });

  it("CLOSE preserves buffered data: chunks first, then null (repeatedly)", async () => {
    const { stream } = make();
    stream.handleData(enc("hello"));
    stream.handleClose();
    expect(dec(await stream.read())).toBe("hello");
    expect(await stream.read()).toBeNull();
    expect(await stream.read()).toBeNull();
  });

  it("async iteration yields every chunk and ends at CLOSE", async () => {
    const { stream } = make();
    stream.handleData(enc("a"));
    stream.handleData(enc("b"));
    setTimeout(() => stream.handleClose(), 1);
    const got: string[] = [];
    for await (const c of stream) got.push(dec(c)!);
    expect(got).toEqual(["a", "b"]);
  });

  it("RESET from the peer discards buffered data and rejects reads with StreamError(code); closed=reset byPeer", async () => {
    const { host, stream } = make();
    stream.handleData(enc("buffered"));
    stream.handleReset(ErrorCode.CANCEL, "peer gave up");
    expect(stream.resetCode).toBe(ErrorCode.CANCEL);
    const err = (await stream.read().catch((e) => e)) as StreamError;
    expect(err).toBeInstanceOf(StreamError);
    expect(err.code).toBe(ErrorCode.CANCEL);
    expect(host.ofType(FrameType.RESET)).toHaveLength(0); // never RESET in answer to RESET
    expect(host.retired).toEqual([1]);
    await expect(stream.closed).resolves.toEqual({ kind: "reset", code: ErrorCode.CANCEL, name: "CANCEL", message: "peer gave up", byPeer: true });
  });

  it("RESET after CLOSE still discards the buffer (RESET wins)", async () => {
    const { stream } = make();
    stream.handleData(enc("x"));
    stream.handleClose();
    stream.handleReset(ErrorCode.INTERNAL_ERROR, "");
    await expect(stream.read()).rejects.toBeInstanceOf(StreamError);
  });
});

describe("MixerStream writing", () => {
  it("chunks writes into 16 KiB DATA frames", async () => {
    const { host, stream } = make(16384, 262144);
    await stream.write(new Uint8Array(16384 * 2 + 10));
    expect(host.data().map((d) => d.length)).toEqual([16384, 16384, 10]);
  });

  it("a string is written as UTF-8", async () => {
    const { host, stream } = make();
    await stream.write("héllo");
    expect(dec(host.data()[0]!)).toBe("héllo");
  });

  it("blocks at send window 0 and resumes on WINDOW", async () => {
    const { host, stream } = make(16384, 16384);
    let done = false;
    const p = stream.write(new Uint8Array(16384 + 5)).then(() => (done = true));
    await tick();
    expect(host.data().map((d) => d.length)).toEqual([16384]);
    expect(done).toBe(false);
    expect(stream.getSendWindow()).toBe(0);
    stream.handleWindow(100);
    await p;
    expect(host.data().map((d) => d.length)).toEqual([16384, 5]);
    expect(stream.getSendWindow()).toBe(95);
  });

  it("WINDOW overflow past 2^31-1 throws ConnError", () => {
    const { stream } = make(16384, 0x7fff_ffff);
    expect(() => stream.handleWindow(1)).toThrow(ConnError);
  });

  it("WINDOW is tolerated on a stream already closed", async () => {
    const { stream } = make();
    stream.handleClose();
    await stream.closeWrite();
    expect(() => stream.handleWindow(5)).not.toThrow();
  });

  it("writes are serialized in call order", async () => {
    const { host, stream } = make();
    host.hold = true;
    const a = stream.write("a");
    const b = stream.write("b");
    await tick();
    host.release();
    await Promise.all([a, b]);
    expect(host.data().map(dec)).toEqual(["a", "b"]);
  });

  it("write after closeWrite() rejects with STREAM_CLOSED", async () => {
    const { stream } = make();
    void stream.closeWrite();
    const err = (await stream.write("late").catch((e) => e)) as StreamError;
    expect(err).toBeInstanceOf(StreamError);
    expect(err.code).toBe(ErrorCode.STREAM_CLOSED);
  });

  it("closeWrite() waits for queued writes, then sends CLOSE after the last DATA", async () => {
    const { host, stream } = make();
    host.hold = true;
    const w = stream.write("data");
    const c = stream.closeWrite();
    await tick();
    expect(host.ofType(FrameType.CLOSE)).toHaveLength(0);
    expect(stream.getState()).toBe("open");
    host.release();
    await Promise.all([w, c]);
    expect(host.kinds()).toEqual(["DATA", "CLOSE"]);
    expect(stream.getState()).toBe("half_closed_local");
  });

  it("a credit-blocked write is woken by reset() and rejects with the reset code; closeWrite() still resolves", async () => {
    const { host, stream } = make(16384, 16384);
    const w = stream.write(new Uint8Array(16384 + 5));
    const werr = w.catch((e) => e as StreamError);
    await tick();
    const c = stream.closeWrite();
    await tick();
    stream.reset(ErrorCode.INTERNAL_ERROR, "giving up");
    const err = await werr;
    expect(err).toBeInstanceOf(StreamError);
    expect((err as StreamError).code).toBe(ErrorCode.INTERNAL_ERROR);
    await c;
    expect(host.ofType(FrameType.CLOSE)).toHaveLength(0);
  });
});

describe("MixerStream reset / close / refuse", () => {
  it("reset() sends RESET with a numeric code, retires, rejects pending reads, closed=reset not byPeer", async () => {
    const { host, stream } = make();
    const r = stream.read();
    stream.reset(ErrorCode.INTERNAL_ERROR, "app gave up");
    await expect(r).rejects.toMatchObject({ code: ErrorCode.INTERNAL_ERROR });
    const rst = host.ofType(FrameType.RESET);
    expect(rst).toHaveLength(1);
    expect(resetCode(rst[0]!)).toBe(ErrorCode.INTERNAL_ERROR);
    expect(resetMessage(rst[0]!)).toBe("app gave up");
    expect(host.retired).toEqual([1]);
    expect(stream.getState()).toBe("closed");
    await expect(stream.closed).resolves.toMatchObject({ kind: "reset", code: ErrorCode.INTERNAL_ERROR, byPeer: false });
  });

  it("reset() accepts a wire code name", () => {
    const { host, stream } = make();
    stream.reset("STREAM_LIMIT");
    expect(resetCode(host.ofType(FrameType.RESET)[0]!)).toBe(ErrorCode.STREAM_LIMIT);
  });

  it("reset() throws on an unknown code name", () => {
    const { stream } = make();
    expect(() => stream.reset("NOT_A_CODE")).toThrow(/unknown ws-mixer error code name/);
  });

  it("reset() truncates the message to 256 UTF-8 bytes on a character boundary", () => {
    const { host, stream } = make();
    stream.reset("CANCEL", "x" + "é".repeat(200));
    const msg = resetMessage(host.ofType(FrameType.RESET)[0]!);
    expect(msg).toBe("x" + "é".repeat(127));
    expect(new TextEncoder().encode(msg).length).toBe(255);
  });

  it("reset() on an already-closed stream is a no-op", async () => {
    const { host, stream } = make();
    stream.handleClose();
    await stream.closeWrite();
    stream.reset("CANCEL");
    expect(host.ofType(FrameType.RESET)).toHaveLength(0);
    await expect(stream.closed).resolves.toEqual({ kind: "finished" });
  });

  it("close() while the peer is still sending sends RESET(CANCEL), no CLOSE", () => {
    const { host, stream } = make();
    stream.close();
    expect(host.kinds()).toEqual(["RESET"]);
    expect(resetCode(host.ofType(FrameType.RESET)[0]!)).toBe(ErrorCode.CANCEL);
  });

  it("close() after the peer's CLOSE is just closeWrite(): CLOSE, no RESET", async () => {
    const { host, stream } = make();
    stream.handleClose();
    stream.close();
    await stream.closed;
    expect(host.kinds()).toEqual(["CLOSE"]);
  });

  it("refuse() sends RESET(REFUSED_STREAM)", () => {
    const { host, stream } = make();
    stream.refuse();
    expect(resetCode(host.ofType(FrameType.RESET)[0]!)).toBe(ErrorCode.REFUSED_STREAM);
  });

  it("abort() (SDK-detected) sends RESET and resolves closed=reset not byPeer; a no-op once closed", async () => {
    const { host, stream } = make();
    stream.abort(ErrorCode.STREAM_CLOSED, "DATA after CLOSE");
    stream.abort(ErrorCode.STREAM_CLOSED, "again");
    expect(host.ofType(FrameType.RESET)).toHaveLength(1);
    expect(stream.resetCode).toBe(ErrorCode.STREAM_CLOSED);
    await expect(stream.closed).resolves.toMatchObject({ kind: "reset", byPeer: false });
  });

  it("sendDone() is true with STREAM_CLOSED once CLOSE was sent, with the RESET error after reset", async () => {
    const a = make();
    expect(a.stream.sendDone().done).toBe(false);
    await a.stream.closeWrite();
    expect(a.stream.sendDone()).toMatchObject({ done: true, error: { code: ErrorCode.STREAM_CLOSED } });
    const b = make();
    b.stream.reset("CANCEL");
    expect(b.stream.sendDone()).toMatchObject({ done: true, error: { code: ErrorCode.CANCEL } });
  });
});

describe("MixerStream.terminate (connection teardown)", () => {
  const lost = () => new ConnError(ErrorCode.INTERNAL_ERROR, "socket closed with code 1006");

  it("an open stream: pending read rejects with the ConnError, closed=connection_lost, never a clean EOF", async () => {
    const { stream } = make();
    const r = stream.read();
    const err = lost();
    stream.terminate(err);
    await expect(r).rejects.toBe(err);
    await expect(stream.read()).rejects.toBe(err);
    await expect(stream.closed).resolves.toEqual({ kind: "connection_lost", error: err });
    expect(stream.getState()).toBe("closed");
  });

  it("keeps buffered data readable, then rejects (no CLOSE had arrived)", async () => {
    const { stream } = make();
    stream.handleData(enc("hello"));
    stream.terminate(lost());
    expect(dec(await stream.read())).toBe("hello");
    await expect(stream.read()).rejects.toBeInstanceOf(ConnError);
  });

  it("CLOSE already received: buffered data, then a clean null", async () => {
    const { stream } = make();
    stream.handleData(enc("hello"));
    stream.handleClose();
    stream.terminate(lost());
    expect(dec(await stream.read())).toBe("hello");
    expect(await stream.read()).toBeNull();
  });

  it("a credit-blocked write and later writes fail promptly with the connection's error", async () => {
    const { stream } = make(16384, 0);
    const w = stream.write("blocked");
    const werr = w.catch((e) => e);
    await tick();
    const err = lost();
    stream.terminate(err);
    expect(await werr).toBe(err);
    await expect(stream.write("later")).rejects.toBe(err);
  });

  it("a StreamError (drain-deadline CANCEL): resetCode set, closed=reset not byPeer, buffered data kept, no RESET sent", async () => {
    const { host, stream } = make();
    stream.handleData(enc("hi"));
    stream.terminate(new StreamError(ErrorCode.CANCEL, 1, "connection drained"));
    expect(stream.resetCode).toBe(ErrorCode.CANCEL);
    expect(dec(await stream.read())).toBe("hi");
    await expect(stream.read()).rejects.toMatchObject({ code: ErrorCode.CANCEL, message: "connection drained" });
    await expect(stream.closed).resolves.toEqual({ kind: "reset", code: ErrorCode.CANCEL, name: "CANCEL", message: "connection drained", byPeer: false });
    expect(host.ofType(FrameType.RESET)).toHaveLength(0);
  });

  it("is a no-op once the stream finished", async () => {
    const { stream } = make();
    stream.handleClose();
    await stream.closeWrite();
    stream.terminate(lost());
    expect(await stream.read()).toBeNull();
    await expect(stream.closed).resolves.toEqual({ kind: "finished" });
  });
});

describe("toWebStreams", () => {
  it("reads chunks then closes; writes DATA then CLOSE on writable.close()", async () => {
    const { host, stream } = make();
    const { readable, writable } = toWebStreams(stream);
    stream.handleData(enc("in"));
    stream.handleClose();
    const reader = readable.getReader();
    expect(dec((await reader.read()).value!)).toBe("in");
    expect((await reader.read()).done).toBe(true);
    const writer = writable.getWriter();
    await writer.write(enc("out"));
    await writer.close();
    expect(host.kinds()).toEqual(["DATA", "CLOSE"]);
  });

  it("cancelling the readable RESETs with CANCEL", async () => {
    const { host, stream } = make();
    const { readable } = toWebStreams(stream);
    await readable.cancel("nope");
    expect(resetCode(host.ofType(FrameType.RESET)[0]!)).toBe(ErrorCode.CANCEL);
  });
});
