/** Unit tests for MixerStream's state machine, credit accounting and half-close semantics, against a fake StreamHost. */
import { describe, expect, it, vi } from "vitest";
import { MixerStream, type StreamHost } from "../src/stream.js";
import { ErrorCode } from "../src/errors.js";
import { decodeFrame, FrameType, windowIncrement } from "../src/frame.js";

function fakeHost(): StreamHost & { frames: Uint8Array[]; retired: number[] } {
  const frames: Uint8Array[] = [];
  const retired: number[] = [];
  return {
    frames,
    retired,
    sendData: vi.fn(async () => {}),
    sendControlFrame: (f: Uint8Array) => frames.push(f),
    retireStream: (id: number) => retired.push(id),
  };
}

describe("MixerStream state machine", () => {
  it("starts open and moves to half_closed_local on closeWrite", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    expect(s.getState()).toBe("open");
    s.closeWrite();
    expect(s.getState()).toBe("half_closed_local");
    const frame = decodeFrame(host.frames[0]!);
    expect(frame.type).toBe(FrameType.CLOSE);
  });

  it("half_closed_local + recv CLOSE -> closed, retires the id", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.closeWrite();
    s.handleClose();
    expect(s.getState()).toBe("closed");
    expect(host.retired).toContain(1);
  });

  it("recv CLOSE delivers buffered data, then EOF (CLOSE preserves data)", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.handleData(new TextEncoder().encode("hello"));
    s.handleClose();
    const chunks: Buffer[] = [];
    for await (const chunk of s) chunks.push(chunk as Buffer);
    expect(Buffer.concat(chunks).toString()).toBe("hello");
  });

  it("RESET discards buffered data", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.on("error", () => {});
    s.handleData(new TextEncoder().encode("hello"));
    s.handleReset(ErrorCode.CANCEL, "nevermind");
    expect(s.getState()).toBe("closed");
    // The stream is destroyed with the RESET error; reading throws rather than yielding "hello".
    expect(s.destroyed).toBe(true);
  });

  it("DATA exceeding recv credit throws (caller turns it into a connection error)", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 10, 1024);
    expect(() => s.handleData(new Uint8Array(11))).toThrow(/credit remaining/);
  });

  it("WINDOW credits sendWindow and DATA after CLOSE-sent is refused", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 0);
    expect(s.getSendWindow()).toBe(0);
    s.handleWindow(100);
    expect(s.getSendWindow()).toBe(100);
  });

  it("WINDOW is tolerated (ignored) on a stream already closed", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 0);
    s.on("error", () => {});
    s.handleReset(ErrorCode.CANCEL, "gone");
    expect(() => s.handleWindow(5)).not.toThrow();
  });

  it("WINDOW overflow past 2^31-1 throws", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 0x7fffffff - 1);
    expect(() => s.handleWindow(10)).toThrow(/2\^31-1/);
  });

  it("sendDone() is true once CLOSE has been sent, with a STREAM_CLOSED error", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.closeWrite();
    const { done, error } = s.sendDone();
    expect(done).toBe(true);
    expect(error!.code).toBe(ErrorCode.STREAM_CLOSED);
  });

  it("no DATA is sent after CLOSE or RESET: _write on a half-closed-local stream fails", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.closeWrite();
    await expect(
      new Promise<void>((resolve, reject) => {
        (s as unknown as { _write: (c: Uint8Array, e: string, cb: (e?: Error | null) => void) => void })._write(
          new Uint8Array([1]),
          "buffer",
          (err) => (err ? reject(err) : resolve()),
        );
      }),
    ).rejects.toThrow();
  });

  it("close() on a stream whose peer hasn't half-closed sends CLOSE then RESET(CANCEL)", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.on("error", () => {});
    s.close();
    const types = host.frames.map((f) => decodeFrame(f).type);
    expect(types).toEqual([FrameType.CLOSE, FrameType.RESET]);
    const resetFrame = decodeFrame(host.frames[1]!);
    expect(resetFrame.payload[3]).toBe(ErrorCode.CANCEL);
  });

  it("close() when the peer already half-closed is just CloseWrite + local teardown (no RESET)", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.handleClose(); // peer half-closed first
    s.close();
    const types = host.frames.map((f) => decodeFrame(f).type);
    expect(types).toEqual([FrameType.CLOSE]);
  });

  it("duplicate CLOSE from the peer is a STREAM_CLOSED stream error", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.handleClose();
    expect(() => s.handleClose()).toThrow(/duplicate CLOSE/);
  });

  it("credits WINDOW back once unacked reaches half the window", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 100, 1024);
    s.handleData(new Uint8Array(60)); // > window/2 = 50
    s.resume();
    await new Promise((r) => setImmediate(r));
    const windowFrames = host.frames.filter((f) => decodeFrame(f).type === FrameType.WINDOW);
    expect(windowFrames.length).toBe(1);
  });

  // --- item 1: credit-on-consume across all four consumption modes -----------

  const WINDOW = 262144; // default window; half-window threshold = 131072
  const PAYLOAD_BYTES = 150000; // > 128 KiB, forces at least one WINDOW credit

  /** Feeds `total` bytes into the stream as a sequence of recvWindow-respecting chunks. */
  function feed(s: MixerStream, total: number, chunkSize = 16384): void {
    let sent = 0;
    while (sent < total) {
      const n = Math.min(chunkSize, total - sent, s.getRecvWindow());
      if (n <= 0) break; // caller must drain between feeds if this happens
      s.handleData(new Uint8Array(n));
      sent += n;
    }
  }

  function windowIncrements(host: ReturnType<typeof fakeHost>): number[] {
    return host.frames.filter((f) => decodeFrame(f).type === FrameType.WINDOW).map((f) => windowIncrement(decodeFrame(f)));
  }

  it("credits on consume via on('data')", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, WINDOW, WINDOW);
    let received = 0;
    s.on("data", (c: Buffer) => (received += c.length));
    const done = new Promise<void>((resolve) => s.on("end", resolve));
    feed(s, PAYLOAD_BYTES);
    s.push(null);
    await done;
    expect(received).toBe(PAYLOAD_BYTES);
    const increments = windowIncrements(host);
    expect(increments.length).toBeGreaterThanOrEqual(1);
    const total = increments.reduce((a, b) => a + b, 0);
    expect(total).toBeGreaterThanOrEqual(WINDOW / 2);
    expect(total).toBeLessThanOrEqual(PAYLOAD_BYTES);
    expect(s.getRecvWindow()).toBeGreaterThan(WINDOW - PAYLOAD_BYTES);
    expect(s.getRecvWindow()).toBeGreaterThan(0);
  });

  it("credits on consume via pipe()", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, WINDOW, WINDOW);
    const { PassThrough } = await import("node:stream");
    const sink = new PassThrough();
    let received = 0;
    sink.on("data", (c: Buffer) => (received += c.length));
    const done = new Promise<void>((resolve) => sink.on("end", resolve));
    s.pipe(sink);
    feed(s, PAYLOAD_BYTES);
    s.push(null);
    await done;
    expect(received).toBe(PAYLOAD_BYTES);
    const total = windowIncrements(host).reduce((a, b) => a + b, 0);
    expect(total).toBeGreaterThanOrEqual(WINDOW / 2);
    expect(total).toBeLessThanOrEqual(PAYLOAD_BYTES);
    expect(s.getRecvWindow()).toBeGreaterThan(WINDOW - PAYLOAD_BYTES);
  });

  it("credits on consume via for await", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, WINDOW, WINDOW);
    let received = 0;
    const iterate = (async () => {
      for await (const chunk of s) received += (chunk as Buffer).length;
    })();
    feed(s, PAYLOAD_BYTES);
    s.push(null);
    await iterate;
    expect(received).toBe(PAYLOAD_BYTES);
    const total = windowIncrements(host).reduce((a, b) => a + b, 0);
    expect(total).toBeGreaterThanOrEqual(WINDOW / 2);
    expect(total).toBeLessThanOrEqual(PAYLOAD_BYTES);
    expect(s.getRecvWindow()).toBeGreaterThan(WINDOW - PAYLOAD_BYTES);
  });

  it("credits on consume via explicit read()", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, WINDOW, WINDOW);
    let received = 0;
    const done = new Promise<void>((resolve) => s.on("end", resolve));
    s.on("readable", () => {
      let chunk: Buffer | null;
      while ((chunk = s.read() as Buffer | null) !== null) received += chunk.length;
    });
    feed(s, PAYLOAD_BYTES);
    s.push(null);
    await done;
    expect(received).toBe(PAYLOAD_BYTES);
    const total = windowIncrements(host).reduce((a, b) => a + b, 0);
    expect(total).toBeGreaterThanOrEqual(WINDOW / 2);
    expect(total).toBeLessThanOrEqual(PAYLOAD_BYTES);
    expect(s.getRecvWindow()).toBeGreaterThan(WINDOW - PAYLOAD_BYTES);
  });

  // --- item 2: peer RESET must not throw with no 'error' listener ------------

  it("RESET(CANCEL) with no 'error' listener does not throw, fires 'close', and sets resetCode", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    // Deliberately no s.on("error", ...): this is exactly the case that used
    // to crash the process (an unhandled Duplex 'error' emission).
    const closeP = new Promise<void>((resolve) => s.on("close", resolve));
    const resetP = new Promise<{ code: number; message: string }>((resolve) => s.on("reset", resolve));
    expect(() => s.handleReset(ErrorCode.CANCEL, "peer cancelled")).not.toThrow();
    const info = await resetP;
    expect(info.code).toBe(ErrorCode.CANCEL);
    await closeP;
    expect(s.resetCode).toBe(ErrorCode.CANCEL);
  });

  it("RESET with an 'error' listener still destroys with the StreamError", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    const errP = new Promise<Error>((resolve) => s.on("error", resolve));
    s.handleReset(ErrorCode.CANCEL, "peer cancelled");
    const err = await errP;
    expect((err as unknown as { code: number }).code).toBe(ErrorCode.CANCEL);
    expect(s.destroyed).toBe(true);
  });

  // --- item 7: reset() code-name-or-number, message truncation, toWeb() ------

  it("reset() accepts a numeric code and sends RESET with it", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.on("error", () => {});
    s.reset(ErrorCode.CANCEL, "bye");
    const frame = decodeFrame(host.frames[0]!);
    expect(frame.type).toBe(FrameType.RESET);
    expect(frame.payload[3]).toBe(ErrorCode.CANCEL);
  });

  it("reset() accepts a wire code name and sends the matching numeric RESET code", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.on("error", () => {});
    s.reset("CANCEL", "bye");
    const frame = decodeFrame(host.frames[0]!);
    expect(frame.payload[3]).toBe(ErrorCode.CANCEL);
  });

  it("reset() rejects an unknown code name", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    expect(() => s.reset("NOT_A_REAL_CODE")).toThrow(/unknown ws-mixer error code name/);
  });

  it("reset() truncates the message to 256 UTF-8 bytes on a character boundary", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.on("error", () => {});
    // Every character is a 3-byte UTF-8 sequence ('☃' snowman), so a
    // naive byte-count truncation would split one in half.
    const longMessage = "☃".repeat(200); // 600 bytes
    s.reset(ErrorCode.CANCEL, longMessage);
    const frame = decodeFrame(host.frames[0]!);
    const messageBytes = frame.payload.subarray(4);
    expect(messageBytes.length).toBeLessThanOrEqual(256);
    // Decoding must not throw or produce a replacement character from a split sequence.
    const decoded = new TextDecoder("utf-8", { fatal: true }).decode(messageBytes);
    expect(decoded.length * 3).toBe(messageBytes.length);
  });

  // --- blocker 1: reset() must not throw with no 'error' listener ------------

  it("reset() with no 'error' listener does not throw and fires 'close'", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    // Deliberately no s.on("error", ...).
    const closeP = new Promise<void>((resolve) => s.on("close", resolve));
    expect(() => s.reset("INTERNAL_ERROR", "x")).not.toThrow();
    await closeP;
    expect(s.getState()).toBe("closed");
  });

  it("reset() with an 'error' listener still carries the code via 'error'", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    const errP = new Promise<Error>((resolve) => s.on("error", resolve));
    s.reset("INTERNAL_ERROR", "x");
    const err = await errP;
    expect((err as unknown as { code: number }).code).toBe(ErrorCode.INTERNAL_ERROR);
    expect(s.destroyed).toBe(true);
  });

  it("reset() sets resetCode but does not emit 'reset' (the app-called API already knows why)", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.on("error", () => {});
    let sawReset = false;
    s.on("reset", () => (sawReset = true));
    s.reset(ErrorCode.CANCEL, "app decided");
    expect(s.resetCode).toBe(ErrorCode.CANCEL);
    expect(sawReset).toBe(false);
  });

  it("abort() sets resetCode and emits 'reset', mirroring a peer-sent RESET", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.on("error", () => {});
    const resetP = new Promise<{ code: number; message: string }>((resolve) => s.on("reset", resolve));
    s.abort(ErrorCode.PROTOCOL_ERROR, "sdk-detected violation");
    const frame = decodeFrame(host.frames[0]!);
    expect(frame.type).toBe(FrameType.RESET);
    expect(frame.payload[3]).toBe(ErrorCode.PROTOCOL_ERROR);
    expect(s.resetCode).toBe(ErrorCode.PROTOCOL_ERROR);
    return resetP.then((info) => {
      expect(info.code).toBe(ErrorCode.PROTOCOL_ERROR);
      expect(info.message).toBe("sdk-detected violation");
    });
  });

  it("abort() on an already-closed stream is a no-op: no duplicate RESET, no 'reset' emitted", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    s.on("error", () => {});
    s.reset(ErrorCode.CANCEL, "first");
    host.frames.length = 0;
    let sawReset = false;
    s.on("reset", () => (sawReset = true));
    expect(() => s.abort(ErrorCode.PROTOCOL_ERROR, "too late")).not.toThrow();
    expect(host.frames).toHaveLength(0);
    expect(sawReset).toBe(false);
    expect(s.resetCode).toBe(ErrorCode.CANCEL); // unchanged from the first reset()
  });

  it("close() with no 'error' listener on a stream whose peer hasn't half-closed does not throw", async () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    const closeP = new Promise<void>((resolve) => s.on("close", resolve));
    expect(() => s.close()).not.toThrow();
    await closeP;
  });

  it("toWeb() returns a Web Streams readable/writable pair", () => {
    const host = fakeHost();
    const s = new MixerStream(1, host, 1024, 1024);
    const web = s.toWeb();
    expect(web.readable).toBeInstanceOf(ReadableStream);
    expect(web.writable).toBeInstanceOf(WritableStream);
  });
});
