/**
 * Values that come from a WebSocket living in another JS realm (a Zotero
 * plugin in a Firefox `Cu.Sandbox` using a chrome window's WebSocket): their
 * ArrayBuffers and Errors fail `instanceof` against this realm's globals.
 * `node:vm` contexts reproduce that.
 */
import vm from "node:vm";
import { describe, expect, it, vi } from "vitest";
import { FakeWS } from "./helpers/fake-ws.js";
import { MixerConn } from "../src/conn.js";
import { encodeControl } from "../src/control.js";
import { ConnError, ErrorCode } from "../src/errors.js";
import { decodeFrame, encodeData, encodeOpen } from "../src/frame.js";
import { MixerStream } from "../src/stream.js";
import { defaultWebSocketFactory, dialWebSocket } from "../src/socket.js";

function foreignArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const ab = vm.runInNewContext(`new ArrayBuffer(${bytes.length})`) as ArrayBuffer;
  new Uint8Array(ab).set(bytes);
  return ab;
}

function foreignUint8Array(bytes: Uint8Array): Uint8Array {
  // Offset view over a larger foreign buffer, so byteOffset/byteLength are honoured.
  const view = vm.runInNewContext(`new Uint8Array(${bytes.length + 4}).subarray(2, ${bytes.length + 2})`) as Uint8Array;
  view.set(bytes);
  return view;
}

const welcome = encodeData(
  0,
  encodeControl({
    t: "welcome",
    v: 1,
    session: "01XREALM",
    window: 262144,
    max_streams: 64,
    ping_interval: 30000,
    ping_timeout: 90000,
  } as never),
);

// The ArrayBuffer case is the regression guard; `ArrayBuffer.isView` was always realm-safe,
// so the Uint8Array case only pins that path.
describe.each([
  ["ArrayBuffer", foreignArrayBuffer],
  ["Uint8Array", foreignUint8Array],
] as const)("cross-realm %s message data", (_name, foreign) => {
  it("completes the handshake and dispatches frames", async () => {
    expect(foreign(new Uint8Array(1))).not.toBeInstanceOf(ArrayBuffer);
    expect(foreign(new Uint8Array(1))).not.toBeInstanceOf(Uint8Array);

    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: { sdk: "ws-mixer-js-test", sdk_version: "0.0.0" } });
    const errors: unknown[] = [];
    conn.on("error", (e) => errors.push(e));
    const handshake = conn.handshake();
    await new Promise((r) => setImmediate(r));
    const hello = JSON.parse(Buffer.from(decodeFrame(ws.sent[0]!).payload).toString()) as { t: string };
    expect(hello.t).toBe("hello");

    ws.receiveRaw(foreign(welcome));
    expect((await handshake).session).toBe("01XREALM");

    const streamP = new Promise<MixerStream>((resolve) => conn.on("stream", resolve));
    ws.receiveRaw(foreign(encodeOpen(1)));
    const stream = await streamP;
    ws.receiveRaw(foreign(encodeData(1, new TextEncoder().encode("hi"))));
    expect(Buffer.from((await stream.read())!).toString()).toBe("hi");

    expect(errors).toEqual([]);
    expect(ws.closedWith).toBeNull();
    expect(conn.stats().bytesIn).toBeGreaterThan(0);
  });
});

describe("cross-realm errors from the WebSocket", () => {
  it("a factory throwing a foreign Error rejects with its message", async () => {
    const foreignError = vm.runInNewContext(`new TypeError("boom from another realm")`) as Error;
    expect(foreignError).not.toBeInstanceOf(Error);
    await expect(
      dialWebSocket("wss://example.invalid", { protocols: ["ws-mixer.v1"], headers: {} }, () => {
        throw foreignError;
      }, 1000),
    ).rejects.toMatchObject({ message: "boom from another realm", fatal: true, cause: foreignError });
  });

  it("send() throwing a foreign Error surfaces its message", async () => {
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: { sdk: "ws-mixer-js-test", sdk_version: "0.0.0" } });
    conn.on("error", () => {});
    conn.on("close", () => {});
    const handshake = conn.handshake();
    await new Promise((r) => setImmediate(r));
    ws.receiveRaw(foreignArrayBuffer(welcome));
    await handshake;
    const foreignErr = vm.runInNewContext(`new Error("socket gone")`) as Error;
    ws.failNextSend(foreignErr);
    const err = (await conn.sendApp({ x: 1 }).catch((e: unknown) => e)) as ConnError;
    expect(err).toBeInstanceOf(ConnError);
    expect(err.message).toBe("socket gone");
    expect(err.cause).toBe(foreignErr);
  });

  it("the default factory wrapping a foreign throw keeps its plain message and cause", () => {
    const foreignError = vm.runInNewContext(`new SyntaxError("bad init")`) as Error;
    vi.stubGlobal(
      "WebSocket",
      class {
        constructor() {
          throw foreignError;
        }
      },
    );
    try {
      let thrown: unknown;
      try {
        defaultWebSocketFactory("wss://x", { protocols: ["ws-mixer.v1"], headers: {} });
      } catch (e) {
        thrown = e;
      }
      expect(thrown).toBeInstanceOf(TypeError);
      expect((thrown as Error).message).toContain("(bad init)");
      expect((thrown as Error).cause).toBe(foreignError);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe("message data that is not binary", () => {
  async function handshakeWith(data: unknown) {
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: { sdk: "ws-mixer-js-test", sdk_version: "0.0.0" } });
    conn.on("error", () => {});
    conn.on("close", () => {});
    const handshake = conn.handshake();
    await new Promise((r) => setImmediate(r));
    ws.receiveRaw(data);
    return (await handshake.catch((e: unknown) => e)) as ConnError;
  }

  it("a text frame is a PROTOCOL_ERROR", async () => {
    const err = await handshakeWith("x");
    expect(err).toBeInstanceOf(ConnError);
    expect(err.code).toBe(ErrorCode.PROTOCOL_ERROR);
    expect(err.message).toMatch(/text frame received/);
  });

  it("a Blob (binaryType ignored) is an INTERNAL_ERROR, not accepted as a buffer", async () => {
    const err = await handshakeWith(new Blob([new Uint8Array(welcome)]));
    expect(err).toBeInstanceOf(ConnError);
    expect(err.code).toBe(ErrorCode.INTERNAL_ERROR);
    expect(err.message).toMatch(/binaryType 'arraybuffer' not honoured/);
  });

  it("a foreign SharedArrayBuffer is read as binary", async () => {
    const sab = vm.runInNewContext(`new SharedArrayBuffer(${welcome.length})`) as SharedArrayBuffer;
    new Uint8Array(sab).set(welcome);
    expect(sab).not.toBeInstanceOf(SharedArrayBuffer);
    const ws = new FakeWS();
    const conn = new MixerConn(ws, { token: "t", agent: { sdk: "ws-mixer-js-test", sdk_version: "0.0.0" } });
    const handshake = conn.handshake();
    await new Promise((r) => setImmediate(r));
    ws.receiveRaw(sab);
    expect((await handshake).session).toBe("01XREALM");
    expect(ws.closedWith).toBeNull();
  });
});
