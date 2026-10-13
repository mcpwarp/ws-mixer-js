import { afterEach, describe, expect, it, vi } from "vitest";
import {
  SUBPROTOCOL,
  defaultWebSocketFactory,
  dialWebSocket,
  webSocketFactory,
  type DialError,
  type WebSocketLike,
} from "../src/socket.js";
import { detectRuntime, isWsPackageConstructor, webSocketAcceptsHeaders } from "../src/runtime.js";
import { ErrorCode } from "../src/errors.js";

type Listener = (ev: any) => void;

class StubSocket implements WebSocketLike {
  static last: StubSocket | null = null;
  readyState = 0;
  protocol = "";
  bufferedAmount = 0;
  binaryType = "blob";
  closedWith: { code?: number; reason?: string } | null = null;
  upgradeFailure?: { status: number; headers?: Record<string, string | undefined> };
  readonly listeners = new Map<string, Set<Listener>>();
  constructor(
    readonly url: string,
    readonly arg?: unknown,
  ) {
    StubSocket.last = this;
  }
  send(): void {}
  close(code?: number, reason?: string): void {
    this.closedWith = { code, reason };
    this.readyState = 3;
  }
  addEventListener(type: string, l: Listener): void {
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type)!.add(l);
  }
  removeEventListener(type: string, l: Listener): void {
    this.listeners.get(type)?.delete(l);
  }
  fire(type: string, ev: unknown = {}): void {
    for (const l of [...(this.listeners.get(type) ?? [])]) l(ev);
  }
  open(protocol = SUBPROTOCOL): void {
    this.protocol = protocol;
    this.readyState = 1;
    this.fire("open");
  }
}

const init = { protocols: [SUBPROTOCOL], headers: { Authorization: "Bearer t", "X-Test": "1" } };
const stubFactory = () => (url: string) => new StubSocket(url);

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("WebSocket factories", () => {
  it("defaultWebSocketFactory passes {protocols, headers} to the global WebSocket on Node", () => {
    vi.stubGlobal("WebSocket", StubSocket);
    const s = defaultWebSocketFactory("wss://x", init) as StubSocket;
    expect(s).toBeInstanceOf(StubSocket);
    expect(s.arg).toEqual({ protocols: [SUBPROTOCOL], headers: init.headers });
  });

  it("webSocketFactory(ctor, {headers:false}) passes the protocols positionally and drops headers", () => {
    const s = webSocketFactory(StubSocket as never, { headers: false })("wss://x", init) as unknown as StubSocket;
    expect(s.arg).toEqual([SUBPROTOCOL]);
  });

  it("webSocketFactory(ctor) defaults to passing headers", () => {
    const s = webSocketFactory(StubSocket as never)("wss://x", init) as unknown as StubSocket;
    expect(s.arg).toEqual({ protocols: [SUBPROTOCOL], headers: init.headers });
  });

  it("a host-native WebSocket under Node gets protocols only (no {protocols, headers} object)", () => {
    vi.stubGlobal("WebSocket", StubSocket.bind(null));
    const s = defaultWebSocketFactory("wss://x", init) as StubSocket;
    expect(s.arg).toEqual([SUBPROTOCOL]);
  });

  it("an Electron renderer is not treated as Node: protocols only, runtime reported as electron", () => {
    vi.stubGlobal("WebSocket", StubSocket);
    const p = process as unknown as { type?: string };
    Object.defineProperty(process.versions, "electron", { value: "32.1.0", configurable: true, enumerable: true });
    p.type = "renderer";
    try {
      const s = defaultWebSocketFactory("wss://x", init) as StubSocket;
      expect(s.arg).toEqual([SUBPROTOCOL]);
      expect(detectRuntime().runtime).toBe("electron/32.1.0");
    } finally {
      delete (process.versions as Record<string, string>).electron;
      delete p.type;
    }
  });

  it("the `ws` package's class (global or explicit) is called as (url, protocols, { headers })", () => {
    class WsShaped extends StubSocket {
      static createWebSocketStream() {}
      static Server = class {};
      options: unknown;
      constructor(url: string, protocols?: unknown, options?: unknown) {
        super(url, protocols);
        this.options = options;
      }
      ping(): void {}
      terminate(): void {}
    }
    expect(isWsPackageConstructor(WsShaped)).toBe(true);
    expect(isWsPackageConstructor(StubSocket)).toBe(false);
    expect(webSocketAcceptsHeaders(WsShaped)).toBe(true);
    vi.stubGlobal("WebSocket", WsShaped);
    for (const s of [defaultWebSocketFactory("wss://x", init), webSocketFactory(WsShaped as never)("wss://x", init)] as WsShaped[]) {
      expect(s.arg).toEqual([SUBPROTOCOL]);
      expect(s.options).toEqual({ headers: init.headers });
    }
    const headerless = webSocketFactory(WsShaped as never, { headers: false })("wss://x", init) as unknown as WsShaped;
    expect(headerless.arg).toEqual([SUBPROTOCOL]);
    expect(headerless.options).toBeUndefined();

    // A subclass of ws's class inherits its statics.
    class WsSubclass extends WsShaped {}
    expect(isWsPackageConstructor(WsSubclass)).toBe(true);
    const sub = webSocketFactory(WsSubclass as never)("wss://x", init) as unknown as WsSubclass;
    expect(sub.arg).toEqual([SUBPROTOCOL]);
    expect(sub.options).toEqual({ headers: init.headers });
  });

  it("a native WebSocket with ping/terminate but no ws statics (Bun's) gets the {protocols, headers} init object", () => {
    class WebSocket extends StubSocket {
      ping(): void {}
      terminate(): void {}
    }
    // A Proxy over a class stringifies as native code, like a host constructor.
    const NativeLike = new Proxy(WebSocket, {});
    expect(Function.prototype.toString.call(NativeLike)).toMatch(/\[native code\]/);
    expect(NativeLike.name).toBe("WebSocket");
    expect(isWsPackageConstructor(NativeLike)).toBe(false);
    expect(isWsPackageConstructor(WebSocket)).toBe(false);
    for (const s of [webSocketFactory(NativeLike as never)("wss://x", init), webSocketFactory(WebSocket as never)("wss://x", init)] as unknown as StubSocket[]) {
      expect(s.arg).toEqual({ protocols: [SUBPROTOCOL], headers: init.headers });
    }
    // Statics alone do not make a native constructor `ws`.
    const NativeWithStatics = new Proxy(Object.assign(class extends WebSocket {}, { Server: class {} }), {});
    expect(isWsPackageConstructor(NativeWithStatics)).toBe(false);
  });

  it("a global WebSocket that throws on the init object fails with guidance to use headers:false", () => {
    class Strict extends StubSocket {
      constructor(url: string, arg?: unknown) {
        if (typeof arg === "object" && !Array.isArray(arg)) throw new SyntaxError("invalid subprotocol '[object Object]'");
        super(url, arg);
      }
    }
    vi.stubGlobal("WebSocket", Strict);
    expect(() => defaultWebSocketFactory("wss://x", init)).toThrow(/headers: false/);
  });

  it("no global WebSocket: the default factory throws and the dial fails fatally", async () => {
    vi.stubGlobal("WebSocket", undefined);
    expect(() => defaultWebSocketFactory("wss://x", init)).toThrow(TypeError);
    const err = (await dialWebSocket("wss://x", init, undefined, 1000).catch((e) => e)) as DialError;
    expect(err.fatal).toBe(true);
    expect(err.cause).toBeInstanceOf(TypeError);
  });
});

describe("dialWebSocket", () => {
  it("resolves on open with the subprotocol echoed and sets binaryType", async () => {
    const p = dialWebSocket("wss://x", init, stubFactory(), 1000);
    const s = StubSocket.last!;
    expect(s.binaryType).toBe("arraybuffer");
    s.open();
    await expect(p).resolves.toBe(s);
    expect(s.listeners.get("open")!.size).toBe(0);
  });

  it("missing subprotocol echo: closes with 4010 and fails fatally with UNSUPPORTED", async () => {
    const p = dialWebSocket("wss://x", init, stubFactory(), 1000);
    const s = StubSocket.last!;
    s.open("");
    const err = (await p.catch((e) => e)) as DialError;
    expect(err.fatal).toBe(true);
    expect(err.errorCode).toBe(ErrorCode.UNSUPPORTED);
    expect(s.closedWith?.code).toBe(4010);
  });

  it("error before open (no upgradeFailure) is a non-fatal dial failure carrying the event's message", async () => {
    const p = dialWebSocket("wss://x", init, stubFactory(), 1000);
    StubSocket.last!.fire("error", { message: "ECONNREFUSED" });
    const err = (await p.catch((e) => e)) as DialError;
    expect(err.fatal).toBe(false);
    expect(err.message).toBe("ECONNREFUSED");
    expect(err.httpStatus).toBeUndefined();
  });

  it("close before open is a non-fatal dial failure", async () => {
    const p = dialWebSocket("wss://x", init, stubFactory(), 1000);
    StubSocket.last!.fire("close", { code: 1006 });
    const err = (await p.catch((e) => e)) as DialError;
    expect(err.fatal).toBe(false);
    expect(err.message).toBe("socket closed with code 1006");
  });

  for (const status of [401, 403, 404]) {
    it(`upgradeFailure ${status} is fatal with httpStatus`, async () => {
      const p = dialWebSocket("wss://x", init, stubFactory(), 1000);
      const s = StubSocket.last!;
      s.upgradeFailure = { status };
      s.fire("error", {});
      s.fire("close", { code: 1006 });
      const err = (await p.catch((e) => e)) as DialError;
      expect(err.fatal).toBe(true);
      expect(err.httpStatus).toBe(status);
      expect(err.errorCode).toBeUndefined();
    });
  }

  it("upgradeFailure 429 carries Retry-After and is non-fatal", async () => {
    const p = dialWebSocket("wss://x", init, stubFactory(), 1000);
    const s = StubSocket.last!;
    s.upgradeFailure = { status: 429, headers: { "retry-after": "7" } };
    s.fire("error", {});
    const err = (await p.catch((e) => e)) as DialError;
    expect(err.fatal).toBe(false);
    expect(err.httpStatus).toBe(429);
    expect(err.retryAfterMs).toBe(7000);
  });

  it("upgradeFailure 503 is non-fatal with httpStatus", async () => {
    const p = dialWebSocket("wss://x", init, stubFactory(), 1000);
    const s = StubSocket.last!;
    s.upgradeFailure = { status: 503 };
    s.fire("close", { code: 1006 });
    const err = (await p.catch((e) => e)) as DialError;
    expect(err.fatal).toBe(false);
    expect(err.httpStatus).toBe(503);
  });

  it("times out (non-fatal) and closes the socket", async () => {
    vi.useFakeTimers();
    const p = dialWebSocket("wss://x", init, stubFactory(), 500);
    const s = StubSocket.last!;
    const caught = p.catch((e: DialError) => e);
    await vi.advanceTimersByTimeAsync(500);
    const err = (await caught) as DialError;
    expect(err.fatal).toBe(false);
    expect(err.message).toBe("connect timed out after 500ms");
    expect(s.closedWith).not.toBeNull();
  });
});

describe("runtime detection", () => {
  it("reports node/<version> and <platform>/<arch> on Node", () => {
    const info = detectRuntime();
    expect(info.runtime).toBe(`node/${process.versions.node}`);
    expect(info.os).toBe(`${process.platform}/${process.arch}`);
    expect(webSocketAcceptsHeaders(globalThis.WebSocket)).toBe(true);
  });
});
