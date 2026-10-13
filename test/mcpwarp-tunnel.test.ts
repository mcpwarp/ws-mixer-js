/** createTunnel() against the in-process FakeEdge: idempotent configure, generations, status mapping, conflict, stop/close. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { createTunnel, type ExposeHandle, type Tunnel, type TunnelBase, type TunnelConfig, type TunnelStatus } from "../src/mcpwarp/index.js";
import { FakeEdge } from "../src/testing/index.js";
import { ErrorCode } from "../src/errors.js";
import type { WebSocketFactory, WebSocketLike } from "../src/socket.js";

// Real expose by default; a test can put a fake behind exposeWithServer.
const exposeHook = vi.hoisted(() => ({ fn: undefined as undefined | (() => Promise<ExposeHandle>) }));
vi.mock("../src/mcpwarp/expose.js", async (importOriginal) => {
  const mod = await importOriginal<typeof import("../src/mcpwarp/expose.js")>();
  return {
    ...mod,
    exposeWithServer: (...args: Parameters<typeof mod.exposeWithServer>) => (exposeHook.fn ? exposeHook.fn() : mod.exposeWithServer(...args)),
  };
});

const INTEGRATION = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10";
const TOKEN = "mcpwarp_pat_id_secret";
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  exposeHook.fn = undefined;
  for (const c of cleanups.splice(0).reverse()) await c();
});

/** A handle that already moved to `state` and never emits. */
function fakeHandle(state: ExposeHandle["state"]): ExposeHandle & { close: ReturnType<typeof vi.fn> } {
  const h = {
    state,
    url: state === "registered" ? "https://fake.mcpwarp.test/mcp" : undefined,
    on: () => h,
    close: vi.fn(async () => {}),
  };
  return h as unknown as ExposeHandle & { close: ReturnType<typeof vi.fn> };
}

/** Wraps `factory` so the client sees "open" only after `gate` resolves and reads `bufferedAmount()` if given; records the sockets. */
function wrappedSockets(factory: WebSocketFactory, sockets: WebSocketLike[], gate: Promise<void> = Promise.resolve(), bufferedAmount?: () => number): WebSocketFactory {
  return (url, init) => {
    const ws = factory(url, init);
    sockets.push(ws);
    const wrapped = new Map<unknown, (ev: unknown) => void>();
    return new Proxy(ws, {
      get(target, prop) {
        if (prop === "addEventListener") {
          return (type: Parameters<WebSocketLike["addEventListener"]>[0], listener: (ev: unknown) => void) => {
            if (type !== "open") return target.addEventListener(type, listener);
            const w = (ev: unknown) => void gate.then(() => listener(ev));
            wrapped.set(listener, w);
            target.addEventListener(type, w);
          };
        }
        if (prop === "removeEventListener") {
          return (type: Parameters<WebSocketLike["addEventListener"]>[0], listener: (ev: unknown) => void) => target.removeEventListener?.(type, wrapped.get(listener) ?? listener);
        }
        if (prop === "bufferedAmount" && bufferedAmount) return bufferedAmount();
        const v: unknown = Reflect.get(target, prop);
        return typeof v === "function" ? v.bind(target) : v;
      },
      set(target, prop, value) {
        return Reflect.set(target, prop, value);
      },
    });
  };
}

async function startEdge() {
  const edge = new FakeEdge();
  await edge.start();
  cleanups.push(() => edge.stop());
  return edge;
}

function tunnelOn(edge: FakeEdge, base: Partial<TunnelBase> = {}): { tunnel: Tunnel; history: TunnelStatus[]; logs: Array<[string, string]> } {
  const logs: Array<[string, string]> = [];
  const tunnel = createTunnel({
    integration: INTEGRATION,
    webSocket: edge.webSocket,
    reconnect: { base: 10, cap: 50 },
    handler: () => new Response("ok"),
    logger: (level, message) => logs.push([level, message]),
    ...base,
  } as TunnelBase);
  cleanups.push(() => tunnel.close());
  const history: TunnelStatus[] = [];
  tunnel.subscribe((s) => history.push(s));
  return { tunnel, history, logs };
}

function cfg(edge: FakeEdge, over: Partial<TunnelConfig> = {}): TunnelConfig {
  return { enabled: true, token: TOKEN, name: "notes", url: edge.url, ...over };
}

async function waitFor(cond: () => boolean, ms = 5000): Promise<void> {
  const until = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > until) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 5));
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("createTunnel", () => {
  it("subscribe fires at once; happy path goes off -> connecting -> online with the url", async () => {
    const edge = await startEdge();
    const { tunnel, history, logs } = tunnelOn(edge);
    expect(history).toEqual([{ state: "off" }]);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    expect(history).toEqual([{ state: "off" }, { state: "connecting" }, { state: "online", url: edge.publicUrl("notes") }]);
    expect(logs.some(([l, m]) => l === "info" && m.includes(edge.publicUrl("notes")))).toBe(true);
    expect(logs.some(([, m]) => m.includes(TOKEN))).toBe(false);
  });

  it("status is a snapshot copy", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    const s = tunnel.status;
    s.state = "online";
    expect(tunnel.status).toEqual({ state: "off" });
  });

  it("an identical configure() is a no-op: one connection", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await tunnel.configure(cfg(edge));
    await sleep(100);
    expect(edge.upgrades).toHaveLength(1);
    expect(edge.connections).toHaveLength(1);
    expect(tunnel.status.state).toBe("online");
  });

  it("a changed config restarts: the old service is unregistered and its connection closed, the new one registers", async () => {
    const edge = await startEdge();
    const { tunnel, history } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await tunnel.configure(cfg(edge, { name: "notes2" }));
    await waitFor(() => tunnel.status.state === "online");
    expect(tunnel.status).toEqual({ state: "online", url: edge.publicUrl("notes2") });
    await waitFor(() => edge.connections.length === 1);
    expect(edge.upgrades).toHaveLength(2);
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register", "unregister", "register"]);
    expect(history.map((s) => s.state)).toEqual(["off", "connecting", "online", "connecting", "online"]);
  });

  it("events of the old connection after a restart are ignored", async () => {
    const edge = await startEdge();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    cleanups.push(() => release());
    let calls = 0;
    const { tunnel, history } = tunnelOn(edge, {
      handler: async () => {
        calls++;
        await gate;
        return new Response("late");
      },
    });
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const oldConn = edge.connections[0]!;
    // An in-flight request keeps the old connection draining while the restart waits for it.
    const inflight = edge.fetch(edge.publicUrl("notes")).catch((e: Error) => e);
    await waitFor(() => calls === 1);
    const restarted = tunnel.configure(cfg(edge, { name: "notes2" }));
    await edge.waitForApp("unregister");
    await edge.sendApp({ mcpwarp: { v: 1, op: "disable", id: edge.serverId("notes"), reason: "disabled" } }, oldConn);
    await edge.drain({ conn: oldConn, deadlineMs: 25000 }).catch(() => {});
    edge.closeConnection({ wsCode: 4000 + ErrorCode.UNAUTHORIZED, conn: oldConn });
    await restarted;
    release();
    await inflight;
    await waitFor(() => tunnel.status.state === "online");
    expect(tunnel.status).toEqual({ state: "online", url: edge.publicUrl("notes2") });
    expect(history).toEqual([
      { state: "off" },
      { state: "connecting" },
      { state: "online", url: edge.publicUrl("notes") },
      { state: "connecting" },
      { state: "online", url: edge.publicUrl("notes2") },
    ]);
  });

  it("overlapping configure() calls: the last one wins, superseded ones never dial", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    const a = tunnel.configure(cfg(edge, { name: "a" }));
    const b = tunnel.configure(cfg(edge, { name: "b" }));
    const c = tunnel.configure(cfg(edge, { name: "c" }));
    await Promise.all([a, b, c]);
    await waitFor(() => tunnel.status.state === "online");
    expect(tunnel.status.url).toBe(edge.publicUrl("c"));
    expect(edge.upgrades).toHaveLength(1);
  });

  it("a configure() while the previous connection drains does not leave the state stuck", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const p1 = tunnel.configure(cfg(edge, { name: "b" }));
    const p2 = tunnel.configure(cfg(edge, { enabled: false }));
    const p3 = tunnel.configure(cfg(edge, { name: "c" }));
    await Promise.all([p1, p2, p3]);
    await waitFor(() => tunnel.status.state === "online");
    expect(tunnel.status.url).toBe(edge.publicUrl("c"));
    await waitFor(() => edge.connections.length === 1);
    expect(edge.upgrades).toHaveLength(2);
    const ops = edge.appLog.map((b) => b.mcpwarp as { op: string; services: Array<{ name: string }> });
    expect(ops.map((o) => o.op)).toEqual(["register", "unregister", "register"]);
    expect(ops.filter((o) => o.op === "register").map((o) => o.services[0]!.name)).toEqual(["notes", "c"]);
  });

  it("a repeat of a pending configure() returns the pending apply", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    const p1 = tunnel.configure(cfg(edge));
    const p2 = tunnel.configure(cfg(edge));
    expect(p2).toBe(p1);
    await p2;
    expect(edge.upgrades).toHaveLength(1);
  });

  it("whitespace around the token is not a change", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await tunnel.configure(cfg(edge, { token: ` ${TOKEN}\n` }));
    await sleep(50);
    expect(edge.upgrades).toHaveLength(1);
    expect(tunnel.status.state).toBe("online");
  });

  it("a non-fatal error while online is logged but leaves the status alone", async () => {
    const edge = await startEdge();
    const { tunnel, history, logs } = tunnelOn(edge, {
      handler: () => {
        throw new Error("handler boom");
      },
    });
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const res = await edge.fetch(edge.publicUrl("notes"));
    expect(res.status).toBe(500);
    await waitFor(() => logs.some(([l, m]) => l === "warn" && m.includes("handler boom")));
    expect(tunnel.status).toEqual({ state: "online", url: edge.publicUrl("notes") });
    expect(history.map((s) => s.state)).toEqual(["off", "connecting", "online"]);
  });

  it("a logger that calls stop() while the tunnel starts prevents the dial", async () => {
    const edge = await startEdge();
    let tunnel!: Tunnel;
    tunnel = tunnelOn(edge, {
      logger: (_level, message) => {
        if (message.includes("connecting as")) void tunnel.stop();
      },
    }).tunnel;
    await tunnel.configure(cfg(edge));
    await sleep(50);
    expect(edge.upgrades).toHaveLength(0);
    expect(tunnel.status).toEqual({ state: "off" });
  });

  it("enabled=false and an empty token are off without dialing (an empty token is no error)", async () => {
    const edge = await startEdge();
    const { tunnel, history } = tunnelOn(edge);
    await tunnel.configure(cfg(edge, { enabled: false }));
    expect(tunnel.status).toEqual({ state: "off" });
    await tunnel.configure(cfg(edge, { token: "   " }));
    expect(tunnel.status).toEqual({ state: "off" });
    await sleep(50);
    expect(edge.upgrades).toHaveLength(0);
    expect(history).toEqual([{ state: "off" }]);
  });

  it("disabling an online tunnel unregisters and goes off", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await tunnel.configure(cfg(edge, { enabled: false }));
    expect(tunnel.status).toEqual({ state: "off" });
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register", "unregister"]);
    await waitFor(() => edge.connections.length === 0);
  });

  it("conflict: state conflict, no re-dial, an identical configure() restarts", async () => {
    const edge = await startEdge();
    const { tunnel, history, logs } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await edge.conflict("notes");
    await waitFor(() => tunnel.status.state === "conflict");
    expect(tunnel.status.lastError).toMatch(/took over/);
    expect(logs.some(([l, m]) => l === "warn" && m.includes("took over"))).toBe(true);
    await waitFor(() => edge.connections.length === 0);
    await sleep(200);
    expect(edge.upgrades).toHaveLength(1);
    expect(tunnel.status.state).toBe("conflict");

    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    expect(edge.upgrades).toHaveLength(2);
    expect(history.map((s) => s.state)).toEqual(["off", "connecting", "online", "conflict", "connecting", "online"]);
  });

  it("a conflict before the first registration (expose() rejects TAKEN_OVER) is state conflict", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    edge.withholdRegistered("notes");
    await tunnel.configure(cfg(edge));
    await edge.waitForApp("register");
    await edge.conflict("notes");
    await waitFor(() => tunnel.status.state === "conflict");
    await sleep(100);
    expect(edge.upgrades).toHaveLength(1);
  });

  it("a fatal failure is state error with lastError; the same config then restarts", async () => {
    const edge = await startEdge();
    const { tunnel, logs } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    edge.closeConnection({ wsCode: 4000 + ErrorCode.UNAUTHORIZED });
    await waitFor(() => tunnel.status.state === "error");
    expect(tunnel.status.lastError).toBeTruthy();
    expect(logs.some(([l]) => l === "error")).toBe(true);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
  });

  it("a rejected token before registration is state error", async () => {
    const edge = new FakeEdge({ acceptToken: () => false });
    await edge.start();
    cleanups.push(() => edge.stop());
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "error");
  });

  it("an invalid name in the config is state error, not a throw", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge, { name: "Bad_Name" }));
    expect(tunnel.status.state).toBe("error");
    expect(tunnel.status.lastError).toMatch(/invalid service `name`/);
    expect(edge.upgrades).toHaveLength(0);
  });

  it("createTunnel validates integration, agent and handler/target up front", () => {
    const handler = () => new Response();
    expect(() => createTunnel({ integration: "nope", handler })).toThrow(/RFC 4122/);
    expect(() => createTunnel({ integration: INTEGRATION } as TunnelBase)).toThrow(/exactly one/);
    expect(() => createTunnel({ integration: INTEGRATION, handler, target: "http://x" } as unknown as TunnelBase)).toThrow(/exactly one/);
    expect(() => createTunnel({ integration: INTEGRATION, handler, agent: { runtime: "a\nb" } })).toThrow(/agent\.runtime/);
  });

  it("disabled from the dashboard maps to disabled, enable back to online", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await edge.disable("notes");
    await waitFor(() => tunnel.status.state === "disabled");
    await edge.enable("notes");
    await waitFor(() => tunnel.status.state === "online");
  });

  it("expose() resolving disabled maps to disabled", async () => {
    const edge = await startEdge();
    await edge.disable("notes");
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "disabled");
    expect(tunnel.status.url).toBeUndefined();
  });

  it("a lost connection goes connecting with lastError, then online again", async () => {
    const edge = await startEdge();
    const { tunnel, history } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    edge.closeConnection({ wsCode: 1006 });
    await waitFor(() => history.length >= 4 && tunnel.status.state === "online");
    expect(history[3]!.state).toBe("connecting");
    expect(history[3]!.lastError).toBeTruthy();
  });

  it("onRetry reasons set lastError without changing state and reach base.onRetry", async () => {
    const edge = await startEdge();
    const retries: string[] = [];
    const { tunnel } = tunnelOn(edge, { onRetry: (i) => retries.push(i.reason) });
    edge.failRegister("notes", { code: "INTERNAL" });
    await tunnel.configure(cfg(edge));
    await waitFor(() => retries.length === 1);
    expect(tunnel.status.lastError).toMatch(/INTERNAL/);
    await waitFor(() => tunnel.status.state === "online", 4000);
  });

  it("stop() goes off and is reusable; the same config then restarts", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await tunnel.stop();
    expect(tunnel.status).toEqual({ state: "off" });
    await waitFor(() => edge.connections.length === 0);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    expect(edge.upgrades).toHaveLength(2);
  });

  it("stop() while still dialing aborts cleanly (no unhandled rejection)", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    edge.withholdRegistered("notes");
    await tunnel.configure(cfg(edge));
    await edge.waitForApp("register");
    await tunnel.stop();
    expect(tunnel.status).toEqual({ state: "off" });
    await waitFor(() => edge.connections.length === 0);
  });

  it("stop() while still registering resolves only after the client closed its socket", async () => {
    const edge = await startEdge();
    const sockets: WebSocketLike[] = [];
    // A full send buffer holds the client's drain write, so its close takes a while.
    let stalled = false;
    const { tunnel } = tunnelOn(edge, { webSocket: wrappedSockets(edge.webSocket, sockets, undefined, () => (stalled ? 2 * 1024 * 1024 : 0)) });
    edge.withholdRegistered("notes");
    await tunnel.configure(cfg(edge));
    await edge.waitForApp("register");
    stalled = true;
    const unstall = setTimeout(() => (stalled = false), 150);
    cleanups.push(() => clearTimeout(unstall));
    await tunnel.stop();
    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.readyState).toBeGreaterThanOrEqual(2);
  });

  it("stop() during the WebSocket dial: never online, the late socket is closed, no re-dial", async () => {
    const edge = await startEdge();
    let open!: () => void;
    const gate = new Promise<void>((r) => (open = r));
    cleanups.push(() => open());
    const sockets: WebSocketLike[] = [];
    const { tunnel, history } = tunnelOn(edge, { webSocket: wrappedSockets(edge.webSocket, sockets, gate) });
    await tunnel.configure(cfg(edge));
    await waitFor(() => sockets.length === 1);
    await tunnel.stop();
    expect(tunnel.status).toEqual({ state: "off" });
    open();
    await waitFor(() => sockets[0]!.readyState === 3);
    await waitFor(() => edge.connections.length === 0);
    await sleep(100);
    expect(edge.upgrades).toHaveLength(1);
    expect(history.map((s) => s.state)).toEqual(["off", "connecting", "off"]);
  });

  it("a handle that already moved on when expose() resolves maps by its state", async () => {
    const edge = await startEdge();
    const cases: Array<[ExposeHandle["state"], string]> = [
      ["closed", "error"],
      ["closing", "error"],
      ["conflict", "conflict"],
      ["registering", "connecting"],
    ];
    for (const [handleState, tunnelState] of cases) {
      const handle = fakeHandle(handleState);
      exposeHook.fn = () => Promise.resolve(handle);
      const { tunnel } = tunnelOn(edge);
      await tunnel.configure(cfg(edge));
      await waitFor(() => tunnel.status.state === tunnelState);
      if (tunnelState !== "connecting") await waitFor(() => handle.close.mock.calls.length === 1);
      await tunnel.close();
    }
  });

  it("a handle resolving after stop() is closed and never shown", async () => {
    const edge = await startEdge();
    let resolve!: (h: ExposeHandle) => void;
    exposeHook.fn = () => new Promise<ExposeHandle>((r) => (resolve = r));
    const { tunnel, history } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    const stopped = tunnel.stop();
    const handle = fakeHandle("registered");
    resolve(handle);
    await stopped;
    expect(handle.close).toHaveBeenCalledTimes(1);
    expect(history.map((s) => s.state)).toEqual(["off", "connecting", "off"]);
  });

  it("close() is final: a final off, listeners cleared, configure()/stop() no-ops; its timer is cleared", async () => {
    const edge = await startEdge();
    const { tunnel, history } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const setSpy = vi.spyOn(globalThis, "setTimeout");
    const clearSpy = vi.spyOn(globalThis, "clearTimeout");
    try {
      await tunnel.close({ timeoutMs: 4321 });
      const idx = setSpy.mock.calls.findIndex((c) => c[1] === 4321);
      expect(idx).toBeGreaterThanOrEqual(0);
      expect(clearSpy.mock.calls.map((c) => c[0])).toContain(setSpy.mock.results[idx]!.value);
    } finally {
      setSpy.mockRestore();
      clearSpy.mockRestore();
    }
    expect(history.at(-1)).toEqual({ state: "off" });
    const n = history.length;
    await tunnel.configure(cfg(edge, { name: "other" }));
    await tunnel.stop();
    await sleep(50);
    expect(history).toHaveLength(n);
    expect(edge.upgrades).toHaveLength(1);
    expect(edge.connections).toHaveLength(0);
  });

  it("close() is bounded by timeoutMs when the drain hangs", async () => {
    const edge = await startEdge();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    cleanups.push(() => release());
    let calls = 0;
    const { tunnel } = tunnelOn(edge, {
      handler: async () => {
        calls++;
        await gate;
        return new Response("late");
      },
    });
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const inflight = edge.fetch(edge.publicUrl("notes")).catch((e: Error) => e);
    await waitFor(() => calls === 1);
    const started = Date.now();
    await tunnel.close({ timeoutMs: 150 });
    const took = Date.now() - started;
    release();
    await inflight;
    expect(took).toBeGreaterThanOrEqual(140);
    expect(took).toBeLessThan(1000);
    expect(tunnel.status).toEqual({ state: "off" });
  });

  it("a throwing listener does not break later notifications or other listeners", async () => {
    const edge = await startEdge();
    const { tunnel, history, logs } = tunnelOn(edge);
    let thrown = 0;
    tunnel.subscribe(() => {
      thrown++;
      throw new Error("listener boom");
    });
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    expect(thrown).toBe(3);
    expect(history.map((s) => s.state)).toEqual(["off", "connecting", "online"]);
    expect(logs.filter(([l, m]) => l === "error" && m.includes("listener boom"))).toHaveLength(3);
  });

  it("unsubscribe stops notifications", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    const seen: string[] = [];
    const off = tunnel.subscribe((s) => seen.push(s.state));
    off();
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    expect(seen).toEqual(["off"]);
  });

  it("runs without AbortController, DOMException, queueMicrotask and structuredClone globals", async () => {
    const edge = await startEdge();
    try {
      vi.stubGlobal("AbortController", undefined);
      vi.stubGlobal("DOMException", undefined);
      vi.stubGlobal("queueMicrotask", undefined);
      vi.stubGlobal("structuredClone", undefined);
      const { tunnel, history } = tunnelOn(edge);
      await tunnel.configure(cfg(edge));
      await waitFor(() => tunnel.status.state === "online");
      await tunnel.configure(cfg(edge, { name: "notes2" }));
      await waitFor(() => tunnel.status.state === "online");
      edge.withholdRegistered("notes3");
      await tunnel.configure(cfg(edge, { name: "notes3" }));
      await edge.waitForApp("register", { count: 3 });
      await tunnel.close();
      expect(history.map((s) => s.state)).toEqual(["off", "connecting", "online", "connecting", "online", "connecting", "off"]);
      await waitFor(() => edge.connections.length === 0);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
