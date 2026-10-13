/** createTunnel() against the in-process FakeEdge: idempotent configure, generations, status mapping, conflict, stop/dispose. */
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createTunnel,
  McpwarpError,
  TunnelStatusCode,
  type ExposeHandle,
  type Tunnel,
  type TunnelBase,
  type TunnelConfig,
  type TunnelOptions,
  type TunnelStatus,
} from "../src/mcpwarp/index.js";
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

/** A fakeHandle() whose events a test emits. */
function emittingHandle(state: ExposeHandle["state"]): { handle: ExposeHandle; wired: () => boolean; emit: (event: string, payload: unknown) => void } {
  const listeners = new Map<string, Array<(payload: unknown) => void>>();
  const h = {
    state,
    url: undefined,
    on: (event: string, fn: (payload: unknown) => void) => {
      listeners.set(event, [...(listeners.get(event) ?? []), fn]);
      return h;
    },
    close: vi.fn(async () => {}),
  };
  return {
    handle: h as unknown as ExposeHandle,
    wired: () => listeners.size > 0,
    emit: (event, payload) => {
      for (const fn of listeners.get(event) ?? []) fn(payload);
    },
  };
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

function tunnelOn(edge: FakeEdge, base: Partial<TunnelOptions> = {}): { tunnel: Tunnel; history: TunnelStatus[]; logs: Array<[string, string]> } {
  const logs: Array<[string, string]> = [];
  const tunnel = createTunnel({
    integration: INTEGRATION,
    webSocket: edge.webSocket,
    reconnect: { base: 10, cap: 50 },
    handler: () => new Response("ok"),
    logger: (level, message) => logs.push([level, message]),
    ...base,
  } as TunnelOptions);
  cleanups.push(() => tunnel.dispose());
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
    expect(logs.filter(([, m]) => m.startsWith("mcpwarp: "))).toEqual([]);
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
    expect(tunnel.status).toEqual({ state: "conflict", code: TunnelStatusCode.TAKEN_OVER, lastError: "Another connection took over this name." });
    expect(logs.some(([l, m]) => l === "warn" && m.includes("took over") && m.includes('"notes"'))).toBe(true);
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
    expect(tunnel.status.code).toBe(TunnelStatusCode.TAKEN_OVER);
    await sleep(100);
    expect(edge.upgrades).toHaveLength(1);
  });

  it("a fatal failure is state error with code and a plain lastError, the technical message logged; the same config then restarts", async () => {
    const edge = await startEdge();
    const { tunnel, logs } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    edge.closeConnection({ wsCode: 4000 + ErrorCode.UNAUTHORIZED });
    await waitFor(() => tunnel.status.state === "error");
    expect(tunnel.status).toEqual({ state: "error", code: TunnelStatusCode.UNAUTHORIZED, lastError: "The token was rejected." });
    expect(logs.some(([l, m]) => l === "error" && m.startsWith("UNAUTHORIZED: "))).toBe(true);
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
    expect(tunnel.status.code).toBe(TunnelStatusCode.UNAUTHORIZED);
  });

  it("an invalid name in the config is state error, not a throw", async () => {
    const edge = await startEdge();
    const { tunnel, logs } = tunnelOn(edge);
    await tunnel.configure(cfg(edge, { name: "Bad_Name" }));
    expect(tunnel.status.state).toBe("error");
    expect(tunnel.status.code).toBe(TunnelStatusCode.INVALID_CONFIG);
    expect(tunnel.status.lastError).toMatch(/^The service name is invalid/);
    expect(logs.some(([l, m]) => l === "error" && m.startsWith("invalid service `name`"))).toBe(true);
    expect(edge.upgrades).toHaveLength(0);
  });

  it("while enabled, config fields of the wrong type (name or token undefined, as from an unset pref) are state error INVALID_CONFIG, not a throw; values are never echoed", async () => {
    const edge = await startEdge();
    const { tunnel, logs } = tunnelOn(edge);
    const cases: Array<[Record<string, unknown>, string]> = [
      [{ name: undefined }, "The service name is not set."],
      [{ token: undefined }, "The token is not set."],
      [{ name: null }, "The service name is not set."],
      [{ enabled: "yes" }, "The tunnel settings are invalid."],
      [{ url: 42 }, "The tunnel settings are invalid."],
      [{ webSocket: "ws" }, "The tunnel settings are invalid."],
    ];
    for (const [over, lastError] of cases) {
      await expect(tunnel.configure({ ...cfg(edge), ...over } as TunnelConfig)).resolves.toBeUndefined();
      expect(tunnel.status).toEqual({ state: "error", code: TunnelStatusCode.INVALID_CONFIG, lastError });
    }
    await expect(tunnel.configure(null as unknown as TunnelConfig)).resolves.toBeUndefined();
    expect(tunnel.status.code).toBe(TunnelStatusCode.INVALID_CONFIG);
    expect(edge.upgrades).toHaveLength(0);
    expect(logs.filter(([l, m]) => l === "error" && m.startsWith("INVALID_CONFIG: "))).toHaveLength(7);
    expect(logs.some(([, m]) => m.includes(TOKEN) || m.includes("notes") || m.includes("yes") || m.includes("42"))).toBe(false);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
  });

  it("disabled, or enabled with an empty token, is off whatever the other fields hold (name or token undefined, a bad url)", async () => {
    const edge = await startEdge();
    const { tunnel, history, logs } = tunnelOn(edge);
    for (const over of [{ name: undefined }, { token: undefined }, { token: null, name: 7, url: 42 }]) {
      await expect(tunnel.configure({ ...cfg(edge), enabled: false, ...over } as unknown as TunnelConfig)).resolves.toBeUndefined();
      expect(tunnel.status).toEqual({ state: "off" });
    }
    for (const over of [{ name: undefined }, { name: 7, url: 42, webSocket: "ws" }]) {
      await expect(tunnel.configure({ ...cfg(edge), token: "  ", ...over } as unknown as TunnelConfig)).resolves.toBeUndefined();
      expect(tunnel.status).toEqual({ state: "off" });
    }
    expect(history).toEqual([{ state: "off" }]);
    expect(logs.filter(([l]) => l === "error")).toEqual([]);
    expect(edge.upgrades).toHaveLength(0);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await tunnel.configure({ ...cfg(edge), enabled: false, token: undefined } as unknown as TunnelConfig);
    expect(tunnel.status).toEqual({ state: "off" });
    await waitFor(() => edge.connections.length === 0);
  });

  it("an invalid config while online stops the connection", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await tunnel.configure(cfg(edge, { name: undefined as unknown as string }));
    expect(tunnel.status).toEqual({ state: "error", code: TunnelStatusCode.INVALID_CONFIG, lastError: "The service name is not set." });
    expect(edge.appLog.map((m) => (m.mcpwarp as { op: string }).op)).toEqual(["register", "unregister"]);
    await waitFor(() => edge.connections.length === 0);
  });

  it("a server drain while online stays online: the replaced connection's end is no disconnect", async () => {
    const edge = await startEdge();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    cleanups.push(() => release());
    let calls = 0;
    const { tunnel, history, logs } = tunnelOn(edge, {
      handler: async () => {
        calls++;
        await gate;
        return new Response("late");
      },
    });
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const oldConn = edge.connections[0]!;
    // An open request (an SSE stream, typically) keeps the old connection up past the replacement's registration.
    const inflight = edge.fetch(edge.publicUrl("notes")).catch((e: Error) => e);
    await waitFor(() => calls === 1);
    await edge.drain({ conn: oldConn, deadlineMs: 10000 });
    await waitFor(() => logs.filter(([, m]) => m.startsWith("online at ")).length === 2);
    release();
    await inflight;
    await waitFor(() => edge.connections.length === 1 && logs.some(([l, m]) => l === "info" && m.startsWith("a replaced connection ended")));
    expect(tunnel.status).toEqual({ state: "online", url: edge.publicUrl("notes") });
    expect(history.map((s) => s.state)).toEqual(["off", "connecting", "online"]);
  });

  it("a retry of a drain replacement while online leaves the status alone but still reaches base.onRetry", async () => {
    const edge = await startEdge();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    cleanups.push(() => release());
    let calls = 0;
    const retries: string[] = [];
    const { tunnel, logs } = tunnelOn(edge, {
      onRetry: (i) => retries.push(i.reason),
      handler: async () => {
        calls++;
        await gate;
        return new Response("late");
      },
    });
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const oldConn = edge.connections[0]!;
    const inflight = edge.fetch(edge.publicUrl("notes")).catch((e: Error) => e);
    await waitFor(() => calls === 1);
    edge.failRegister("notes", { code: "INTERNAL" });
    await edge.drain({ conn: oldConn, deadlineMs: 10000 });
    await waitFor(() => retries.length === 1);
    expect(tunnel.status).toEqual({ state: "online", url: edge.publicUrl("notes") });
    expect(logs.some(([l, m]) => l === "warn" && m.startsWith("retry #1"))).toBe(true);
    release();
    await inflight;
  });

  it("the error event: only mcpwarp errors while connecting change the status; a handler TypeError is no INVALID_CONFIG; disabled keeps its code", async () => {
    const edge = await startEdge();
    const pending = emittingHandle("registering");
    exposeHook.fn = () => Promise.resolve(pending.handle);
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => pending.wired());
    pending.emit("error", new TypeError("handler did not return a Response"));
    expect(tunnel.status).toEqual({ state: "connecting" });
    pending.emit("error", new McpwarpError("INTERNAL", "register failed"));
    expect(tunnel.status).toEqual({ state: "connecting", code: TunnelStatusCode.RETRYING, lastError: "mcpwarp couldn't register the service; retrying." });
    await tunnel.dispose();

    const disabled = emittingHandle("disabled");
    exposeHook.fn = () => Promise.resolve(disabled.handle);
    const second = tunnelOn(edge);
    await second.tunnel.configure(cfg(edge));
    await waitFor(() => second.tunnel.status.state === "disabled");
    disabled.emit("error", new McpwarpError("TOO_MANY_SERVICES", "too many services"));
    expect(second.tunnel.status).toEqual({ state: "disabled", code: TunnelStatusCode.DISABLED, lastError: "This service is disabled in the mcpwarp dashboard." });
    expect(second.logs.some(([l, m]) => l === "warn" && m.startsWith("TOO_MANY_SERVICES: "))).toBe(true);
  });

  it("a register refused after an earlier success is fatal: state error with its code, the connection closed", async () => {
    const edge = await startEdge();
    const { tunnel, history } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    edge.failRegister("notes", { code: "UNAUTHENTICATED" });
    edge.closeConnection({ wsCode: 1006 });
    await waitFor(() => tunnel.status.state === "error");
    expect(tunnel.status).toEqual({ state: "error", code: TunnelStatusCode.UNAUTHORIZED, lastError: "The token was rejected." });
    expect(history).toContainEqual({ state: "connecting", code: TunnelStatusCode.DISCONNECTED, lastError: "Connection lost; retrying." });
    await waitFor(() => edge.connections.length === 0);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
  });

  it("register CONFLICT and app UNSUPPORTED_VERSION have their own codes", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    edge.failRegister("notes", { code: "CONFLICT" });
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "error");
    expect(tunnel.status).toEqual({ state: "error", code: TunnelStatusCode.CONFLICT, lastError: "The name is in use by another server." });
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await edge.appError("UNSUPPORTED_VERSION", "v1 is gone");
    await waitFor(() => tunnel.status.state === "error");
    expect(tunnel.status).toEqual({
      state: "error",
      code: TunnelStatusCode.UNSUPPORTED_VERSION,
      lastError: "This SDK version is no longer supported; please update.",
    });
  });

  it("an HTTP 401 at the upgrade (reported via upgradeFailure) is UNAUTHORIZED, a 403 or 404 DISCONNECTED_FATAL", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    const cases = [
      [401, TunnelStatusCode.UNAUTHORIZED],
      [403, TunnelStatusCode.DISCONNECTED_FATAL],
      [404, TunnelStatusCode.DISCONNECTED_FATAL],
    ] as const;
    for (const [status, code] of cases) {
      edge.rejectNextUpgrade(status);
      await tunnel.configure(cfg(edge));
      await waitFor(() => tunnel.status.state === "error");
      expect(tunnel.status.code).toBe(code);
      await tunnel.stop();
    }
    expect(edge.connections).toHaveLength(0);
  });

  it("createTunnel validates integration, agent and handler/target up front", () => {
    const handler = () => new Response();
    expect(() => createTunnel({ integration: "nope", handler })).toThrow(/RFC 4122/);
    expect(() => createTunnel({ integration: INTEGRATION } as TunnelOptions)).toThrow(/exactly one/);
    expect(() => createTunnel({ integration: INTEGRATION, handler, target: "http://x" } as unknown as TunnelOptions)).toThrow(/exactly one/);
    expect(() => createTunnel({ integration: INTEGRATION, handler, agent: { runtime: "a\nb" } })).toThrow(/agent\.runtime/);
  });

  it("disabled from the dashboard maps to disabled, enable back to online", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    await edge.disable("notes");
    await waitFor(() => tunnel.status.state === "disabled");
    expect(tunnel.status.code).toBe(TunnelStatusCode.DISABLED);
    await edge.enable("notes");
    await waitFor(() => tunnel.status.state === "online");
    expect(tunnel.status).toEqual({ state: "online", url: edge.publicUrl("notes") });
  });

  it("expose() resolving disabled maps to disabled", async () => {
    const edge = await startEdge();
    await edge.disable("notes");
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "disabled");
    expect(tunnel.status).toEqual({ state: "disabled", code: TunnelStatusCode.DISABLED, lastError: "This service is disabled in the mcpwarp dashboard." });
  });

  it("a lost connection goes connecting with code DISCONNECTED, then online again", async () => {
    const edge = await startEdge();
    const { tunnel, history } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    edge.closeConnection({ wsCode: 1006 });
    await waitFor(() => history.length >= 4 && tunnel.status.state === "online");
    expect(history[3]).toEqual({ state: "connecting", code: TunnelStatusCode.DISCONNECTED, lastError: "Connection lost; retrying." });
  });

  it("onRetry reasons set code RETRYING without changing state, log the reason and reach base.onRetry", async () => {
    const edge = await startEdge();
    const retries: string[] = [];
    const { tunnel, logs } = tunnelOn(edge, { onRetry: (i) => retries.push(i.reason) });
    edge.failRegister("notes", { code: "INTERNAL" });
    await tunnel.configure(cfg(edge));
    await waitFor(() => retries.length === 1);
    expect(tunnel.status).toEqual({ state: "connecting", code: TunnelStatusCode.RETRYING, lastError: "mcpwarp couldn't register the service; retrying." });
    expect(logs.some(([l, m]) => l === "warn" && m.includes("INTERNAL"))).toBe(true);
    await waitFor(() => tunnel.status.state === "online", 4000);
  });

  it("a failed dial is RETRYING; redials after a lost connection stay DISCONNECTED", async () => {
    const edge = await startEdge();
    const retries: string[] = [];
    const { tunnel, history } = tunnelOn(edge, { onRetry: (i) => retries.push(i.reason) });
    edge.rejectNextUpgrade(503);
    await tunnel.configure(cfg(edge));
    await waitFor(() => retries.length === 1);
    // The redial can be online before the poll sees RETRYING.
    expect(history).toContainEqual({ state: "connecting", code: TunnelStatusCode.RETRYING, lastError: "Can't reach mcpwarp; retrying." });
    await waitFor(() => tunnel.status.state === "online");
    const n = history.length;
    edge.rejectNextUpgrade(503);
    edge.closeConnection({ wsCode: 1006 });
    await waitFor(() => retries.length >= 3);
    await waitFor(() => tunnel.status.state === "online");
    expect(history.slice(n)).toEqual([
      { state: "connecting", code: TunnelStatusCode.DISCONNECTED, lastError: "Connection lost; retrying." },
      { state: "online", url: edge.publicUrl("notes") },
    ]);
  });

  it("a webSocket in the config overrides the options' one; a different factory restarts, the same one does not", async () => {
    const edge = await startEdge();
    const dialsA: WebSocketLike[] = [];
    const dialsB: WebSocketLike[] = [];
    const a = wrappedSockets(edge.webSocket, dialsA);
    const b = wrappedSockets(edge.webSocket, dialsB);
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge, { webSocket: a }));
    await waitFor(() => tunnel.status.state === "online");
    expect(dialsA).toHaveLength(1);
    const toB = tunnel.configure(cfg(edge, { webSocket: b }));
    await toB;
    await waitFor(() => tunnel.status.state === "online" && dialsB.length === 1);
    // An unchanged config returns the earlier apply and queues nothing.
    expect(tunnel.configure(cfg(edge, { webSocket: b }))).toBe(toB);
    expect(dialsA).toHaveLength(1);
    expect(dialsB).toHaveLength(1);
    expect(edge.upgrades).toHaveLength(2);
    expect(edge.appLog.map((m) => (m.mcpwarp as { op: string }).op)).toEqual(["register", "unregister", "register"]);
  });

  it("webSocket equality uses the effective factory: the options' one or undefined is no change, dropping an override restarts on the options' one", async () => {
    const edge = await startEdge();
    const dialsB: WebSocketLike[] = [];
    const b = wrappedSockets(edge.webSocket, dialsB);
    const { tunnel } = tunnelOn(edge);
    const first = tunnel.configure(cfg(edge));
    await first;
    await waitFor(() => tunnel.status.state === "online");
    expect(tunnel.configure(cfg(edge, { webSocket: edge.webSocket }))).toBe(first);
    expect(tunnel.configure(cfg(edge, { webSocket: undefined }))).toBe(first);
    expect(edge.upgrades).toHaveLength(1);
    await tunnel.configure(cfg(edge, { webSocket: b }));
    await waitFor(() => tunnel.status.state === "online" && dialsB.length === 1);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online" && edge.upgrades.length === 3);
    expect(dialsB).toHaveLength(1);
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
      await tunnel.dispose();
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

  it("dispose() is final: a final off, listeners cleared, configure()/stop() no-ops; its timer is cleared", async () => {
    const edge = await startEdge();
    const { tunnel, history } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const setSpy = vi.spyOn(globalThis, "setTimeout");
    const clearSpy = vi.spyOn(globalThis, "clearTimeout");
    try {
      await tunnel.dispose({ timeoutMs: 4321 });
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

  it("dispose() is bounded by timeoutMs when the drain hangs", async () => {
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
    await tunnel.dispose({ timeoutMs: 150 });
    const took = Date.now() - started;
    release();
    await inflight;
    expect(took).toBeGreaterThanOrEqual(140);
    expect(took).toBeLessThan(1000);
    expect(tunnel.status).toEqual({ state: "off" });
  });

  it("the deprecated close() is dispose(): same promise, final", async () => {
    const edge = await startEdge();
    const { tunnel } = tunnelOn(edge);
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
    const closed = tunnel.close({ timeoutMs: 1000 });
    expect(tunnel.dispose()).toBe(closed);
    await closed;
    await tunnel.configure(cfg(edge, { name: "other" }));
    expect(tunnel.status).toEqual({ state: "off" });
    expect(edge.upgrades).toHaveLength(1);
  });

  it("the deprecated TunnelBase type still types createTunnel()'s options", async () => {
    const edge = await startEdge();
    const options: TunnelBase = { integration: INTEGRATION, webSocket: edge.webSocket, handler: () => new Response("ok") };
    const tunnel = createTunnel(options);
    cleanups.push(() => tunnel.dispose());
    await tunnel.configure(cfg(edge));
    await waitFor(() => tunnel.status.state === "online");
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
      await tunnel.dispose();
      expect(history.map((s) => s.state)).toEqual(["off", "connecting", "online", "connecting", "online", "connecting", "off"]);
      await waitFor(() => edge.connections.length === 0);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
