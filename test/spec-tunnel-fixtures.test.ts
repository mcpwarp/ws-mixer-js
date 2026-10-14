/**
 * Replays spec/fixtures/mcpwarp/tunnel/*.json (MCPWARP.md section 10.4, the
 * fixture README's runner contract) against createTunnel() over a scripted
 * fake edge and fake timers, and compares every status snapshot exactly.
 */
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { encodeControl, parseControl, type ControlMessage } from "../src/control.js";
import { decodeFrame, encodeData, FrameType } from "../src/frame.js";
import { createTunnel, type Tunnel, type TunnelConfig, type TunnelStatus } from "../src/mcpwarp/index.js";
import { SUBPROTOCOL, type WebSocketLike } from "../src/socket.js";
import { resolveSpecDir } from "./helpers/spec-dir.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const specResult = resolveSpecDir(here);
const fixtureDir = path.join(specResult.dir, "fixtures/mcpwarp/tunnel");

type Event = Record<string, any>;
interface TunnelFixture {
  description: string;
  config: Record<string, unknown>;
  events: Event[];
  expect: TunnelStatus[];
  expectDials: number;
}

const INTEGRATION = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10";

/** One dial, scripted by the fixture: pending until `connected` / `dial_failed`. */
class EdgeSocket implements WebSocketLike {
  readyState = 0;
  readonly protocol = SUBPROTOCOL;
  binaryType = "arraybuffer";
  bufferedAmount = 0;
  /** The service name this connection registered. */
  name: string | undefined;
  held = false;
  private readonly listeners = new Map<string, Set<(ev: any) => void>>();

  addEventListener(type: string, listener: (ev: any) => void): void {
    let set = this.listeners.get(type);
    if (!set) this.listeners.set(type, (set = new Set()));
    set.add(listener);
  }

  removeEventListener(type: string, listener: (ev: any) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  send(data: Uint8Array): void {
    if (this.readyState !== 1) return;
    const frame = decodeFrame(data);
    if (frame.streamId !== 0 || frame.type !== FrameType.DATA) return;
    const msg = parseControl(frame.payload);
    if (msg.t === "ping") {
      queueMicrotask(() => this.control({ t: "pong", id: msg.id } as ControlMessage));
      return;
    }
    if (msg.t !== "app") return;
    const env = (msg.body as { mcpwarp?: { op?: string; services?: Array<{ name: string }> } }).mcpwarp;
    if (env?.op === "register") this.name = env.services?.[0]?.name;
    if (env?.op === "unregister" && !this.held) {
      const name = env.services?.[0]?.name;
      queueMicrotask(() => this.app({ op: "unregistered", services: [{ name, id: `srv_${name}` }] }));
    }
  }

  close(code?: number, reason?: string): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    queueMicrotask(() => this.dispatch("close", { code: code ?? 1000, reason: reason ?? "" }));
  }

  open(): void {
    this.readyState = 1;
    this.dispatch("open", {});
  }

  failDial(): void {
    this.readyState = 3;
    this.dispatch("error", { message: "connection refused" });
    this.dispatch("close", { code: 1006, reason: "" });
  }

  serverClose(code: number): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatch("close", { code, reason: "" });
  }

  control(msg: ControlMessage): void {
    if (this.readyState !== 1) return;
    this.dispatch("message", { data: encodeData(0, encodeControl(msg)).slice().buffer });
  }

  app(mcpwarp: Record<string, unknown>): void {
    this.control({ t: "app", body: { mcpwarp: { v: 1, ...mcpwarp } } } as ControlMessage);
  }

  private dispatch(type: string, ev: unknown): void {
    for (const l of [...(this.listeners.get(type) ?? [])]) l(ev);
  }
}

/** Lets the SDK run until nothing is runnable without moving the clock. */
async function settle(): Promise<void> {
  for (let i = 0; i < 10; i++) {
    await new Promise<void>((r) => setImmediate(r));
    await vi.advanceTimersByTimeAsync(0);
  }
}

function merged(base: Record<string, unknown>, over: Record<string, unknown>): TunnelConfig {
  const out: Record<string, unknown> = { ...base };
  for (const [k, v] of Object.entries(over)) {
    if (v === null) delete out[k];
    else out[k] = v;
  }
  return out as unknown as TunnelConfig;
}

async function run(fx: TunnelFixture): Promise<{ snapshots: TunnelStatus[]; dials: number; unsettled: number }> {
  const dials: EdgeSocket[] = [];
  const serverIds = new Map<string, string>();
  const retries: Array<{ delayMs: number; reason: string }> = [];
  const tunnel: Tunnel = createTunnel({
    integration: INTEGRATION,
    handler: () => new Response("ok"),
    webSocket: () => {
      const s = new EdgeSocket();
      dials.push(s);
      return s;
    },
    onRetry: (info) => retries.push(info),
  });
  const snapshots: TunnelStatus[] = [];
  tunnel.subscribe((s) => snapshots.push(s));
  let unsettled = 0;
  const track = (p: Promise<void>): void => {
    unsettled++;
    void p.then(() => unsettled--);
  };
  const sock = (payload: { conn?: number }): EdgeSocket => {
    const s = payload.conn !== undefined ? dials[payload.conn - 1] : dials.at(-1);
    if (!s) throw new Error(`no dial #${payload.conn ?? dials.length}`);
    return s;
  };
  const nameOf = (s: EdgeSocket): string => s.name ?? String(fx.config.name);
  const serverId = (s: EdgeSocket): string => serverIds.get(nameOf(s)) ?? `srv_${nameOf(s)}`;
  const call = (ev: Event): void => {
    const [kind, payload] = Object.entries(ev)[0]!;
    if (kind === "configure") track(tunnel.configure(merged(fx.config, payload)));
    else if (kind === "stop") track(tunnel.stop());
    else throw new Error(`burst: unexpected ${kind}`);
  };

  for (const ev of fx.events) {
    const [kind, payload] = Object.entries(ev)[0] as [string, any];
    switch (kind) {
      case "configure":
      case "stop":
        call(ev);
        break;
      case "burst":
        for (const inner of payload as Event[]) call(inner);
        break;
      case "dispose":
        track(payload.timeoutMs === undefined ? tunnel.dispose() : tunnel.dispose({ timeoutMs: payload.timeoutMs }));
        break;
      case "subscribe":
        tunnel.subscribe((s) => snapshots.push(s))();
        break;
      case "connected":
        sock(payload).open();
        break;
      case "dial_failed":
        sock(payload).failDial();
        break;
      case "welcome":
        sock(payload).control({
          t: "welcome",
          v: 1,
          session: "fixture",
          window: 262144,
          max_streams: 100,
          ping_interval: 3_600_000,
          ping_timeout: 7_200_000,
        } as ControlMessage);
        break;
      case "registered": {
        const s = sock(payload);
        for (const svc of payload.services) serverIds.set(nameOf(s), svc.id);
        s.app({ op: "registered", services: payload.services.map((svc: object) => ({ name: nameOf(s), ...svc })) });
        break;
      }
      case "register_error": {
        const s = sock(payload);
        if (payload.id !== undefined) serverIds.set(nameOf(s), payload.id);
        const entry: Record<string, unknown> = { name: nameOf(s), code: payload.code };
        if (payload.message !== undefined) entry.message = payload.message;
        if (payload.id !== undefined) entry.id = payload.id;
        s.app({ op: "registered", errors: [entry] });
        break;
      }
      case "app_error": {
        const s = sock(payload);
        s.app({ op: "error", code: payload.code, ...(payload.message !== undefined ? { message: payload.message } : {}) });
        break;
      }
      case "disabled": {
        const s = sock(payload);
        s.app({ op: "disable", id: serverId(s), ...(payload.reason !== undefined ? { reason: payload.reason } : {}) });
        break;
      }
      case "enabled": {
        const s = sock(payload);
        s.app({ op: "enable", id: serverId(s), name: nameOf(s) });
        break;
      }
      case "conflict": {
        const s = sock(payload);
        s.app({ op: "conflict", id: serverId(s), name: nameOf(s), ...(payload.reason !== undefined ? { reason: payload.reason } : {}) });
        break;
      }
      case "disconnect":
        sock(payload).serverClose(payload.wsCode ?? 1006);
        break;
      case "drain":
        sock(payload).control({ t: "drain", reason: payload.reason ?? "rollout", last_stream_id: 0 } as ControlMessage);
        break;
      case "hold":
        sock(payload).held = true;
        break;
      case "retry": {
        const pending = retries.splice(0);
        const last = pending.at(-1);
        if (!last) throw new Error(`retry (${payload.cause}): no retry was scheduled`);
        expect(pending, `retry (${payload.cause}): exactly one scheduled`).toHaveLength(1);
        expect(last.delayMs, `retry (${payload.cause}) delay`).toBeLessThanOrEqual(payload.delayMs);
        if (payload.cause === "register" || payload.cause === "overloaded") expect(last.delayMs).toBe(payload.delayMs);
        // onRetry carries no kind; the reason tells the fixture's causes apart.
        const kindOk: Record<string, (reason: string) => boolean> = {
          drain: (r) => r === "drain",
          register: (r) => /^register failed: /.test(r) && !/^register failed: OVERLOADED: /.test(r),
          overloaded: (r) => /^register failed: OVERLOADED: /.test(r),
          dial: (r) => r !== "drain" && !r.startsWith("register failed"),
          lost: (r) => r !== "drain" && !r.startsWith("register failed"),
        };
        expect(kindOk[payload.cause], `unknown retry cause ${payload.cause}`).toBeDefined();
        expect(kindOk[payload.cause]!(last.reason), `retry (${payload.cause}) fired ${JSON.stringify(last.reason)}`).toBe(true);
        await vi.advanceTimersByTimeAsync(last.delayMs);
        break;
      }
      case "tick":
        retries.length = 0;
        await vi.advanceTimersByTimeAsync(payload.ms);
        break;
      default:
        throw new Error(`unknown fixture event ${kind}`);
    }
    await settle();
  }
  const result = { snapshots: [...snapshots], dials: dials.length, unsettled };
  void tunnel.dispose({ timeoutMs: 1 });
  await vi.advanceTimersByTimeAsync(60_000);
  return result;
}

describe("mcpwarp tunnel fixtures", () => {
  if (!specResult.available) {
    // CI fetches the spec at spec.pin, so a missing corpus there is a broken pin, not a local setup gap.
    if (process.env.CI) it("spec fixtures are required in CI", () => expect.fail(specResult.reason));
    else it.skip(`spec fixtures unavailable: ${specResult.reason}`, () => {});
    return;
  }
  const files = readdirSync(fixtureDir).filter((f) => f.endsWith(".json")).sort();

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });
    vi.spyOn(Math, "random").mockReturnValue(0.5);
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("loaded every tunnel fixture", () => {
    expect(files.length).toBeGreaterThanOrEqual(32);
  });

  for (const file of files) {
    const fx = JSON.parse(readFileSync(path.join(fixtureDir, file), "utf8")) as TunnelFixture;
    it(`${file}: ${fx.description}`, async () => {
      const { snapshots, dials, unsettled } = await run(fx);
      expect(snapshots).toStrictEqual(fx.expect);
      expect(dials, "dials").toBe(fx.expectDials);
      expect(unsettled, "configure/stop/dispose promises still pending at the end").toBe(0);
    });
  }
});
