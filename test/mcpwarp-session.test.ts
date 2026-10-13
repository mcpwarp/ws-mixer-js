/** The registration state machine (src/mcpwarp/session.ts) against a fake clock and a recording sendApp. */
import { describe, expect, it } from "vitest";
import { Session } from "../src/mcpwarp/session.js";
import { McpwarpError } from "../src/mcpwarp/errors.js";

class FakeClock {
  now = 0;
  private timers: Array<{ at: number; fn: () => void; id: number }> = [];
  private nextId = 1;
  setTimeout = (fn: () => void, ms: number): unknown => {
    const id = this.nextId++;
    this.timers.push({ at: this.now + ms, fn, id });
    return id;
  };
  clearTimeout = (h: unknown): void => {
    this.timers = this.timers.filter((t) => t.id !== h);
  };
  advance(ms: number): void {
    const until = this.now + ms;
    for (;;) {
      this.timers.sort((a, b) => a.at - b.at);
      const next = this.timers[0];
      if (!next || next.at > until) break;
      this.timers.shift();
      this.now = next.at;
      next.fn();
    }
    this.now = until;
  }
  pending(): number {
    return this.timers.length;
  }
}

function make(opts: { sendFails?: boolean } = {}) {
  const clock = new FakeClock();
  const sent: Array<Record<string, any>> = [];
  const events: Array<[string, unknown]> = [];
  const fatals: McpwarpError[] = [];
  const session = new Session({
    name: "anki",
    kind: "http",
    sendApp: (body) => {
      sent.push((body as { mcpwarp: Record<string, any> }).mcpwarp);
      return opts.sendFails ? Promise.reject(new Error("no conn")) : Promise.resolve();
    },
    setTimeout: clock.setTimeout,
    clearTimeout: clock.clearTimeout,
    emit: (event, payload) => events.push([event, payload]),
    fatal: (err) => fatals.push(err),
  });
  const ops = () => sent.map((m) => m.op);
  const of = (name: string) => events.filter(([e]) => e === name).map(([, p]) => p);
  const app = (env: Record<string, unknown>) => session.onApp({ mcpwarp: { v: 1, ...env } });
  const registered = (created = false) => app({ op: "registered", services: [{ name: "anki", id: "srv_anki", url: "https://anki-u.x/mcp", created }] });
  const regError = (code: string, extra: Record<string, unknown> = {}) => app({ op: "registered", errors: [{ name: "anki", code, message: code.toLowerCase(), ...extra }] });
  return { clock, sent, events, fatals, session, ops, of, app, registered, regError };
}

describe("Session", () => {
  it("registers on every welcome, emits registered with the created flag each time", () => {
    const t = make();
    expect(t.session.state).toBe("connecting");
    t.session.onWelcome();
    expect(t.session.state).toBe("registering");
    expect(t.sent).toEqual([{ v: 1, op: "register", services: [{ name: "anki", kind: "http" }] }]);
    t.registered(true);
    expect(t.session.state).toBe("registered");
    expect(t.session.url).toBe("https://anki-u.x/mcp");
    expect(t.session.serverId).toBe("srv_anki");
    t.session.onDisconnect();
    expect(t.session.state).toBe("connecting");
    t.session.onWelcome();
    t.registered(false);
    expect(t.ops()).toEqual(["register", "register"]);
    expect(t.of("registered")).toEqual([
      { url: "https://anki-u.x/mcp", serverId: "srv_anki", created: true },
      { url: "https://anki-u.x/mcp", serverId: "srv_anki", created: false },
    ]);
    expect(t.of("state")).toEqual(["registering", "registered", "connecting", "registering", "registered"]);
  });

  it("ignores registered entries and errors for other names, and non-mcpwarp bodies", () => {
    const t = make();
    t.session.onWelcome();
    t.app({ op: "registered", services: [{ name: "other", id: "x" }], errors: [{ name: "other2", code: "CONFLICT" }] });
    t.session.onApp({ hello: 1 });
    expect(t.session.state).toBe("registering");
    expect(t.fatals).toHaveLength(0);
  });

  it("SERVER_DISABLED -> disabled with the error's id", () => {
    const t = make();
    t.session.onWelcome();
    t.regError("SERVER_DISABLED", { id: "srv_9" });
    expect(t.session.state).toBe("disabled");
    expect(t.of("disabled")).toEqual([{ serverId: "srv_9", reason: "disabled" }]);
  });

  it("transient register errors retry with backoff 1/2/4 s, capped at 30 s, and reset after success", () => {
    const t = make();
    t.session.onWelcome();
    const delays: number[] = [];
    for (let i = 0; i < 7; i++) {
      t.regError("UNAVAILABLE");
      const before = t.sent.length;
      const start = t.clock.now;
      while (t.sent.length === before) t.clock.advance(100);
      delays.push(t.clock.now - start);
    }
    expect(delays).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000]);
    expect(t.of("error").every((e) => e instanceof McpwarpError && !e.fatal)).toBe(true);
    expect(t.session.state).toBe("registering");
    t.registered();
    t.regError("TIMEOUT");
    const before = t.sent.length;
    t.clock.advance(1000);
    expect(t.sent.length).toBe(before + 1);
  });

  it("a disconnect cancels a pending register retry", () => {
    const t = make();
    t.session.onWelcome();
    t.regError("INTERNAL");
    t.session.onDisconnect();
    t.clock.advance(5000);
    expect(t.ops()).toEqual(["register"]);
  });

  it("a non-transient register error is fatal before the first registration, non-fatal after", () => {
    const first = make();
    first.session.onWelcome();
    first.regError("QUOTA_EXCEEDED");
    expect(first.fatals).toHaveLength(1);
    expect(first.fatals[0]).toMatchObject({ code: "QUOTA_EXCEEDED", fatal: true, serviceName: "anki" });

    const later = make();
    later.session.onWelcome();
    later.registered();
    later.session.onDisconnect();
    later.session.onWelcome();
    later.regError("CONFLICT");
    expect(later.fatals).toHaveLength(0);
    expect(later.of("error")).toHaveLength(1);
    later.clock.advance(60_000);
    expect(later.ops()).toEqual(["register", "register"]); // no retry until the next welcome
  });

  it("disable (matching id or before any id) -> disabled; enable re-registers", () => {
    const t = make();
    t.session.onWelcome();
    t.registered();
    t.app({ op: "disable", id: "srv_other" });
    expect(t.session.state).toBe("registered");
    t.app({ op: "disable", id: "srv_anki", reason: "disabled" });
    expect(t.session.state).toBe("disabled");
    t.app({ op: "enable", id: "srv_anki", name: "other" });
    expect(t.session.state).toBe("disabled");
    t.app({ op: "enable", id: "srv_anki", name: "anki" });
    expect(t.of("enabled")).toEqual([{ serverId: "srv_anki" }]);
    expect(t.session.state).toBe("registering");
    expect(t.ops()).toEqual(["register", "register"]);
    t.registered();
    expect(t.session.state).toBe("registered");

    const fresh = make();
    fresh.session.onWelcome();
    fresh.app({ op: "disable", id: "srv_x" });
    expect(fresh.session.state).toBe("disabled");
    expect(fresh.session.serverId).toBe("srv_x");
  });

  it("OVERLOADED re-sends the outstanding register once after 1 s", () => {
    const t = make();
    t.session.onWelcome();
    t.app({ op: "error", code: "OVERLOADED", message: "busy" });
    t.app({ op: "error", code: "OVERLOADED", message: "busy" });
    t.clock.advance(999);
    expect(t.ops()).toEqual(["register"]);
    t.clock.advance(1);
    expect(t.ops()).toEqual(["register", "register"]);
    t.registered();
    t.app({ op: "error", code: "OVERLOADED", message: "busy" });
    t.clock.advance(5000);
    expect(t.ops()).toEqual(["register", "register"]); // nothing outstanding
  });

  it("UNSUPPORTED_VERSION is fatal; CONNECTION_LIMIT, TOO_MANY_SERVICES, UNKNOWN_OP are non-fatal errors", () => {
    const t = make();
    t.session.onWelcome();
    for (const code of ["CONNECTION_LIMIT", "TOO_MANY_SERVICES", "UNKNOWN_OP", "BAD_REQUEST"]) t.app({ op: "error", code });
    expect(t.fatals).toHaveLength(0);
    expect(t.of("error").map((e) => (e as McpwarpError).code)).toEqual(["CONNECTION_LIMIT", "TOO_MANY_SERVICES", "UNKNOWN_OP", "BAD_REQUEST"]);
    t.app({ op: "error", code: "UNSUPPORTED_VERSION" });
    expect(t.fatals.map((e) => e.code)).toEqual(["UNSUPPORTED_VERSION"]);
  });

  it("a malformed envelope is a non-fatal error", () => {
    const t = make();
    t.session.onWelcome();
    t.app({ op: "disable" });
    expect(t.of("error")).toHaveLength(1);
    expect((t.of("error")[0] as McpwarpError).code).toBe("MALFORMED_ENVELOPE");
  });

  it("beginClose: sends unregister and resolves on unregistered for our name", async () => {
    const t = make();
    t.session.onWelcome();
    t.registered();
    let done = false;
    const p = t.session.beginClose().then(() => (done = true));
    expect(t.session.state).toBe("closing");
    expect(t.ops()).toEqual(["register", "unregister"]);
    t.app({ op: "unregistered", services: [{ name: "other", id: "x" }] });
    await Promise.resolve();
    expect(done).toBe(false);
    t.app({ op: "unregistered", services: [{ name: "anki", id: "srv_anki" }] });
    await p;
    expect(t.clock.pending()).toBe(0);
    t.session.markClosed();
    expect(t.session.state).toBe("closed");
  });

  it("beginClose: resolves after the 2 s budget when unregistered never comes", async () => {
    const t = make();
    t.session.onWelcome();
    t.registered();
    let done = false;
    const p = t.session.beginClose().then(() => (done = true));
    t.clock.advance(1999);
    await Promise.resolve();
    expect(done).toBe(false);
    t.clock.advance(1);
    await p;
  });

  it("conflict for our name or id -> conflict state, event, timers cancelled; never registers again, across reconnects and enable", async () => {
    const t = make();
    t.session.onWelcome();
    t.registered();
    t.app({ op: "conflict", id: "srv_other", name: "other", reason: "taken_over" });
    expect(t.session.state).toBe("registered");
    t.regError("UNAVAILABLE"); // arms a retry timer
    t.app({ op: "conflict", id: "srv_anki", name: "anki", reason: "taken_over" });
    expect(t.session.state).toBe("conflict");
    expect(t.session.takenOver).toBe(true);
    expect(t.of("conflict")).toEqual([{ serverId: "srv_anki", name: "anki", reason: "taken_over" }]);
    expect(t.clock.pending()).toBe(0);
    t.app({ op: "conflict", id: "srv_anki", name: "anki", reason: "taken_over" });
    expect(t.of("conflict")).toHaveLength(1);

    t.session.onDisconnect();
    expect(t.session.state).toBe("conflict");
    t.session.onWelcome();
    t.app({ op: "enable", id: "srv_anki", name: "anki" });
    t.app({ op: "disable", id: "srv_anki" });
    t.registered();
    t.app({ op: "error", code: "OVERLOADED" });
    t.clock.advance(60_000);
    expect(t.session.state).toBe("conflict");
    expect(t.ops()).toEqual(["register"]);
    expect(t.of("enabled")).toEqual([]);
    expect(t.of("disabled")).toEqual([]);

    await t.session.beginClose();
    expect(t.ops()).toEqual(["register"]); // no unregister: the name is not ours
    t.session.markClosed();
    expect(t.of("state")).toEqual(["registering", "registered", "conflict", "closing", "closed"]);
  });

  it("conflict matches by id when the name differs", () => {
    const t = make();
    t.session.onWelcome();
    t.registered();
    t.app({ op: "conflict", id: "srv_anki", name: "renamed" });
    expect(t.session.state).toBe("conflict");
    expect(t.of("conflict")).toEqual([{ serverId: "srv_anki", name: "renamed", reason: "taken_over" }]);
  });

  it("beginClose: sends nothing while disconnected, and registrations after closing are ignored", async () => {
    const t = make();
    t.session.onWelcome();
    t.session.onDisconnect();
    await t.session.beginClose();
    expect(t.ops()).toEqual(["register"]);
    t.session.onWelcome();
    t.registered();
    expect(t.session.state).toBe("closing");
    expect(t.ops()).toEqual(["register"]);
  });
});
