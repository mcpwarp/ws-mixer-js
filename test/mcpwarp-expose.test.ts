/** expose() end to end against the in-process FakeEdge: handler and target adapters, lifecycle, errors, drain. */
import http from "node:http";
import { gzipSync } from "node:zlib";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { McpwarpError, expose, type ExposeHandle, type ExposeOptions } from "../src/mcpwarp/index.js";
import { FakeEdge } from "../src/testing/index.js";
import type { EdgeConnection } from "../src/testing/edge-conn.js";
import { SDK_VERSION } from "../src/client.js";
import { ErrorCode, WsMixerError } from "../src/errors.js";
import { StreamByteReader } from "../src/http1/reader.js";
import { readResponse } from "../src/http1/response.js";
import { latin1Encode } from "../src/http1/headers.js";

const INTEGRATION = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10";
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c();
});

async function startEdge() {
  const edge = new FakeEdge();
  await edge.start();
  cleanups.push(() => edge.stop());
  return edge;
}

async function exposeOn(edge: FakeEdge, opts: Partial<ExposeOptions> & ({ handler: ExposeOptions["handler"] } | { target: string | URL })): Promise<ExposeHandle> {
  const handle = await expose({
    url: edge.url,
    webSocket: edge.webSocket,
    token: "mcpwarp_pat_id_secret",
    name: "notes",
    integration: INTEGRATION,
    reconnect: { base: 10, cap: 50 },
    ...opts,
  } as ExposeOptions);
  cleanups.push(() => handle.close());
  return handle;
}

async function waitFor(cond: () => boolean, ms = 5000): Promise<void> {
  const until = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > until) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 5));
  }
}

/** Sends raw bytes as one request on a fresh edge stream and parses the reply. */
async function rawRequest(edge: FakeEdge, bytes: string): Promise<{ status: number; body: string }> {
  const conn = edge.connections[edge.connections.length - 1] as EdgeConnection;
  const stream = conn.openStream();
  await stream.write(latin1Encode(bytes));
  await stream.closeWrite();
  const { head, body } = await readResponse(new StreamByteReader(stream), "GET");
  let text = "";
  for await (const c of body) text += new TextDecoder().decode(c);
  return { status: head.status, body: text };
}

describe("expose({handler})", () => {
  it("resolves registered with the public URL; identity goes in the upgrade headers and hello.meta", async () => {
    const edge = await startEdge();
    const handle = await exposeOn(edge, { handler: () => new Response("ok") });
    expect(handle.url).toBe("https://notes-tester.mcpwarp.test/mcp");
    expect(handle.serverId).toBe("srv_notes");
    expect(handle.state).toBe("registered");
    const h = edge.upgrades[0]!.headers;
    expect(h["X-Mcpwarp-Integration"]).toBe(INTEGRATION);
    expect(Object.keys(h).filter((k) => k.toLowerCase() === "x-mcpwarp-client")).toEqual([]);
    expect(h.Authorization).toBe("Bearer mcpwarp_pat_id_secret");
    expect(edge.hellos[0]!.meta).toEqual({ client: "sdk-js", version: SDK_VERSION, os: `${process.platform}/${process.arch}`, integration: INTEGRATION });
    expect((await edge.waitForApp("register")).mcpwarp).toEqual({ v: 1, op: "register", services: [{ name: "notes", kind: "http" }] });
  });

  it("`agent` overrides the detected runtime/os in hello.agent, User-Agent and hello.meta.os", async () => {
    const edge = await startEdge();
    await exposeOn(edge, { handler: () => new Response("ok"), agent: { runtime: "zotero", os: "Darwin" } });
    expect(edge.hellos[0]!.agent).toEqual({ sdk: "ws-mixer-js", sdk_version: SDK_VERSION, runtime: "zotero", os: "Darwin" });
    expect(edge.hellos[0]!.meta).toMatchObject({ os: "Darwin" });
    expect(edge.upgrades[0]!.headers["User-Agent"]).toBe(`ws-mixer-js/${SDK_VERSION} (zotero Darwin)`);
  });

  it("a partial `agent` keeps the detected value for the other field", async () => {
    const edge = await startEdge();
    await exposeOn(edge, { handler: () => new Response("ok"), agent: { runtime: "zotero" } });
    expect(edge.hellos[0]!.agent).toMatchObject({ runtime: "zotero", os: `${process.platform}/${process.arch}` });
    expect(edge.upgrades[0]!.headers["User-Agent"]).toBe(`ws-mixer-js/${SDK_VERSION} (zotero ${process.platform}/${process.arch})`);
  });

  it("`agent` fields must be 1-64 printable ASCII; the error does not echo the value", async () => {
    const base = { token: "t", name: "ok", integration: INTEGRATION, handler: () => new Response() };
    const bad: unknown[] = ["zotero\r\nX-Evil: 1", "line\nfeed", "x".repeat(65), "", "Zotérо", 42, { a: 1 }];
    for (const k of ["runtime", "os"] as const) {
      for (const v of bad) {
        let thrown: unknown;
        try {
          expose({ ...base, agent: { [k]: v } } as never);
        } catch (e) {
          thrown = e;
        }
        expect(thrown).toBeInstanceOf(TypeError);
        expect((thrown as Error).message).toContain(`agent.${k}`);
        if (typeof v === "string" && v !== "") expect((thrown as Error).message).not.toContain(v);
      }
    }
    expect(() => expose({ ...base, agent: "zotero" } as never)).toThrow(TypeError);
    expect(() => expose({ ...base, agent: null } as never)).toThrow(TypeError);

    const edge = await startEdge();
    const max = "r".repeat(64);
    await exposeOn(edge, { handler: () => new Response("ok"), agent: { runtime: max, os: "WINNT" } });
    expect(edge.hellos[0]!.agent).toMatchObject({ runtime: max, os: "WINNT" });
  });

  it("a JS caller cannot override sdk/sdk_version through `agent`", async () => {
    const edge = await startEdge();
    await exposeOn(edge, { handler: () => new Response("ok"), agent: { sdk: "x", sdk_version: "9" } as never });
    expect(edge.hellos[0]!.agent).toMatchObject({ sdk: "ws-mixer-js", sdk_version: SDK_VERSION });
  });

  it("validates synchronously", () => {
    const base = { token: "t", integration: INTEGRATION, handler: () => new Response() };
    expect(() => expose({ ...base, name: "Bad_Name" })).toThrow(TypeError);
    expect(() => expose({ ...base, name: "a".repeat(31) })).toThrow(TypeError);
    expect(() => expose({ ...base, name: "ok", integration: "" })).toThrow(TypeError);
    expect(() => expose({ token: "t", name: "ok", handler: () => new Response() } as never)).toThrow(/`integration`.*required/);
    expect(() => expose({ ...base, name: "ok", integration: 42 as unknown as string })).toThrow(TypeError);
    for (const bad of ["i", "my-zotero-plugin", "0f6c2a8e4c1b4b7e9a4e2f1d9b7c3a10", "0f6c2a8e-4c1b-0b7e-9a4e-2f1d9b7c3a10", ` ${INTEGRATION}`]) {
      expect(() => expose({ ...base, name: "ok", integration: bad })).toThrow(TypeError);
      expect(() => expose({ ...base, name: "ok", integration: bad })).toThrow(/must be an RFC 4122 UUID.*silently ignores/);
    }
    const secret = "mcpwarp_pat_id_secret";
    let thrown: unknown;
    try {
      expose({ ...base, name: "ok", integration: secret });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(TypeError);
    expect((thrown as Error).message).not.toContain(secret);
    expect(() => expose({ token: "t", name: "ok", integration: INTEGRATION } as never)).toThrow(/exactly one/);
    expect(() => expose({ ...base, name: "ok", target: "http://x" } as never)).toThrow(/exactly one/);
  });

  it("an upper-case integration UUID (and the nil/max UUIDs) are accepted and sent lowercased", async () => {
    const edge = await startEdge();
    await exposeOn(edge, { handler: () => new Response(), integration: INTEGRATION.toUpperCase() });
    expect(edge.upgrades[0]!.headers["X-Mcpwarp-Integration"]).toBe(INTEGRATION);
    expect(edge.hellos[0]!.meta).toMatchObject({ integration: INTEGRATION });
    for (const id of ["00000000-0000-0000-0000-000000000000", "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"]) {
      const handle = await exposeOn(edge, { handler: () => new Response(), name: id[0] === "0" ? "nil" : "max", integration: id });
      expect(handle.state).toBe("registered");
    }
    expect(edge.hellos.map((h) => (h.meta as { integration: string }).integration)).toEqual([
      INTEGRATION,
      "00000000-0000-0000-0000-000000000000",
      "ffffffff-ffff-ffff-ffff-ffffffffffff",
    ]);
  });

  it("conflict: streams are RESET first, the conflict event fires, stray streams are RESET without reaching the handler, no register after a reconnect, close() skips unregister", async () => {
    const edge = await startEdge();
    let calls = 0;
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const handle = await exposeOn(edge, {
      handler: async () => {
        calls++;
        await gate;
        return new Response("late");
      },
    });
    const states: string[] = [];
    const conflicts: unknown[] = [];
    handle.on("conflict", (e) => {
      conflicts.push(e);
      states.push(handle.state);
    });

    const inflight = edge.fetch(edge.publicUrl("notes")).catch((e: Error) => e);
    await waitFor(() => calls === 1);
    await edge.conflict("notes");
    expect(await inflight).toBeInstanceOf(Error);
    await waitFor(() => conflicts.length === 1);
    release();
    expect(conflicts).toEqual([{ serverId: "srv_notes", name: "notes", reason: "taken_over" }]);
    expect(states).toEqual(["conflict"]);
    expect(handle.state).toBe("conflict");

    // A stray stream (the edge should route nothing here any more) is RESET with CANCEL.
    const conn = edge.connections[edge.connections.length - 1] as EdgeConnection;
    const stray = conn.openStream();
    await stray.write(latin1Encode("GET /mcp HTTP/1.1\r\nHost: notes-tester.mcpwarp.test\r\n\r\n")).catch(() => {});
    await stray.closeWrite().catch(() => {});
    expect(await stray.closed).toMatchObject({ kind: "reset", name: "CANCEL", byPeer: true });
    expect(calls).toBe(1);

    // A forced reconnect: the SDK dials and welcomes again but never re-registers.
    edge.closeConnection({ wsCode: 1006 });
    await waitFor(() => edge.hellos.length === 2 && edge.connections.length === 1);
    await new Promise((r) => setTimeout(r, 50));
    expect(handle.state).toBe("conflict");
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register"]);

    // And a drain hand-over does not either.
    await edge.drain({ deadlineMs: 25000 });
    await waitFor(() => edge.hellos.length === 3);
    await new Promise((r) => setTimeout(r, 50));
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register"]);

    const started = Date.now();
    await handle.close();
    expect(Date.now() - started).toBeLessThan(1000);
    expect(handle.state).toBe("closed");
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register"]);
  });

  it("the handler gets a web Request: public URL, method, headers without authorization/expect, body", async () => {
    const edge = await startEdge();
    const seen: Array<{ url: string; method: string; headers: Record<string, string>; body: string }> = [];
    await exposeOn(edge, {
      handler: async (req) => {
        const headers: Record<string, string> = {};
        req.headers.forEach((v, k) => (headers[k] = v));
        seen.push({ url: req.url, method: req.method, headers, body: await req.text() });
        return Response.json({ ok: true });
      },
    });
    const res = await edge.fetch(`${edge.publicUrl("notes")}?session=1`, {
      method: "POST",
      body: '{"jsonrpc":"2.0","id":1,"method":"ping"}',
      headers: { "content-type": "application/json", authorization: "Bearer user-token", expect: "100-continue", "mcp-session-id": "abc" },
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
    expect(seen[0]!.url).toBe("https://notes-tester.mcpwarp.test/mcp?session=1");
    expect(seen[0]!.method).toBe("POST");
    expect(seen[0]!.body).toBe('{"jsonrpc":"2.0","id":1,"method":"ping"}');
    expect(seen[0]!.headers["mcp-session-id"]).toBe("abc");
    expect(seen[0]!.headers.authorization).toBeUndefined();
    expect(seen[0]!.headers.expect).toBeUndefined();
  });

  it("errors: a throwing handler -> 500 (+ error event), malformed request -> 400, head too large -> 431", async () => {
    const edge = await startEdge();
    const errors: unknown[] = [];
    const handle = await exposeOn(edge, {
      handler: (req) => {
        if (new URL(req.url).searchParams.has("boom")) throw new Error("boom");
        return new Response("fine");
      },
    });
    handle.on("error", (e) => errors.push(e));
    const res = await edge.fetch(`${edge.publicUrl("notes")}?boom=1`);
    expect(res.status).toBe(500);
    expect(await res.json()).toEqual({ error: "handler error" });
    expect((errors[0] as Error).message).toBe("boom");

    expect((await rawRequest(edge, "GET /mcp HTTP/1.1\r\nX: 1\r\n\r\n")).status).toBe(400);
    const big = await edge.fetch(edge.publicUrl("notes"), { headers: { "x-big": "x".repeat(70 * 1024) } });
    expect(big.status).toBe(431);
    expect((await edge.fetch(edge.publicUrl("notes"))).status).toBe(200);
  });

  it("disabled -> 503 without calling the handler; enable -> re-register -> served again", async () => {
    const edge = await startEdge();
    let calls = 0;
    const handle = await exposeOn(edge, {
      handler: () => {
        calls++;
        return new Response("served");
      },
    });
    const disabled = new Promise((resolve) => handle.once("disabled", resolve));
    await edge.disable("notes");
    expect(await disabled).toEqual({ serverId: "srv_notes", reason: "disabled" });
    expect(handle.state).toBe("disabled");
    expect((await edge.fetch(edge.publicUrl("notes"))).status).toBe(503); // the edge's own answer
    expect(await rawRequest(edge, "GET /mcp HTTP/1.1\r\nHost: notes-tester.mcpwarp.test\r\n\r\n")).toEqual({ status: 503, body: '{"error":"service disabled"}' });
    expect(calls).toBe(0);

    const registered = new Promise((resolve) => handle.once("registered", resolve));
    await edge.enable("notes");
    await registered;
    expect(handle.state).toBe("registered");
    expect(await (await edge.fetch(edge.publicUrl("notes"))).text()).toBe("served");
    expect(edge.appLog.filter((b) => (b.mcpwarp as { op: string }).op === "register")).toHaveLength(2);
  });

  it("re-registers on reconnect after an abnormal close", async () => {
    const edge = await startEdge();
    const handle = await exposeOn(edge, { handler: () => new Response("again") });
    const disconnects: unknown[] = [];
    handle.on("disconnect", (r) => disconnects.push(r));
    edge.closeConnection({ wsCode: 1006 });
    await edge.waitForApp("register", { count: 2 });
    await edge.waitForRegistered("notes");
    expect(disconnects).toHaveLength(1);
    await waitFor(() => handle.state === "registered");
    expect(await (await edge.fetch(edge.publicUrl("notes"))).text()).toBe("again");
  });

  it("a slow handler on one stream does not delay another", async () => {
    const edge = await startEdge();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    await exposeOn(edge, {
      handler: async (req) => {
        if (new URL(req.url).searchParams.has("slow")) await gate;
        return new Response(new URL(req.url).search);
      },
    });
    const slow = edge.fetch(`${edge.publicUrl("notes")}?slow=1`);
    await new Promise((r) => setTimeout(r, 20));
    expect(await (await edge.fetch(`${edge.publicUrl("notes")}?fast=1`)).text()).toBe("?fast=1");
    release();
    expect(await (await slow).text()).toBe("?slow=1");
  });

  it("close(): unregister reaches the edge before the socket closes; the handle closes once", async () => {
    const edge = await startEdge();
    const handle = await exposeOn(edge, { handler: () => new Response("x") });
    const closes: unknown[] = [];
    handle.on("close", (e) => closes.push(e));
    const conn = edge.connections[0]!;
    await handle.close();
    await handle.close();
    const ops = edge.appLog.map((b) => (b.mcpwarp as { op: string }).op);
    expect(ops).toEqual(["register", "unregister"]);
    expect(conn.closed).toBe(true);
    expect(handle.state).toBe("closed");
    expect(closes).toEqual([{}]);
  });

  it("a rejected token (close 4011) rejects expose() with McpwarpError UNAUTHORIZED, the transport error as cause", async () => {
    const edge = new FakeEdge({ acceptToken: () => false });
    await edge.start();
    cleanups.push(() => edge.stop());
    const err = await expose({ url: edge.url, webSocket: edge.webSocket, token: "bad", name: "notes", integration: INTEGRATION, handler: () => new Response() }).catch(
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(McpwarpError);
    expect(err).toMatchObject({ code: "UNAUTHORIZED", fatal: true, serviceName: "notes" });
    const cause = (err as McpwarpError).cause;
    expect(cause).toBeInstanceOf(WsMixerError);
    expect(cause).toMatchObject({ code: ErrorCode.UNAUTHORIZED, fatal: true });
  });

  it("an HTTP 401 at the upgrade rejects expose() with UNAUTHORIZED, a 403 with DISCONNECTED_FATAL; the cause carries httpStatus", async () => {
    const edge = await startEdge();
    for (const [status, code] of [
      [401, "UNAUTHORIZED"],
      [403, "DISCONNECTED_FATAL"],
    ] as const) {
      edge.rejectNextUpgrade(status);
      const err = await expose({ url: edge.url, webSocket: edge.webSocket, token: "t", name: "notes", integration: INTEGRATION, handler: () => new Response() }).catch(
        (e: unknown) => e,
      );
      expect(err).toMatchObject({ code, fatal: true });
      expect((err as McpwarpError).cause).toBeInstanceOf(WsMixerError);
      expect((err as McpwarpError).cause).toMatchObject({ httpStatus: status, fatal: true });
    }
  });

  it("a fatal close after registration closes the handle with a McpwarpError, the transport error as cause", async () => {
    const edge = await startEdge();
    const handle = await exposeOn(edge, { handler: () => new Response() });
    const closed = new Promise<{ error?: Error }>((r) => handle.once("close", r));
    edge.closeConnection({ wsCode: 4000 + ErrorCode.UNAUTHORIZED });
    const { error } = await closed;
    expect(error).toBeInstanceOf(McpwarpError);
    expect(error).toMatchObject({ code: "UNAUTHORIZED", fatal: true, serviceName: "notes" });
    expect((error as McpwarpError).cause).toBeInstanceOf(WsMixerError);
    expect(handle.state).toBe("closed");
  });

  it("registration: readable right after expose() resolves; a drain re-register keeps the url with created false", async () => {
    const edge = await startEdge();
    const handle = await exposeOn(edge, { handler: () => new Response() });
    expect(handle.registration).toEqual({ url: "https://notes-tester.mcpwarp.test/mcp", serverId: "srv_notes", created: true });
    const reregistered = new Promise((r) => handle.once("registered", r));
    await edge.drain({ deadlineMs: 25000, conn: edge.connections[0]! as EdgeConnection });
    await reregistered;
    expect(handle.registration).toEqual({ url: "https://notes-tester.mcpwarp.test/mcp", serverId: "srv_notes", created: false });
  });

  it("registration is undefined when expose() resolved as disabled with no registration yet", async () => {
    const edge = await startEdge();
    edge.failRegister("notes", { code: "SERVER_DISABLED" });
    const handle = await exposeOn(edge, { handler: () => new Response() });
    expect(handle.state).toBe("disabled");
    expect(handle.registration).toBeUndefined();
  });

  it("a fatal first registration rejects expose()", async () => {
    const edge = await startEdge();
    edge.failRegister("notes", { code: "QUOTA_EXCEEDED", message: "plan limit" });
    await expect(
      expose({ url: edge.url, webSocket: edge.webSocket, token: "t", name: "notes", integration: INTEGRATION, handler: () => new Response() }),
    ).rejects.toMatchObject({ code: "QUOTA_EXCEEDED", fatal: true });
  });

  it("conflict before the first registered rejects expose() with TAKEN_OVER, closes the client, and nothing is dialed or registered again", async () => {
    const edge = await startEdge();
    edge.withholdRegistered("notes");
    const outcome = expose({
      url: edge.url,
      webSocket: edge.webSocket,
      token: "t",
      name: "notes",
      integration: INTEGRATION,
      handler: () => new Response(),
      reconnect: { base: 5, cap: 10 },
    }).catch((e: unknown) => e);
    await edge.waitForApp("register");
    await edge.conflict("notes");
    const err = await outcome;
    expect(err).toBeInstanceOf(McpwarpError);
    expect(err).toMatchObject({ code: "TAKEN_OVER", fatal: true, serviceName: "notes" });
    await waitFor(() => edge.connections.length === 0);
    await new Promise((r) => setTimeout(r, 100));
    expect(edge.upgrades).toHaveLength(1);
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register"]);
  });

  it("drain: an SSE response in flight on the old connection completes there; the service re-registers and new requests land on the replacement", async () => {
    const edge = await startEdge();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const sessions: string[] = [];
    const handle = await exposeOn(edge, {
      handler: async (req) => {
        if (new URL(req.url).searchParams.has("sse")) {
          const body = new ReadableStream<Uint8Array>({
            async start(c) {
              c.enqueue(new TextEncoder().encode("event: one\n\n"));
              await gate;
              c.enqueue(new TextEncoder().encode("event: two\n\n"));
              c.close();
            },
          });
          return new Response(body, { headers: { "content-type": "text/event-stream" } });
        }
        return new Response("plain");
      },
    });
    const first = edge.connections[0]! as EdgeConnection;
    sessions.push(first.session);
    const sse = await edge.fetch(`${edge.publicUrl("notes")}?sse=1`);
    const reader = sse.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("event: one\n\n");

    await edge.drain({ deadlineMs: 25000, conn: first });
    await edge.waitForApp("register", { count: 2, timeoutMs: 5000 });
    await waitFor(() => edge.connections.length === 2);
    const second = edge.connections[1]! as EdgeConnection;
    expect(second.session).not.toBe(first.session);
    expect(first.closed).toBe(false);

    // A new request lands on the replacement while the SSE is still open on the old one.
    expect(await (await edge.fetch(edge.publicUrl("notes"))).text()).toBe("plain");

    release();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("event: two\n\n");
    expect((await reader.read()).done).toBe(true);
    await waitFor(() => first.closed);
    expect(second.closed).toBe(false);

    // The old connection's end does not unregister the service in the agent's eyes.
    await new Promise((r) => setTimeout(r, 20));
    expect(handle.state).toBe("registered");
    expect(await (await edge.fetch(edge.publicUrl("notes"))).text()).toBe("plain");
    await handle.close();
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register", "register", "unregister"]);
  });

  it("a response body that errors mid-stream RESETs the stream and reports the error (chunked and Content-Length)", async () => {
    const edge = await startEdge();
    const handle = await exposeOn(edge, {
      handler: (req) => {
        const body = new ReadableStream<Uint8Array>({
          async start(c) {
            c.enqueue(new TextEncoder().encode("data: first\n\n"));
            await new Promise((r) => setTimeout(r, 10));
            c.error(new Error("upstream died"));
          },
        });
        const headers: Record<string, string> = { "content-type": "text/event-stream" };
        if (new URL(req.url).searchParams.has("cl")) headers["content-length"] = "1000";
        return new Response(body, { headers });
      },
    });
    const errors: Error[] = [];
    handle.on("error", (e) => errors.push(e));
    for (const query of ["", "?cl=1"]) {
      const res = await edge.fetch(edge.publicUrl("notes") + query);
      const reader = res.body!.getReader();
      expect(new TextDecoder().decode((await reader.read()).value)).toBe("data: first\n\n");
      await expect(reader.read()).rejects.toThrow();
    }
    await waitFor(() => errors.length === 2);
    expect(errors.map((e) => e.message)).toEqual(["upstream died", "upstream died"]);
    expect(handle.client.currentConn()!.liveStreamCount()).toBe(0);
  });

  it("a throwing handle listener is reported on 'error' and does not stop the reconnect", async () => {
    const edge = await startEdge();
    const handle = await exposeOn(edge, { handler: () => new Response("back") });
    const errors: Error[] = [];
    handle.on("error", (e) => errors.push(e));
    handle.on("disconnect", () => {
      throw new Error("listener bug");
    });
    handle.on("disconnect", () => {
      throw new Error("listener bug 1b");
    });
    let laterDisconnectListener = 0;
    handle.on("disconnect", () => laterDisconnectListener++);
    handle.on("registered", () => {
      throw new Error("listener bug 2");
    });
    edge.closeConnection({ wsCode: 1006 });
    await edge.waitForApp("register", { count: 2 });
    await waitFor(() => handle.state === "registered");
    expect(await (await edge.fetch(edge.publicUrl("notes"))).text()).toBe("back");
    expect(errors.map((e) => e.message)).toEqual(["listener bug", "listener bug 1b", "listener bug 2"]);
    expect(laterDisconnectListener).toBe(1);
  });

  it("reconnect.maxAttempts exhausted after registration: the handle closes with DISCONNECTED_FATAL", async () => {
    const edge = await startEdge();
    const handle = await exposeOn(edge, { handler: () => new Response(), reconnect: { base: 5, cap: 10, maxAttempts: 1 } });
    const closed = new Promise<{ error?: Error }>((r) => handle.once("close", r));
    edge.rejectNextUpgrade(503);
    edge.rejectNextUpgrade(503);
    edge.closeConnection({ wsCode: 1006 });
    const { error } = await closed;
    expect(error).toMatchObject({ code: "DISCONNECTED_FATAL", fatal: true, serviceName: "notes" });
    // The disconnect payload, not a WsMixerError.
    expect((error as McpwarpError).cause).toMatchObject({ fatal: true, message: expect.any(String) });
    expect(handle.state).toBe("closed");
  });

  it("reconnect.maxAttempts exhausted before the first registration rejects expose() with DISCONNECTED_FATAL", async () => {
    const edge = await startEdge();
    edge.failRegister("notes", { code: "UNAVAILABLE", message: "busy" });
    const outcome = expose({
      url: edge.url,
      webSocket: edge.webSocket,
      token: "t",
      name: "notes",
      integration: INTEGRATION,
      handler: () => new Response(),
      reconnect: { base: 5, cap: 10, maxAttempts: 1 },
    }).catch((e: Error) => e);
    await edge.waitForApp("register");
    edge.rejectNextUpgrade(503);
    edge.rejectNextUpgrade(503);
    edge.closeConnection({ wsCode: 1006 });
    const err = await outcome;
    expect(err).toMatchObject({ code: "DISCONNECTED_FATAL", fatal: true });
    expect((err as McpwarpError).cause).toMatchObject({ fatal: true, message: expect.any(String) });
  });

  it("close() after the service was refused (SERVER_DISABLED) does not wait out the unregister budget", async () => {
    const edge = await startEdge();
    edge.failRegister("notes", { code: "SERVER_DISABLED", id: "srv_notes" });
    const handle = await exposeOn(edge, { handler: () => new Response() });
    expect(handle.state).toBe("disabled");
    const started = Date.now();
    await handle.close();
    expect(Date.now() - started).toBeLessThan(1000);
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register", "unregister"]);
  });
});

describe("expose(): signal and onRetry", () => {
  /** Every dial is refused at the upgrade with 503: a non-fatal failure retried forever. */
  function refusingEdge(edge: FakeEdge) {
    return (url: string, init: Parameters<FakeEdge["webSocket"]>[1]) => {
      edge.rejectNextUpgrade(503);
      return edge.webSocket(url, init);
    };
  }

  it("aborting while the first connection keeps failing rejects with AbortError and stops dialing; onRetry saw the retries", async () => {
    const edge = await startEdge();
    const ac = new AbortController();
    const retries: Array<{ attempt: number; delayMs: number; reason: string }> = [];
    const p = expose({
      url: edge.url,
      webSocket: refusingEdge(edge),
      token: "t",
      name: "notes",
      integration: INTEGRATION,
      handler: () => new Response(),
      reconnect: { base: 5, cap: 10 },
      signal: ac.signal,
      onRetry: (info) => retries.push(info),
    });
    const outcome = p.catch((e: Error) => e);
    await waitFor(() => retries.length >= 3);
    ac.abort();
    const err = (await outcome) as Error;
    expect(err.name).toBe("AbortError");
    expect(retries[0]).toMatchObject({ attempt: 1, reason: expect.stringMatching(/503/) });
    const dials = edge.upgrades.length;
    await new Promise((r) => setTimeout(r, 100));
    expect(edge.upgrades.length).toBe(dials);
  });

  it("a pre-101 429/503 seen through a standard WebSocket (bare error + close 1006, no status) is retried with backoff and reported via onRetry", async () => {
    const edge = await startEdge();
    /** What a browser/undici WebSocket shows for any refused upgrade: no status, no headers, no Retry-After. */
    class RefusedSocket {
      readyState = 0;
      protocol = "";
      bufferedAmount = 0;
      binaryType = "blob";
      private listeners = new Map<string, Array<(ev: unknown) => void>>();
      constructor() {
        setTimeout(() => {
          this.readyState = 3;
          for (const l of this.listeners.get("error") ?? []) l({ type: "error" });
          for (const l of this.listeners.get("close") ?? []) l({ type: "close", code: 1006, reason: "" });
        }, 1);
      }
      addEventListener(type: string, l: (ev: unknown) => void): void {
        this.listeners.set(type, [...(this.listeners.get(type) ?? []), l]);
      }
      removeEventListener(type: string, l: (ev: unknown) => void): void {
        this.listeners.set(type, (this.listeners.get(type) ?? []).filter((x) => x !== l));
      }
      send(): void {
        throw new Error("not open");
      }
      close(): void {}
    }
    const failures = 3;
    let dials = 0;
    const retries: Array<{ attempt: number; delayMs: number; reason: string }> = [];
    const handle = await exposeOn(edge, {
      handler: () => new Response("up"),
      webSocket: (url, init) => (++dials <= failures ? new RefusedSocket() : edge.webSocket(url, init)),
      reconnect: { base: 5, cap: 20 },
      onRetry: (info) => retries.push(info),
    });
    expect(handle.state).toBe("registered");
    expect(dials).toBe(failures + 1);
    expect(retries.map((r) => r.attempt)).toEqual([1, 2, 3]);
    for (const r of retries) {
      expect(r.reason).toBe("WebSocket connection failed");
      expect(r.delayMs).toBeLessThanOrEqual(20);
    }
    expect(await (await edge.fetch(edge.publicUrl("notes"))).text()).toBe("up");
  });

  it("an already-aborted signal rejects without dialing", async () => {
    const edge = await startEdge();
    const err = (await expose({
      url: edge.url,
      webSocket: edge.webSocket,
      token: "t",
      name: "notes",
      integration: INTEGRATION,
      handler: () => new Response(),
      signal: AbortSignal.abort(),
    }).catch((e: Error) => e)) as Error;
    expect(err.name).toBe("AbortError");
    expect(edge.upgrades).toHaveLength(0);
  });

  it("aborting after registration closes the handle (unregister, then close)", async () => {
    const edge = await startEdge();
    const ac = new AbortController();
    const handle = await exposeOn(edge, { handler: () => new Response(), signal: ac.signal });
    const closed = new Promise((r) => handle.once("close", r));
    ac.abort();
    await closed;
    expect(handle.state).toBe("closed");
    expect(edge.appLog.map((b) => (b.mcpwarp as { op: string }).op)).toEqual(["register", "unregister"]);
  });

  it("onRetry reports a transient register error before the first registration", async () => {
    const edge = await startEdge();
    edge.failRegister("notes", { code: "UNAVAILABLE", message: "backend busy" });
    const retries: Array<{ attempt: number; delayMs: number; reason: string }> = [];
    const handle = await exposeOn(edge, { handler: () => new Response(), onRetry: (info) => retries.push(info) });
    expect(handle.state).toBe("registered");
    expect(retries).toEqual([{ attempt: 1, delayMs: 1000, reason: "register failed: UNAVAILABLE: backend busy" }]);
  });
});

describe("expose({target})", () => {
  async function localServer(handler: http.RequestListener) {
    const server = http.createServer(handler);
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    cleanups.push(() => new Promise<void>((r) => server.close(() => r())));
    return `127.0.0.1:${(server.address() as AddressInfo).port}`;
  }

  it("forwards to the local server: Host rewritten, x-forwarded-*, query merged for /mcp, gzip decoded and re-served plain", async () => {
    const edge = await startEdge();
    const seen: Array<{ url: string; headers: http.IncomingHttpHeaders; body: string }> = [];
    const host = await localServer((req, res) => {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        seen.push({ url: req.url!, headers: req.headers, body });
        if (req.url!.includes("gz=1")) {
          res.writeHead(200, { "content-type": "text/plain", "content-encoding": "gzip" });
          res.end(gzipSync("zipped body"));
          return;
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ path: req.url }));
      });
    });
    await exposeOn(edge, { target: `http://${host}/mcp?token=abc` });

    const res = await edge.fetch(`${edge.publicUrl("notes")}?b=2`, { method: "POST", body: "payload" });
    expect(await res.json()).toEqual({ path: "/mcp?token=abc&b=2" });
    expect(seen[0]!.headers.host).toBe(host);
    expect(seen[0]!.headers["x-forwarded-host"]).toBe("notes-tester.mcpwarp.test");
    expect(seen[0]!.headers["x-forwarded-proto"]).toBe("https");
    expect(seen[0]!.headers["accept-encoding"]).toBe("identity");
    expect(seen[0]!.body).toBe("payload");

    const gz = await edge.fetch(`${edge.publicUrl("notes")}?gz=1`);
    expect(gz.headers.get("content-encoding")).toBeNull();
    expect(await gz.text()).toBe("zipped body");
    expect(seen[1]!.url).toBe("/mcp?token=abc&gz=1");
  });

  it("a target that dies mid-body: the stream is RESET and the error reported", async () => {
    const edge = await startEdge();
    const host = await localServer((_req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write("data: first\n\n");
      setTimeout(() => res.destroy(), 20);
    });
    const handle = await exposeOn(edge, { target: `http://${host}/mcp` });
    const errors: Error[] = [];
    handle.on("error", (e) => errors.push(e));
    const res = await edge.fetch(edge.publicUrl("notes"));
    const reader = res.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("data: first\n\n");
    await expect(reader.read()).rejects.toThrow();
    await waitFor(() => errors.length === 1);
    expect(handle.client.currentConn()!.liveStreamCount()).toBe(0);
  });

  it("an unreachable target -> 502", async () => {
    const edge = await startEdge();
    const closed = http.createServer();
    await new Promise<void>((r) => closed.listen(0, "127.0.0.1", r));
    const port = (closed.address() as AddressInfo).port;
    await new Promise<void>((r) => closed.close(() => r()));
    await exposeOn(edge, { target: `http://127.0.0.1:${port}/mcp` });
    const res = await edge.fetch(edge.publicUrl("notes"));
    expect(res.status).toBe(502);
    expect((await res.json()).error).toMatch(/local server unreachable/);
  });

  it("a target that sends no headers within targetTimeoutMs -> 504, and the target's request is aborted", async () => {
    const edge = await startEdge();
    let targetClosed!: () => void;
    const closedByProxy = new Promise<void>((r) => (targetClosed = r));
    const host = await localServer((req) => {
      req.socket.once("close", () => targetClosed());
    });
    await exposeOn(edge, { target: `http://${host}/mcp`, targetTimeoutMs: 100 });
    const started = Date.now();
    const res = await edge.fetch(edge.publicUrl("notes"));
    expect(res.status).toBe(504);
    expect(await res.json()).toEqual({ error: "local server timed out" });
    expect(Date.now() - started).toBeLessThan(2000);
    await closedByProxy;
  });

  it("without an AbortController a hung target still gets 504 after targetTimeoutMs", async () => {
    const edge = await startEdge();
    let fetches = 0;
    try {
      vi.stubGlobal("AbortController", undefined);
      vi.stubGlobal("fetch", () => {
        fetches++;
        return new Promise<Response>(() => {});
      });
      await exposeOn(edge, { target: "http://127.0.0.1:1/mcp", targetTimeoutMs: 100 });
      const started = Date.now();
      const res = await edge.fetch(edge.publicUrl("notes"));
      expect(res.status).toBe(504);
      expect(await res.json()).toEqual({ error: "local server timed out" });
      expect(Date.now() - started).toBeLessThan(2000);
      expect(fetches).toBe(1);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("targetTimeoutMs only bounds the wait for headers: a body streaming past it is delivered whole", async () => {
    const edge = await startEdge();
    const host = await localServer((_req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.flushHeaders();
      let n = 0;
      const tick = setInterval(() => {
        res.write(`data: ${++n}\n\n`);
        if (n === 6) {
          clearInterval(tick);
          res.end();
        }
      }, 50);
    });
    await exposeOn(edge, { target: `http://${host}/mcp`, targetTimeoutMs: 100 });
    const started = Date.now();
    const res = await edge.fetch(edge.publicUrl("notes"));
    expect(res.status).toBe(200);
    expect(await res.text()).toBe([1, 2, 3, 4, 5, 6].map((n) => `data: ${n}\n\n`).join(""));
    expect(Date.now() - started).toBeGreaterThanOrEqual(250);
  });

  it("targetTimeoutMs is validated synchronously", () => {
    const base = { token: "t", name: "ok", integration: INTEGRATION, target: "http://127.0.0.1:1/mcp" };
    for (const bad of [0, -1, Number.NaN, Infinity, 2_147_483_648, 1e10, "5" as unknown as number]) {
      expect(() => expose({ ...base, targetTimeoutMs: bad })).toThrow(/targetTimeoutMs/);
    }
  });

  it("targetTimeoutMs with `handler` is a TypeError", () => {
    const base = { token: "t", name: "ok", integration: INTEGRATION };
    expect(() => expose({ ...base, handler: () => new Response(), targetTimeoutMs: 100 } as never)).toThrow(TypeError);
    expect(() => expose({ ...base, handler: () => new Response(), targetTimeoutMs: 100 } as never)).toThrow(/targetTimeoutMs.*only to `target`/);
  });
});

describe("expose(): path allowlist", () => {
  const at = (edge: FakeEdge, path: string) => edge.publicUrl("notes").replace(/\/mcp$/, path);
  const headerNames = (res: Response) => [...res.headers.keys()].sort();

  async function counting(kind: "handler" | "target"): Promise<{ calls: () => number; seen: string[]; opts: { handler: ExposeOptions["handler"] } | { target: string } }> {
    const seen: string[] = [];
    if (kind === "handler") {
      return {
        calls: () => seen.length,
        seen,
        opts: {
          handler: (req) => {
            const u = new URL(req.url);
            seen.push(u.pathname + u.search);
            return new Response("served");
          },
        },
      };
    }
    const server = http.createServer((req, res) => {
      seen.push(req.url!);
      req.resume();
      req.on("end", () => res.end("served"));
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    cleanups.push(() => new Promise<void>((r) => server.close(() => r())));
    return { calls: () => seen.length, seen, opts: { target: `http://127.0.0.1:${(server.address() as AddressInfo).port}/mcp` } };
  }

  for (const kind of ["handler", "target"] as const) {
    describe(kind, () => {
      it("only /mcp is forwarded: GET/POST /healthz, /MCP and /mcp/ get 404 not_found without reaching the app; /mcp?x=1 passes", async () => {
        const edge = await startEdge();
        const app = await counting(kind);
        const handle = await exposeOn(edge, { ...app.opts, maxRequestBodyBytes: 10 });
        expect(handle.path).toBe("/mcp");

        for (const init of [{}, { method: "POST", body: "x".repeat(300 * 1024) }]) {
          const res = await edge.fetch(at(edge, "/healthz"), init);
          expect(res.status).toBe(404);
          expect(headerNames(res)).toEqual(["content-length", "content-type"]);
          expect(res.headers.get("content-type")).toBe("application/json");
          expect(await res.text()).toBe('{"error":"not_found"}');
        }
        for (const p of ["/MCP", "/mcp/", "/"]) expect((await edge.fetch(at(edge, p))).status).toBe(404);
        expect(app.calls()).toBe(0);

        const ok = await edge.fetch(`${edge.publicUrl("notes")}?x=1`);
        expect(ok.status).toBe(200);
        expect(await ok.text()).toBe("served");
        expect(app.seen).toEqual(["/mcp?x=1"]);
        await waitFor(() => handle.client.currentConn()!.liveStreamCount() === 0);
      });

      it("disabled: an unknown path still gets 404, the exposed one 503", async () => {
        const edge = await startEdge();
        const app = await counting(kind);
        const handle = await exposeOn(edge, app.opts);
        const disabled = new Promise((r) => handle.once("disabled", r));
        await edge.disable("notes");
        await disabled;
        const host = "notes-tester.mcpwarp.test";
        expect(await rawRequest(edge, `GET /healthz HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).toEqual({ status: 404, body: '{"error":"not_found"}' });
        expect(await rawRequest(edge, `GET /mcp HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).toEqual({ status: 503, body: '{"error":"service disabled"}' });
        expect(app.calls()).toBe(0);
        await waitFor(() => handle.client.currentConn()!.liveStreamCount() === 0);
      });

      it("HEAD to an unknown path: the GET Content-Length, no body", async () => {
        const edge = await startEdge();
        const app = await counting(kind);
        await exposeOn(edge, app.opts);
        const conn = edge.connections[edge.connections.length - 1] as EdgeConnection;
        const stream = conn.openStream();
        await stream.write(latin1Encode("HEAD /healthz HTTP/1.1\r\nHost: notes-tester.mcpwarp.test\r\n\r\n"));
        await stream.closeWrite();
        const reader = new StreamByteReader(stream);
        let text = "";
        for (let c = await reader.readChunk(); c !== null; c = await reader.readChunk()) text += new TextDecoder().decode(c);
        expect(text).toBe('HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 21\r\n\r\n');
        expect(app.calls()).toBe(0);
      });
    });
  }

  it("an absolute-form request target is matched on its path component", async () => {
    const edge = await startEdge();
    const app = await counting("handler");
    await exposeOn(edge, app.opts);
    const host = "notes-tester.mcpwarp.test";
    expect((await rawRequest(edge, `GET http://${host}/mcp?x=1 HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).status).toBe(200);
    for (const t of [`http://${host}/healthz`, `http://${host}/a/../mcp`, `http://${host}\\mcp`]) {
      expect((await rawRequest(edge, `GET ${t} HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).status).toBe(404);
    }
    expect(app.seen).toEqual(["/mcp?x=1"]);
  });
});
