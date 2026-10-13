/**
 * The fake edge itself (src/testing): handshake, keepalive, the mcpwarp
 * registry, request/response streams against a hand-written agent built on
 * connect(), flow control in both directions, and drain.
 */
import { afterEach, describe, expect, it } from "vitest";
import { connect, type DisconnectPayload } from "../src/client.js";
import type { MixerStream } from "../src/stream.js";
import { FakeEdge } from "../src/testing/index.js";
import { StreamByteReader } from "../src/http1/reader.js";
import { readRequest } from "../src/http1/request.js";
import { writeResponse } from "../src/http1/response.js";
import { encodeRegister, encodeUnregister } from "../src/mcpwarp/envelope.js";

const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c();
});

async function agent(edge: FakeEdge, serve?: (s: MixerStream) => Promise<void>, extra: { onDisconnect?: (r: DisconnectPayload) => void } = {}) {
  const apps: Record<string, unknown>[] = [];
  const client = await connect(edge.url, {
    token: "mcpwarp_pat_x_y",
    webSocket: edge.webSocket,
    onApp: (b) => {
      apps.push(b);
    },
    onStream: (s) => {
      if (serve) void serve(s);
    },
    ...extra,
  });
  cleanups.push(() => client.close());
  return { client, apps };
}

async function startEdge(opts?: ConstructorParameters<typeof FakeEdge>[0]) {
  const edge = new FakeEdge(opts);
  await edge.start();
  cleanups.push(() => edge.stop());
  return edge;
}

/** Echo agent: replies with the request's method, target and body. */
async function echo(s: MixerStream): Promise<void> {
  const req = await readRequest(new StreamByteReader(s), { maxBodyBytes: 16 * 1024 * 1024 });
  const body = req.body ?? new Uint8Array(0);
  await writeResponse(
    s,
    new Response(body as BodyInit, { status: 200, headers: { "x-method": req.method, "x-target": req.target, "x-host": req.host, "content-length": String(body.length) } }),
    req.method,
  );
}

async function waitFor(cond: () => boolean, ms = 2000): Promise<void> {
  const until = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > until) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe("FakeEdge", () => {
  it("records the upgrade and hello, and welcomes", async () => {
    const edge = await startEdge();
    const { client } = await agent(edge);
    expect(edge.upgrades).toHaveLength(1);
    expect(edge.upgrades[0]!.url).toBe("wss://connect.mcpwarp.test");
    expect(edge.upgrades[0]!.protocols).toEqual(["ws-mixer.v1"]);
    expect(edge.upgrades[0]!.headers.Authorization).toBe("Bearer mcpwarp_pat_x_y");
    expect(edge.hellos[0]!.token).toBe("mcpwarp_pat_x_y");
    expect(edge.connections).toHaveLength(1);
    expect(client.currentConn()!.session).toBe(edge.connections[0]!.session);
  });

  it("answers ping with pong", async () => {
    const edge = await startEdge();
    const { client } = await agent(edge);
    const conn = client.currentConn()!;
    const pong = new Promise<{ id: number }>((resolve) => conn.once("pong", resolve));
    (conn as unknown as { sendPing: () => void }).sendPing();
    expect((await pong).id).toBe(0);
  });

  it("rejects a bad token with error{UNAUTHORIZED} + 4011 (fatal)", async () => {
    const edge = await startEdge({ acceptToken: (t) => t === "good" });
    await expect(connect(edge.url, { token: "bad", webSocket: edge.webSocket })).rejects.toMatchObject({ fatal: true });
  });

  it("registry: register -> registered (created once), unregister -> unregistered, failRegister arms one failure", async () => {
    const edge = await startEdge();
    const { client, apps } = await agent(edge);
    edge.failRegister("anki", { code: "QUOTA_EXCEEDED", message: "too many" });
    await client.sendApp(encodeRegister("anki", "http"));
    await waitFor(() => apps.length === 1);
    expect(apps[0]).toEqual({ mcpwarp: { v: 1, op: "registered", services: [], errors: [{ name: "anki", code: "QUOTA_EXCEEDED", message: "too many" }] } });

    await client.sendApp(encodeRegister("anki", "http"));
    expect(await edge.waitForRegistered("anki")).toEqual({ id: "srv_anki", url: "https://anki-tester.mcpwarp.test/mcp" });
    await client.sendApp(encodeRegister("anki", "http"));
    await waitFor(() => apps.length === 3);
    expect((apps[1]!.mcpwarp as { services: unknown[] }).services).toEqual([{ name: "anki", id: "srv_anki", url: "https://anki-tester.mcpwarp.test/mcp", created: true }]);
    expect((apps[2]!.mcpwarp as { services: Array<{ created: boolean }> }).services[0]!.created).toBe(false);

    await client.sendApp(encodeUnregister("anki"));
    await waitFor(() => apps.length === 4);
    expect(apps[3]).toEqual({ mcpwarp: { v: 1, op: "unregistered", services: [{ name: "anki", id: "srv_anki" }] } });
    expect((await edge.waitForApp("unregister")).mcpwarp).toEqual({ v: 1, op: "unregister", services: [{ name: "anki" }] });
    await expect(edge.request(edge.publicUrl("anki"))).rejects.toThrow(/no agent registered for anki/);
  });

  it("unknown op and wrong version get op:error replies", async () => {
    const edge = await startEdge();
    const { client, apps } = await agent(edge);
    await client.sendApp({ mcpwarp: { v: 1, op: "nope" } });
    await client.sendApp({ mcpwarp: { v: 2, op: "register" } });
    await waitFor(() => apps.length === 2);
    expect(apps.map((a) => (a.mcpwarp as { code: string }).code)).toEqual(["UNKNOWN_OP", "UNSUPPORTED_VERSION"]);
  });

  it("request round trip: Go Request.Write shape (Host = public host, no Authorization, User-Agent default)", async () => {
    const edge = await startEdge();
    const seen: Array<[string, string][]> = [];
    const { client } = await agent(edge, async (s) => {
      const req = await readRequest(new StreamByteReader(s), { maxBodyBytes: 1024 });
      seen.push(req.headers);
      await writeResponse(s, new Response(`got ${req.method} ${req.target} ${req.host} ${req.body ? new TextDecoder().decode(req.body) : "-"}`), req.method);
    });
    await client.sendApp(encodeRegister("anki", "http"));
    await edge.waitForRegistered("anki");
    const res = await edge.fetch(`${edge.publicUrl("anki")}?q=1`, { method: "POST", body: "hi", headers: { authorization: "Bearer secret", "x-a": "1" } });
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("got POST /mcp?q=1 anki-tester.mcpwarp.test hi");
    const names = seen[0]!.map(([n]) => n.toLowerCase());
    expect(names).not.toContain("authorization");
    expect(seen[0]).toContainEqual(["user-agent", "Go-http-client/1.1"]);
    expect(seen[0]).toContainEqual(["x-a", "1"]);
  });

  it("flow control: a 1 MiB request body and a 1 MiB response both get through (WINDOW credit-back in both directions)", async () => {
    const edge = await startEdge({ window: 16384 });
    const { client } = await agent(edge, echo);
    await client.sendApp(encodeRegister("big", "http"));
    await edge.waitForRegistered("big");
    const payload = new Uint8Array(1024 * 1024);
    for (let i = 0; i < payload.length; i++) payload[i] = i % 251;
    const res = await edge.fetch(edge.publicUrl("big"), { method: "POST", body: payload });
    const got = new Uint8Array(await res.arrayBuffer());
    expect(got.length).toBe(payload.length);
    expect(got.every((b, i) => b === i % 251)).toBe(true);
  });

  it("a response streamed with chunked framing arrives chunk by chunk", async () => {
    const edge = await startEdge();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const { client } = await agent(edge, async (s) => {
      const req = await readRequest(new StreamByteReader(s), { maxBodyBytes: 1024 });
      const body = new ReadableStream<Uint8Array>({
        async start(c) {
          c.enqueue(new TextEncoder().encode("event: one\n\n"));
          await gate;
          c.enqueue(new TextEncoder().encode("event: two\n\n"));
          c.close();
        },
      });
      await writeResponse(s, new Response(body, { headers: { "content-type": "text/event-stream" } }), req.method);
    });
    await client.sendApp(encodeRegister("sse", "http"));
    await edge.waitForRegistered("sse");
    const res = await edge.fetch(edge.publicUrl("sse"));
    const reader = res.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("event: one\n\n");
    release();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("event: two\n\n");
    expect((await reader.read()).done).toBe(true);
  });

  it("drain with no open streams closes the connection right away with 4012", async () => {
    const edge = await startEdge();
    const disconnects: DisconnectPayload[] = [];
    await agent(edge, undefined, { onDisconnect: (r) => disconnects.push(r) });
    await edge.drain({ deadlineMs: 10_000 });
    await waitFor(() => disconnects.length > 0);
    expect(disconnects[0]).toMatchObject({ wsCode: 4012, fatal: false });
  });

  it("the factory throws before start()", () => {
    const edge = new FakeEdge();
    expect(() => edge.webSocket(edge.url, { protocols: ["ws-mixer.v1"], headers: {} })).toThrow(/not started/);
  });

  it("stop() closes connections and rejects a pending request", async () => {
    const edge = await startEdge();
    const { client } = await agent(edge, async () => {
      // never answers
    });
    await client.sendApp(encodeRegister("slow", "http"));
    await edge.waitForRegistered("slow");
    const p = edge.fetch(edge.publicUrl("slow"));
    await new Promise((r) => setTimeout(r, 20));
    await edge.stop();
    await expect(p).rejects.toThrow(/stopped/);
    expect(edge.connections).toHaveLength(0);
  });
});
