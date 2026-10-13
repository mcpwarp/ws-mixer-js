/**
 * End to end with the real MCP SDKs (devDependencies only): a v1 MCP client
 * talks to the public URL through the FakeEdge (`fetch: edge.fetch`), the
 * request crosses the ws-mixer tunnel, and a real MCP server answers via
 * each adapter: v2 `createMcpHandler` through `handler`, v1
 * `StreamableHTTPServerTransport` on a plain http.Server through `server`,
 * and the same v1 server listening on 127.0.0.1 with DNS-rebinding
 * protection through `target`.
 */
import http from "node:http";
import { randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { McpServer as McpServerV1 } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { McpServer as McpServerV2, createMcpHandler } from "@modelcontextprotocol/server";
import { expose } from "../src/node/index.js";
import { FakeEdge } from "../src/testing/index.js";

const INTEGRATION = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10";
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) {
    try {
      await c();
    } catch {
      // best effort
    }
  }
});

/** A latch the test opens, so `slow` provably streams its progress before finishing. */
function latch() {
  let open!: () => void;
  const opened = new Promise<void>((r) => (open = r));
  return { open, opened };
}

function buildServerV2(gate: Promise<void>) {
  const server = new McpServerV2({ name: "e2e-v2", version: "1.0.0" });
  server.registerTool("echo", { description: "echo", inputSchema: z.object({ text: z.string() }) }, async ({ text }) => ({
    content: [{ type: "text", text }],
  }));
  server.registerTool("slow", { description: "slow", inputSchema: z.object({}) }, async (_args, ctx) => {
    const progressToken = ctx.mcpReq._meta?.progressToken;
    if (progressToken !== undefined) {
      await ctx.mcpReq.notify({ method: "notifications/progress", params: { progressToken, progress: 1, total: 2 } });
    }
    await gate;
    return { content: [{ type: "text", text: "done" }] };
  });
  return server;
}

function buildServerV1(gate: Promise<void>) {
  const server = new McpServerV1({ name: "e2e-v1", version: "1.0.0" });
  server.registerTool("echo", { description: "echo", inputSchema: { text: z.string() } }, async ({ text }) => ({
    content: [{ type: "text" as const, text }],
  }));
  server.registerTool("slow", { description: "slow", inputSchema: {} }, async (_args, extra) => {
    const progressToken = extra._meta?.progressToken;
    if (progressToken !== undefined) {
      await extra.sendNotification({ method: "notifications/progress", params: { progressToken, progress: 1, total: 2 } });
    }
    await gate;
    return { content: [{ type: "text" as const, text: "done" }] };
  });
  return server;
}

async function startEdge() {
  const edge = new FakeEdge();
  await edge.start();
  cleanups.push(() => edge.stop());
  return edge;
}

async function mcpClient(edge: FakeEdge, name: string) {
  const client = new Client({ name: "e2e-client", version: "1.0.0" });
  const transport = new StreamableHTTPClientTransport(new URL(edge.publicUrl(name)), { fetch: edge.fetch });
  await client.connect(transport);
  cleanups.push(() => client.close());
  return { client, transport };
}

/** listTools, echo round trip, and `slow`: progress must arrive while the tool is still blocked. */
async function exercise(client: Client, gate: { open: () => void }) {
  const tools = await client.listTools();
  expect(tools.tools.map((t) => t.name).sort()).toEqual(["echo", "slow"]);

  const echoed = await client.callTool({ name: "echo", arguments: { text: "through the tunnel" } });
  expect(echoed.content).toEqual([{ type: "text", text: "through the tunnel" }]);

  let progressed!: () => void;
  const progress = new Promise<void>((r) => (progressed = r));
  const slow = client.callTool({ name: "slow", arguments: {} }, undefined, { onprogress: () => progressed() });
  await Promise.race([progress, new Promise((_, rej) => setTimeout(() => rej(new Error("no progress notification before the result")), 5000))]);
  gate.open();
  const result = await slow;
  expect(result.content).toEqual([{ type: "text", text: "done" }]);
}

describe("MCP SDKs end to end through the tunnel", () => {
  it("v2 createMcpHandler through expose({handler})", async () => {
    const edge = await startEdge();
    const gate = latch();
    const mcp = createMcpHandler(() => buildServerV2(gate.opened));
    cleanups.push(() => mcp.close());
    const handle = await expose({ url: edge.url, webSocket: edge.webSocket, token: "t", name: "v2", integration: INTEGRATION, handler: mcp.fetch });
    cleanups.push(() => handle.close());
    const { client } = await mcpClient(edge, "v2");
    await exercise(client, gate);
  });

  it("v1 StreamableHTTPServerTransport (stateful) on an http.Server through expose({server})", async () => {
    const edge = await startEdge();
    const gate = latch();
    const server = buildServerV1(gate.opened);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: () => randomUUID() });
    await server.connect(transport);
    cleanups.push(() => server.close());
    const httpServer = http.createServer((req, res) => void transport.handleRequest(req, res));
    const handle = await expose({ url: edge.url, webSocket: edge.webSocket, token: "t", name: "v1", integration: INTEGRATION, server: httpServer });
    cleanups.push(() => handle.close());
    const { client, transport: clientTransport } = await mcpClient(edge, "v1");
    expect(transport.sessionId).toBeDefined();
    expect(clientTransport.sessionId).toBe(transport.sessionId);
    await exercise(client, gate);
  });

  it("v1 server on 127.0.0.1 with DNS-rebinding protection through expose({target}) (Host is rewritten to the target's; SSE progress streams through fetch)", async () => {
    const edge = await startEdge();
    const gate = latch();
    let route: http.RequestListener = (_req, res) => res.end();
    const httpServer = http.createServer((req, res) => route(req, res));
    await new Promise<void>((r) => httpServer.listen(0, "127.0.0.1", r));
    cleanups.push(() => new Promise<void>((r) => httpServer.close(() => r())));
    cleanups.push(() => httpServer.closeAllConnections());
    const port = (httpServer.address() as AddressInfo).port;
    const server = buildServerV1(gate.opened);
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      enableDnsRebindingProtection: true,
      allowedHosts: [`127.0.0.1:${port}`],
    });
    await server.connect(transport);
    cleanups.push(() => server.close());
    route = (req, res) => void transport.handleRequest(req, res);

    const handle = await expose({ url: edge.url, webSocket: edge.webSocket, token: "t", name: "tgt", integration: INTEGRATION, target: `http://127.0.0.1:${port}/mcp` });
    cleanups.push(() => handle.close());
    const { client } = await mcpClient(edge, "tgt");
    await exercise(client, gate);
  });
});
