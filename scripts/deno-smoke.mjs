// Smoke test of the platform-neutral build on a second engine:
// `deno run scripts/deno-smoke.mjs` (after `npm run build`, no permissions
// needed -- the transport is the in-memory FakeEdge). Imports the three
// neutral entries, exposes a fetch-style handler, and sends one POST and
// one chunked (SSE-style) request through the tunnel. This proves the code
// runs on Deno's own WebSocket-free stack with its Request/Response/
// ReadableStream; it does not prove the absence of Node imports (Deno
// provides node:* itself) -- test/boundary.test.ts and check-neutral.mjs do.

import { SDK_VERSION } from "../dist/index.js";
import { expose } from "../dist/mcpwarp/index.js";
import { FakeEdge } from "../dist/testing/index.js";

function check(cond, message) {
  if (!cond) throw new Error(`deno-smoke: ${message}`);
}

const edge = new FakeEdge();
await edge.start();
const handle = await expose({
  url: edge.url,
  webSocket: edge.webSocket,
  token: "smoke-token",
  name: "smoke",
  integration: "00000000-0000-4000-8000-000000000000",
  handler: async (req) => {
    if (req.method === "POST") return Response.json({ echo: await req.text() });
    const enc = new TextEncoder();
    const body = new ReadableStream({
      start(c) {
        c.enqueue(enc.encode("data: one\n\n"));
        c.enqueue(enc.encode("data: two\n\n"));
        c.close();
      },
    });
    return new Response(body, { headers: { "content-type": "text/event-stream" } });
  },
});
check(handle.url === edge.publicUrl("smoke"), `unexpected url ${handle.url}`);
check(edge.hellos[0].meta.client === "sdk-js" && edge.hellos[0].meta.version === SDK_VERSION, "hello.meta identity missing");
check(edge.hellos[0].agent.runtime.startsWith("deno/"), `runtime not detected as deno: ${edge.hellos[0].agent.runtime}`);

const post = await edge.fetch(edge.publicUrl("smoke"), { method: "POST", body: "hello deno" });
const json = await post.json();
check(post.status === 200 && json.echo === "hello deno", `POST failed: ${post.status} ${JSON.stringify(json)}`);

const sse = await edge.fetch(edge.publicUrl("smoke"));
const text = await sse.text();
check(text === "data: one\n\ndata: two\n\n", `SSE body was ${JSON.stringify(text)}`);

await handle.close();
await edge.stop();
console.log(`deno-smoke: ok (${edge.hellos[0].agent.runtime}, ${edge.hellos[0].agent.os})`);
