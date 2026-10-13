/** `@mcpwarp/ws-mixer/node`: expose({server}) through a private http.Server, and toDuplex. */
import http from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { expose, serveWithNodeServer, toDuplex } from "../src/node/index.js";
import type { ExposeHandle } from "../src/mcpwarp/index.js";
import { FakeEdge } from "../src/testing/index.js";
import { MixerStream, type StreamHost } from "../src/stream.js";
import { FrameType, decodeFrame, resetCode } from "../src/frame.js";
import { ErrorCode } from "../src/errors.js";
import type { EdgeConnection } from "../src/testing/edge-conn.js";
import { StreamByteReader } from "../src/http1/reader.js";
import { readResponse } from "../src/http1/response.js";

const INTEGRATION = "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10";
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c();
});

async function waitFor(cond: () => boolean, ms = 5000): Promise<void> {
  const until = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > until) throw new Error("waitFor timed out");
    await new Promise((r) => setTimeout(r, 5));
  }
}

/** Resolves `done` once `tick()` has been called `n` times. */
function countdown(n: number): { tick(): void; done: Promise<void> } {
  let resolve!: () => void;
  const done = new Promise<void>((r) => (resolve = r));
  return {
    tick: () => {
      if (--n === 0) resolve();
    },
    done,
  };
}

async function setup(server: http.Server | http.RequestListener, extra?: { maxRequestBodyBytes?: number }): Promise<{ edge: FakeEdge; handle: ExposeHandle }> {
  const edge = new FakeEdge();
  await edge.start();
  cleanups.push(() => edge.stop());
  const handle = await expose({ url: edge.url, webSocket: edge.webSocket, token: "t", name: "app", integration: INTEGRATION, server, ...extra });
  cleanups.push(() => handle.close());
  return { edge, handle };
}

describe("expose({server})", () => {
  it("an http.Server: JSON round trip; Host is localhost with x-forwarded-host; the user's server is not mutated", async () => {
    const seen: http.IncomingHttpHeaders[] = [];
    const server = http.createServer((req, res) => {
      seen.push(req.headers);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ method: req.method, url: req.url }));
    });
    const before = { request: server.listenerCount("request"), connection: server.listenerCount("connection"), halfOpen: (server as unknown as { httpAllowHalfOpen?: boolean }).httpAllowHalfOpen };
    const { edge } = await setup(server);
    const res = await edge.fetch(`${edge.publicUrl("app")}?x=1`);
    expect(await res.json()).toEqual({ method: "GET", url: "/mcp?x=1" });
    expect(seen[0]!.host).toBe("localhost");
    expect(seen[0]!["x-forwarded-host"]).toBe("app-tester.mcpwarp.test");
    expect(seen[0]!["x-forwarded-proto"]).toBe("https");
    expect(server.listenerCount("request")).toBe(before.request);
    expect(server.listenerCount("connection")).toBe(before.connection);
    expect((server as unknown as { httpAllowHalfOpen?: boolean }).httpAllowHalfOpen).toBe(before.halfOpen);
    expect(server.listening).toBe(false);
  });

  it("a (req, res) listener; POST with Expect: 100-continue gets no interim response (the fake edge rejects any 1xx)", async () => {
    const { edge } = await setup((req, res) => {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        res.writeHead(201, { "content-type": "text/plain" });
        res.end(`got ${body}`);
      });
    });
    const res = await edge.fetch(edge.publicUrl("app"), { method: "POST", body: "payload", headers: { expect: "100-continue" } });
    expect(res.status).toBe(201);
    expect(await res.text()).toBe("got payload");
  });

  it("SSE: each event reaches the client as it is written", async () => {
    let wroteThird = false;
    const { edge } = await setup((_req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.flushHeaders();
      let n = 0;
      const tick = setInterval(() => {
        n++;
        res.write(`data: ${n}\n\n`);
        if (n === 3) {
          wroteThird = true;
          clearInterval(tick);
          res.end();
        }
      }, 100);
    });
    const res = await edge.fetch(edge.publicUrl("app"));
    const reader = res.body!.getReader();
    const first = new TextDecoder().decode((await reader.read()).value);
    expect(first).toBe("data: 1\n\n");
    expect(wroteThird).toBe(false);
    let rest = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      rest += new TextDecoder().decode(value);
    }
    expect(rest).toBe("data: 2\n\ndata: 3\n\n");
  });

  it("an http.Server with no request listener -> 502", async () => {
    const { edge } = await setup(http.createServer());
    expect((await edge.fetch(edge.publicUrl("app"))).status).toBe(502);
  });

  it("same edge-facing rules as handler/target: 431 for a head over 64 KiB, 413 over the body cap, no Expect, no Connection/Keep-Alive", async () => {
    const seen: http.IncomingHttpHeaders[] = [];
    const { edge } = await setup(
      (req, res) => {
        seen.push(req.headers);
        req.resume();
        req.on("end", () => res.end("ok"));
      },
      { maxRequestBodyBytes: 10 },
    );
    const big = await edge.fetch(edge.publicUrl("app"), { headers: { "x-big": "x".repeat(70 * 1024) } });
    expect(big.status).toBe(431);
    expect(await big.json()).toEqual({ error: "request head too large" });

    const tooLarge = await edge.fetch(edge.publicUrl("app"), { method: "POST", body: "x".repeat(11) });
    expect(tooLarge.status).toBe(413);
    expect([...tooLarge.headers.keys()].sort()).toEqual(["content-length", "content-type"]);
    expect(seen).toHaveLength(0);

    const ok = await edge.fetch(edge.publicUrl("app"), { method: "POST", body: "small", headers: { expect: "100-continue" } });
    expect(ok.status).toBe(200);
    expect(await ok.text()).toBe("ok");
    expect(seen[0]!.expect).toBeUndefined();
    expect(ok.headers.get("connection")).toBeNull();
    expect(ok.headers.get("keep-alive")).toBeNull();
    expect(ok.headers.get("content-length")).toBe("2");
  });

  it("a chunked body over the cap: 413 before the app answers, RESET after; the app does not crash; the next request is served", async () => {
    const aborted: string[] = [];
    const reqErrors: string[] = [];
    const lateWrites: string[] = [];
    // Both oversized requests: 'aborted', the late write, and (first one only) 'error'.
    const settled = countdown(5);
    const { edge } = await setup(
      (req, res) => {
        const started = req.url!.includes("started");
        if (started) {
          res.writeHead(200, { "content-type": "text/plain" });
          res.flushHeaders();
        } else {
          req.on("error", (e) => {
            reqErrors.push(e.message);
            settled.tick();
          });
        }
        req.on("aborted", () => {
          aborted.push(req.url!);
          settled.tick();
        });
        let n = 0;
        req.on("data", (c: Buffer) => (n += c.length));
        req.on("end", () => res.end(`read ${n}`));
        // Writes after the abort must be harmless.
        setTimeout(() => {
          if (!res.writableEnded) {
            res.end("late");
            lateWrites.push(req.url!);
            settled.tick();
          }
        }, 50);
      },
      { maxRequestBodyBytes: 10 },
    );
    const chunked = () =>
      new ReadableStream<Uint8Array>({
        start(c) {
          for (let i = 0; i < 25; i++) c.enqueue(new Uint8Array(4096).fill(120));
          c.close();
        },
      });

    const tooLarge = await edge.fetch(edge.publicUrl("app"), { method: "POST", body: chunked() });
    expect(tooLarge.status).toBe(413);
    expect(await tooLarge.json()).toEqual({ error: "request body too large" });

    const started = await edge.fetch(`${edge.publicUrl("app")}?started=1`, { method: "POST", body: chunked() });
    expect(started.status).toBe(200);
    await expect(started.text()).rejects.toThrow();

    await settled.done;
    expect(aborted).toEqual(["/mcp", "/mcp?started=1"]);
    expect(reqErrors).toEqual(["aborted"]);
    expect(lateWrites).toEqual(["/mcp", "/mcp?started=1"]);

    const small = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode("tiny"));
        c.close();
      },
    });
    const ok = await edge.fetch(edge.publicUrl("app"), { method: "POST", body: small });
    expect(ok.status).toBe(200);
    expect(await ok.text()).toBe("read 4");
  });

  describe("path allowlist", () => {
    const at = (edge: FakeEdge, path: string) => edge.publicUrl("app").replace(/\/mcp$/, path);

    function counting() {
      const seen: string[] = [];
      const server = http.createServer((req, res) => {
        seen.push(req.url!);
        req.resume();
        req.on("end", () => res.end("served"));
      });
      return { server, seen };
    }

    async function raw(edge: FakeEdge, bytes: string): Promise<{ status: number; body: string }> {
      const conn = edge.connections[edge.connections.length - 1] as EdgeConnection;
      const stream = conn.openStream();
      await stream.write(new TextEncoder().encode(bytes));
      await stream.closeWrite();
      const { head, body } = await readResponse(new StreamByteReader(stream), "GET");
      let text = "";
      for await (const c of body) text += new TextDecoder().decode(c);
      return { status: head.status, body: text };
    }

    it("only /mcp reaches the server: GET/POST /healthz, /MCP and /mcp/ get 404 not_found; /mcp?x=1 passes", async () => {
      const app = counting();
      const { edge, handle } = await setup(app.server, { maxRequestBodyBytes: 10 });
      expect(handle.path).toBe("/mcp");
      for (const init of [{}, { method: "POST", body: "x".repeat(300 * 1024) }]) {
        const res = await edge.fetch(at(edge, "/healthz"), init);
        expect(res.status).toBe(404);
        expect([...res.headers.keys()].sort()).toEqual(["content-length", "content-type"]);
        expect(res.headers.get("content-type")).toBe("application/json");
        expect(await res.text()).toBe('{"error":"not_found"}');
      }
      for (const p of ["/MCP", "/mcp/", "/"]) expect((await edge.fetch(at(edge, p))).status).toBe(404);
      expect(app.seen).toEqual([]);
      const ok = await edge.fetch(`${edge.publicUrl("app")}?x=1`);
      expect(ok.status).toBe(200);
      expect(await ok.text()).toBe("served");
      expect(app.seen).toEqual(["/mcp?x=1"]);
      await waitFor(() => handle.client.currentConn()!.liveStreamCount() === 0);
    });

    it("an absolute-form request target is matched on its path component", async () => {
      const app = counting();
      const { edge } = await setup(app.server);
      const host = "app-tester.mcpwarp.test";
      expect((await raw(edge, `GET http://${host}/mcp?x=1 HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).status).toBe(200);
      for (const t of [`http://${host}/healthz`, `http://${host}/a/../mcp`]) {
        expect((await raw(edge, `GET ${t} HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).status).toBe(404);
      }
      // Node's own parser rejects a `\` in the target before the path check.
      expect((await raw(edge, `GET http://${host}\\mcp HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).status).toBe(400);
      expect(app.seen).toEqual([`http://${host}/mcp?x=1`]);
    });

    it("OPTIONS * and authority-form get 400, as in the handler/target adapters", async () => {
      const app = counting();
      const { edge } = await setup(app.server);
      const host = "app-tester.mcpwarp.test";
      expect(await raw(edge, `OPTIONS * HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).toEqual({ status: 400, body: '{"error":"unsupported request target form"}' });
      expect((await raw(edge, `GET ${host}:443 HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).status).toBe(400);
      expect(app.seen).toEqual([]);
    });

    it("disabled: an unknown path still gets 404, the exposed one 503", async () => {
      const app = counting();
      const { edge, handle } = await setup(app.server);
      const disabled = new Promise((r) => handle.once("disabled", r));
      await edge.disable("app");
      await disabled;
      const host = "app-tester.mcpwarp.test";
      expect(await raw(edge, `GET /healthz HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).toEqual({ status: 404, body: '{"error":"not_found"}' });
      expect(await raw(edge, `GET /mcp HTTP/1.1\r\nHost: ${host}\r\n\r\n`)).toEqual({ status: 503, body: '{"error":"service disabled"}' });
      expect(app.seen).toEqual([]);
      await waitFor(() => handle.client.currentConn()!.liveStreamCount() === 0);
    });
  });

  it("targetTimeoutMs with `server` is a TypeError", () => {
    expect(() => expose({ token: "t", name: "app", integration: INTEGRATION, server: () => {}, targetTimeoutMs: 100 } as never)).toThrow(/targetTimeoutMs.*only to `target`/);
  });

  it("throws on Bun (its node:http ignores emit('connection')), pointing to `target`", () => {
    Object.defineProperty(process.versions, "bun", { value: "1.3.3", configurable: true, enumerable: true });
    try {
      expect(() => expose({ url: "ws://x", token: "t", name: "app", integration: INTEGRATION, server: () => {} })).toThrow(/not supported on Bun.*`target`/);
    } finally {
      delete (process.versions as Record<string, string>).bun;
    }
  });
});

describe("serveWithNodeServer on a raw stream", () => {
  const head = "POST /mcp HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n";
  const chunk = (n: number) => `${n.toString(16)}\r\n${"x".repeat(n)}\r\n`;
  const enc = (s: string) => new TextEncoder().encode(s);

  function make() {
    const out: string[] = [];
    const host: StreamHost = {
      sendData: async (_id, c) => {
        out.push(new TextDecoder("latin1").decode(c));
      },
      sendControlFrame: (raw) => {
        const f = decodeFrame(raw);
        out.push(f.type === FrameType.RESET ? `<RESET ${resetCode(f)}>` : f.type === FrameType.CLOSE ? "<CLOSE>" : `<${f.type}>`);
      },
      retireStream: () => {},
    };
    return { stream: new MixerStream(1, host, 1 << 24, 1 << 24), out };
  }

  it("a malformed chunk after the app flushed its head: the stream is reset, no second status line", async () => {
    const errors: unknown[] = [];
    const { stream, out } = make();
    const flushed = countdown(1);
    const serve = serveWithNodeServer(
      (req, res) => {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.flushHeaders();
        req.on("error", () => {});
        req.resume();
        flushed.tick();
      },
      { isAvailable: () => true, maxBodyBytes: 1 << 20, onError: (e) => errors.push(e) },
    );
    const done = serve(stream);
    stream.handleData(enc(head + chunk(3)));
    await flushed.done;
    stream.handleData(enc("zz\r\nbad\r\n"));
    await done;
    const wire = out.join("");
    expect(wire.match(/HTTP\/1\.1 /g)).toHaveLength(1);
    expect(wire).toMatch(/^HTTP\/1\.1 200 OK\r\n/);
    expect(out.at(-1)).toBe(`<RESET ${ErrorCode.CANCEL}>`);
    expect(out).not.toContain("<CLOSE>");
    expect(errors).toHaveLength(1);
  });

  it("a chunked body over the cap after the app finished its response: the response ends with CLOSE, no RESET", async () => {
    const { stream, out } = make();
    const serve = serveWithNodeServer((_req, res) => res.end("early"), { isAvailable: () => true, maxBodyBytes: 10, onError: () => {} });
    const done = serve(stream);
    stream.handleData(enc(head));
    for (let i = 0; i < 20; i++) {
      stream.handleData(enc(chunk(4096)));
      await new Promise((r) => setImmediate(r));
    }
    stream.handleData(enc("0\r\n\r\n"));
    stream.handleClose();
    await done;
    await stream.closed;
    const wire = out.filter((s) => !s.startsWith("<")).join("");
    expect(wire).toMatch(/^HTTP\/1\.1 200 OK\r\n[\s\S]*Content-Length: 5\r\n\r\nearly$/);
    expect(out.at(-1)).toBe("<CLOSE>");
    expect(out.some((s) => s.startsWith("<RESET"))).toBe(false);
  });
});

describe("toDuplex", () => {
  function make() {
    const frames: Array<{ type: number; data?: string; code?: number }> = [];
    const host: StreamHost = {
      sendData: async (_id, chunk) => {
        frames.push({ type: FrameType.DATA, data: new TextDecoder().decode(chunk) });
      },
      sendControlFrame: (raw) => {
        const f = decodeFrame(raw);
        frames.push({ type: f.type, ...(f.type === FrameType.RESET ? { code: resetCode(f) } : {}) });
      },
      retireStream: () => {},
    };
    return { stream: new MixerStream(1, host, 65536, 65536), frames };
  }

  it("round trip: reads the stream's bytes to 'end', writes DATA, end() sends CLOSE", async () => {
    const { stream, frames } = make();
    const d = toDuplex(stream);
    stream.handleData(new TextEncoder().encode("ping"));
    stream.handleClose();
    let got = "";
    d.on("data", (c: Buffer) => (got += c.toString()));
    await new Promise<void>((r) => d.once("end", () => r()));
    expect(got).toBe("ping");
    await new Promise<void>((r) => d.end("pong", () => r()));
    expect(frames).toEqual([{ type: FrameType.DATA, data: "pong" }, { type: FrameType.CLOSE }]);
    await expect(stream.closed).resolves.toEqual({ kind: "finished" });
  });

  it("destroy() RESETs the stream with CANCEL", async () => {
    const { stream, frames } = make();
    const d = toDuplex(stream);
    d.destroy();
    await new Promise((r) => setImmediate(r));
    expect(frames).toEqual([{ type: FrameType.RESET, code: ErrorCode.CANCEL }]);
    await expect(stream.closed).resolves.toMatchObject({ kind: "reset", code: ErrorCode.CANCEL });
  });
});
