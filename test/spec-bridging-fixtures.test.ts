/**
 * spec/fixtures/mcpwarp/bridging/*.json (MCPWARP.md section 10.3): each
 * fixture's raw HTTP/1.1 `in` is written on one stream, served in the
 * fixture's `sdkState` (the availability and 503 reasons `expose()` uses),
 * and the stream's bytes are compared with `out`. Per spec/README.md: an
 * SDK-generated error reply is byte-exact; an app response is compared by
 * status line, header set (names case-insensitive, order free) and
 * de-chunked body. The app call is compared with `upstream` (or must not
 * happen when it is null). No 1xx may precede the response; the stream must
 * end with CLOSE, or with RESET `outReset.code` for a `*_reset` fixture.
 *
 * Two modes. Buffering: serveWithHandler / serveWithTarget, per the
 * fixture's `mode`. Server: the ./node `server` adapter (serveWithNodeServer
 * over an `http.Server`, as `expose({ server })` wires it), for the
 * `handler`-mode fixtures; the request it hands the app is mapped back to the
 * public one (`x-forwarded-*` to the URL and `host`, framing headers
 * dropped) and Node's `Date` is ignored. `*_reset` fixtures apply only to a
 * streaming adapter, so only the server mode runs them (section 10.3).
 */
import http from "node:http";
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MixerStream, type StreamHost } from "../src/stream.js";
import { FrameType, decodeFrame, resetCode } from "../src/frame.js";
import { latin1Decode, latin1Encode } from "../src/http1/headers.js";
import { serveWithHandler, serveWithTarget, type ServeContext } from "../src/mcpwarp/serve.js";
import { UNAVAILABLE_REASONS } from "../src/mcpwarp/expose.js";
import { serveWithNodeServer } from "../src/node/server.js";
import { resolveSpecDir } from "./helpers/spec-dir.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const specResult = resolveSpecDir(here);
const bridgingDir = path.join(specResult.dir, "fixtures/mcpwarp/bridging");
const available = specResult.available && existsSync(bridgingDir);
const skipReason = specResult.reason ?? `no fixtures/mcpwarp/bridging under ${specResult.dir} (spec.pin older than v0.6.0?)`;

interface Upstream {
  method: string;
  url: string;
  headers: Record<string, string>;
  /** Absent with `outReset`: the app reads a body stream that fails. */
  body?: string;
}

interface BridgingFixture {
  description: string;
  sdkState: "online" | "disabled" | "closing";
  mode: "handler" | "target";
  target?: string;
  limits: { maxRequestBodyBytes: number; targetTimeoutMs?: number };
  handler: { behaviour: "echo" | "throw" | "hang" };
  in: string;
  inExpand?: Record<string, { repeat: string; count: number }>;
  /** Written once the response head has arrived. */
  inRest?: string;
  upstream: Upstream | null;
  out: string;
  outReset?: { code: number };
  notes?: string;
}

type Mode = "buffering" | "server";

function load(): Array<{ file: string; fixture: BridgingFixture }> {
  if (!available) return [];
  return readdirSync(bridgingDir)
    .filter((f) => f.endsWith(".json"))
    .sort()
    .map((file) => ({ file, fixture: JSON.parse(readFileSync(path.join(bridgingDir, file), "utf8")) as BridgingFixture }));
}

/** A stream whose outbound DATA/CLOSE/RESET are recorded; the test plays the edge. */
function recordedStream() {
  const out: Array<{ kind: "DATA"; bytes: Uint8Array } | { kind: "CLOSE" } | { kind: "RESET"; code: number }> = [];
  const host: StreamHost = {
    sendData: async (_id, chunk) => {
      out.push({ kind: "DATA", bytes: chunk.slice() });
    },
    sendControlFrame: (frame) => {
      const f = decodeFrame(frame);
      if (f.type === FrameType.CLOSE) out.push({ kind: "CLOSE" });
      else if (f.type === FrameType.RESET) out.push({ kind: "RESET", code: resetCode(f) });
    },
    retireStream: () => {},
  };
  const stream = new MixerStream(1, host, 64 * 1024 * 1024, 64 * 1024 * 1024);
  const feed = (bytes: Uint8Array) => {
    for (let i = 0; i < bytes.length; i += 16384) stream.handleData(bytes.subarray(i, i + 16384));
  };
  const written = () => out.filter((o): o is { kind: "DATA"; bytes: Uint8Array } => o.kind === "DATA").map((o) => latin1Decode(o.bytes)).join("");
  return { stream, out, feed, written };
}

function expandIn(f: BridgingFixture): string {
  let s = f.in;
  for (const [marker, { repeat, count }] of Object.entries(f.inExpand ?? {})) s = s.split(marker).join(repeat.repeat(count));
  return s;
}

interface ParsedHttpResponse {
  statusLine: string;
  headers: [string, string][];
  body: string;
}

function parseHead(raw: string): { statusLine: string; headers: [string, string][]; rest: string } {
  const end = raw.indexOf("\r\n\r\n");
  expect(end, `no end of head in ${JSON.stringify(raw)}`).toBeGreaterThan(0);
  const [statusLine, ...lines] = raw.slice(0, end).split("\r\n");
  const headers = lines.map((l): [string, string] => [l.slice(0, l.indexOf(":")).trim().toLowerCase(), l.slice(l.indexOf(":") + 1).trim()]);
  return { statusLine: statusLine!, headers, rest: raw.slice(end + 4) };
}

/** Splits a raw response; a chunked body is de-chunked. */
function parseResponse(raw: string): ParsedHttpResponse {
  const { statusLine, headers, rest: afterHead } = parseHead(raw);
  let rest = afterHead;
  let body = "";
  if (headers.some(([n, v]) => n === "transfer-encoding" && v.toLowerCase() === "chunked")) {
    for (;;) {
      const lineEnd = rest.indexOf("\r\n");
      const size = parseInt(rest.slice(0, lineEnd).split(";")[0]!, 16);
      expect(Number.isFinite(size)).toBe(true);
      rest = rest.slice(lineEnd + 2);
      if (size === 0) {
        expect(rest).toBe("\r\n");
        break;
      }
      body += rest.slice(0, size);
      expect(rest.slice(size, size + 2)).toBe("\r\n");
      rest = rest.slice(size + 2);
    }
  } else {
    body = rest;
  }
  return { statusLine, headers, body };
}

const sortHeaders = (h: [string, string][]) => [...h].sort((a, b) => (a[0] === b[0] ? a[1].localeCompare(b[1]) : a[0].localeCompare(b[0])));

async function waitFor(cond: () => boolean, what: string, ms = 5000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

function bufferingServer(fixture: BridgingFixture, ctx: ServeContext, calls: Upstream[]) {
  if (fixture.mode === "handler") {
    return serveWithHandler(async (request) => {
      const headers: Record<string, string> = {};
      request.headers.forEach((v, k) => (headers[k] = v));
      const body = await request.text();
      calls.push({ method: request.method, url: request.url, headers, body });
      if (fixture.handler.behaviour === "throw") throw new Error("fixture handler throws");
      const resHeaders = new Headers();
      const ct = request.headers.get("content-type");
      if (ct !== null) resHeaders.set("content-type", ct);
      return new Response(body, { status: 200, headers: resHeaders });
    }, ctx);
  }
  expect(fixture.handler.behaviour).toBe("hang");
  vi.stubGlobal("fetch", (input: string, init: RequestInit) => {
    const headers: Record<string, string> = {};
    new Headers(init.headers).forEach((v, k) => (headers[k] = v));
    const body = init.body === undefined || init.body === null ? "" : latin1Decode(init.body as Uint8Array);
    calls.push({ method: init.method ?? "GET", url: input, headers, body });
    return new Promise<Response>((_, reject) => {
      init.signal?.addEventListener("abort", () => reject(init.signal!.reason), { once: true });
    });
  });
  return serveWithTarget(fixture.target!, ctx, fixture.limits.targetTimeoutMs);
}

/**
 * The fixture's app as a Node `request` listener: `echo` answers its head at
 * once and pipes the request body back (the body is not read first), `throw`
 * throws before reading it.
 */
function nodeServer(fixture: BridgingFixture, ctx: ServeContext, calls: Upstream[]) {
  const app = http.createServer((req, res) => {
    const headers = { ...req.headers } as Record<string, string>;
    const publicHost = headers["x-forwarded-host"];
    const url = `${headers["x-forwarded-proto"]}://${publicHost}${req.url}`;
    for (const name of ["x-forwarded-host", "x-forwarded-proto", "content-length", "transfer-encoding"]) delete headers[name];
    if (publicHost !== undefined) headers.host = publicHost;
    const call: Upstream = { method: req.method!, url, headers };
    calls.push(call);
    if (fixture.handler.behaviour === "throw") throw new Error("fixture handler throws");
    call.body = "";
    req.on("data", (c: Buffer) => (call.body += c.toString("latin1")));
    req.on("error", () => {});
    const ct = req.headers["content-type"];
    res.writeHead(200, ct !== undefined ? { "content-type": ct } : {});
    req.pipe(res);
  });
  return serveWithNodeServer(app, ctx);
}

async function runFixture(fixture: BridgingFixture, mode: Mode): Promise<void> {
  const state = fixture.sdkState;
  const errors: unknown[] = [];
  const ctx: ServeContext = {
    isAvailable: () => state === "online",
    unavailableReason: () => (state === "disabled" ? UNAVAILABLE_REASONS.disabled : UNAVAILABLE_REASONS.closing),
    maxBodyBytes: fixture.limits.maxRequestBodyBytes,
    onError: (e) => errors.push(e),
  };
  const calls: Upstream[] = [];
  const serve = mode === "server" ? nodeServer(fixture, ctx, calls) : bufferingServer(fixture, ctx, calls);

  const { stream, out, feed, written } = recordedStream();
  const done = serve(stream);
  const input = expandIn(fixture);
  const bytes = latin1Encode(input);
  const headEnd = input.indexOf("\r\n\r\n");
  if (fixture.inRest !== undefined) {
    feed(bytes);
    await waitFor(() => written().includes("\r\n\r\n"), "the response head");
    feed(latin1Encode(fixture.inRest));
    stream.handleClose();
    await done;
  } else if (state !== "online" && headEnd >= 0) {
    // 404/503 answer right after the head: they must not wait for (or read) the body.
    feed(bytes.subarray(0, headEnd + 4));
    // The server adapter drains the body after its reply, so it only finishes once the body is in.
    if (mode === "buffering") await done;
    else await waitFor(() => written().length >= fixture.out.length, "the refusal");
    feed(bytes.subarray(headEnd + 4));
    stream.handleClose();
    await done;
  } else {
    feed(bytes);
    stream.handleClose();
    await done;
  }
  await waitFor(() => out.at(-1)?.kind === "CLOSE" || out.at(-1)?.kind === "RESET", "the end of the stream");

  const raw = written();
  expect(raw.startsWith("HTTP/1.1 1"), "no 1xx before the final response").toBe(false);
  expect(raw.match(/HTTP\/1\.[01] \d{3}/g)).toHaveLength(1);
  if (fixture.outReset) {
    expect(out.at(-1)).toEqual({ kind: "RESET", code: fixture.outReset.code });
    expect(out.some((o) => o.kind === "CLOSE")).toBe(false);
  } else {
    expect(out.some((o) => o.kind === "RESET")).toBe(false);
    expect(out.at(-1)?.kind).toBe("CLOSE");
  }

  if (fixture.upstream === null) {
    expect(calls).toEqual([]);
  } else {
    expect(calls).toHaveLength(1);
    // No body to compare: `outReset` fixtures have none, and the server mode's `throw` app never read it.
    if (fixture.upstream.body === undefined || calls[0]!.body === undefined) {
      const { body: _want, ...want } = fixture.upstream;
      const { body: _got, ...got } = calls[0]!;
      expect(got).toEqual(want);
    } else {
      expect(calls[0]).toEqual(fixture.upstream);
    }
  }

  const appHeaders = (h: [string, string][]) => sortHeaders(mode === "server" ? h.filter(([n]) => n !== "date") : h);
  const fromApp = fixture.upstream !== null && fixture.handler.behaviour === "echo";
  if (fixture.outReset) {
    const got = parseHead(raw);
    const want = parseHead(fixture.out);
    expect(got.statusLine).toBe(want.statusLine);
    expect(appHeaders(got.headers)).toEqual(appHeaders(want.headers));
  } else if (!fromApp) {
    expect(raw).toBe(fixture.out);
  } else {
    const got = parseResponse(raw);
    const want = parseResponse(fixture.out);
    expect(got.statusLine).toBe(want.statusLine);
    expect(appHeaders(got.headers)).toEqual(appHeaders(want.headers));
    expect(got.body).toBe(want.body);
  }
  if (fixture.handler.behaviour === "throw") expect(errors).toHaveLength(1);
}

const fixtures = load();

afterEach(() => {
  vi.unstubAllGlobals();
});

describe.skipIf(!available)("spec bridging fixtures", () => {
  it("the corpus has every §10.3 file", () => {
    const names = fixtures.map((f) => f.file);
    for (const name of [
      "mcp_post_ok",
      "non_mcp_404",
      "disabled_503",
      "body_413",
      "target_504",
      "handler_throw_500",
      "head_431",
      "notification_passthrough",
      "expect_continue_200",
      "precedence_disabled_unknown_path_404",
      "precedence_closing_over_cap_503",
    ]) {
      expect(names, `${name}.json`).toContain(`${name}.json`);
    }
  });

  for (const { file, fixture } of fixtures) {
    if (fixture.outReset) {
      it.skip(`[${fixture.mode}] ${file}: skipped, a *_reset fixture applies only to a streaming adapter and ${fixture.mode} buffers the body (§10.3); see [server]`, () => {});
    } else {
      it(`[${fixture.mode}] ${file}: ${fixture.description}`, { timeout: 15_000 }, () => runFixture(fixture, "buffering"));
    }
    if (fixture.mode === "handler") {
      it(`[server] ${file}: ${fixture.description}`, { timeout: 15_000 }, () => runFixture(fixture, "server"));
    } else {
      it.skip(`[server] ${file}: skipped, a ${fixture.mode}-mode fixture describes a local target, not an app`, () => {});
    }
  }
});

describe.skipIf(available)("spec bridging fixtures (unavailable)", () => {
  // CI fetches the spec at spec.pin, so a missing corpus there is a broken pin, not a local setup gap.
  if (process.env.CI) it("spec fixtures are required in CI", () => expect.fail(skipReason));
  else it.skip(skipReason, () => {});
});
