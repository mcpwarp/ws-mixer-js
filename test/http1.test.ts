/**
 * The HTTP/1.1 codec over MixerStream (src/http1/*): request parsing and
 * web Request conversion, response writing (Content-Length vs chunked,
 * flush per chunk, bodyless replies), and round trips through the fake
 * edge's serializer/parser.
 */
import { describe, expect, it } from "vitest";
import { MixerStream, type StreamHost } from "../src/stream.js";
import { FrameType, decodeFrame, resetCode } from "../src/frame.js";
import { ErrorCode } from "../src/errors.js";
import { HttpParseError, StreamByteReader } from "../src/http1/reader.js";
import { DEFAULT_MAX_BODY_BYTES, readRequest, requestPath, serializeRequestHead, toWebRequest } from "../src/http1/request.js";
import { readResponse, writeErrorResponse, writeResponse } from "../src/http1/response.js";
import { latin1Encode } from "../src/http1/headers.js";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);
const tick = () => new Promise<void>((r) => setImmediate(r));

/** A stream whose outbound frames are recorded; inbound bytes are fed by the test. */
function makeStream() {
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
  const feed = (bytes: Uint8Array | string, close = true) => {
    const b = typeof bytes === "string" ? latin1Encode(bytes) : bytes;
    for (let i = 0; i < b.length; i += 16384) stream.handleData(b.subarray(i, i + 16384));
    if (close) stream.handleClose();
  };
  const written = () => {
    const parts = out.filter((o): o is { kind: "DATA"; bytes: Uint8Array } => o.kind === "DATA").map((o) => o.bytes);
    const total = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let off = 0;
    for (const p of parts) {
      total.set(p, off);
      off += p.length;
    }
    return total;
  };
  return { stream, feed, out, written };
}

async function parse(raw: string | Uint8Array, maxBodyBytes = DEFAULT_MAX_BODY_BYTES) {
  const { stream, feed } = makeStream();
  feed(raw);
  return readRequest(new StreamByteReader(stream), { maxBodyBytes });
}

async function parseError(raw: string | Uint8Array, maxBodyBytes = DEFAULT_MAX_BODY_BYTES): Promise<HttpParseError> {
  const e = await parse(raw, maxBodyBytes).then(
    () => null,
    (err: unknown) => err,
  );
  expect(e).toBeInstanceOf(HttpParseError);
  return e as HttpParseError;
}

describe("readRequest", () => {
  it("parses a Content-Length body", async () => {
    const p = await parse("POST /mcp HTTP/1.1\r\nHost: a-b.example\r\nContent-Type: application/json\r\nContent-Length: 7\r\n\r\n{\"a\":1}");
    expect(p.method).toBe("POST");
    expect(p.target).toBe("/mcp");
    expect(p.host).toBe("a-b.example");
    expect(dec(p.body!)).toBe('{"a":1}');
    expect(p.headers).toContainEqual(["Content-Type", "application/json"]);
  });

  it("parses a chunked body with chunk extensions and trailers", async () => {
    const p = await parse("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n4;ext=1\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Trailer: t\r\n\r\n");
    expect(dec(p.body!)).toBe("Wikipedia");
  });

  it("no body framing: body is null", async () => {
    const p = await parse("GET /x?y=1 HTTP/1.1\r\nHost: h\r\n\r\n");
    expect(p.body).toBeNull();
    expect(p.target).toBe("/x?y=1");
  });

  it("absolute-form target becomes path + query", async () => {
    const p = await parse("GET https://other.example/p/q?z=2 HTTP/1.1\r\nHost: h\r\n\r\n");
    expect(p.target).toBe("/p/q?z=2");
  });

  it("strips Expect and records 100-continue", async () => {
    const p = await parse("POST / HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\nhi");
    expect(p.expectContinue).toBe(true);
    expect(p.headers.some(([n]) => n.toLowerCase() === "expect")).toBe(false);
  });

  it("missing Host -> 400", async () => {
    expect((await parseError("GET / HTTP/1.1\r\nX: y\r\n\r\n")).status).toBe(400);
  });

  it("Content-Length together with Transfer-Encoding -> 400", async () => {
    expect((await parseError("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n")).status).toBe(400);
  });

  it("a Transfer-Encoding other than chunked -> 400", async () => {
    expect((await parseError("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip, chunked\r\n\r\n")).status).toBe(400);
  });

  it("conflicting Content-Length values -> 400", async () => {
    expect((await parseError("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab")).status).toBe(400);
  });

  it("obs-fold header line -> 400", async () => {
    expect((await parseError("GET / HTTP/1.1\r\nHost: h\r\nX-A: 1\r\n  continued\r\n\r\n")).status).toBe(400);
  });

  it("malformed request line / version -> 400", async () => {
    expect((await parseError("GET / HTTP/2.0\r\nHost: h\r\n\r\n")).status).toBe(400);
    expect((await parseError("GET * HTTP/1.1\r\nHost: h\r\n\r\n")).status).toBe(400);
  });

  it("a head over 64 KiB -> 431", async () => {
    const big = "x".repeat(70 * 1024);
    expect((await parseError(`GET / HTTP/1.1\r\nHost: h\r\nX-Big: ${big}\r\n\r\n`)).status).toBe(431);
  });

  it("a body over the cap -> 413 (Content-Length and chunked)", async () => {
    expect((await parseError("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\n\r\nhello world", 10)).status).toBe(413);
    expect((await parseError("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\nb\r\nhello world\r\n0\r\n\r\n", 10)).status).toBe(413);
  });

  it("EOF inside the body -> 400", async () => {
    expect((await parseError("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 10\r\n\r\nabc")).status).toBe(400);
  });
});

describe("requestPath", () => {
  it.each([
    ["/mcp", "/mcp"],
    ["/mcp?x=1#f", "/mcp"],
    ["/mcp#f", "/mcp"],
    ["http://h/mcp?x", "/mcp"],
    ["HTTPS://u@h:1/mcp", "/mcp"],
    ["http://h", "/"],
    ["http://h/a/../mcp", "/a/../mcp"],
    ["/a/../mcp", "/a/../mcp"],
    ["http://h\\mcp", "/"],
    ["/%6dcp", "/%6dcp"],
    ["*", "*"],
    ["h:443", "h:443"],
  ])("%s -> %s", (target, path) => {
    expect(requestPath(target)).toBe(path);
  });

  it("the parsed target gives the same path as the raw one (absolute-form is not normalised)", async () => {
    for (const raw of ["http://h/mcp?x", "http://h/a/../mcp", "http://h\\mcp", "http://h/%6dcp", "http://h/mcp#f", "http://h?x"]) {
      const p = await parse(`GET ${raw} HTTP/1.1\r\nHost: h\r\n\r\n`);
      expect(requestPath(p.target)).toBe(requestPath(raw));
    }
  });
});

describe("toWebRequest", () => {
  it("builds https://<Host><target> and drops hop-by-hop, Connection-listed and Content-Length headers", async () => {
    const p = await parse(
      "POST /mcp?x=1 HTTP/1.1\r\nHost: svc-user.example\r\nConnection: keep-alive, X-Hop\r\nKeep-Alive: 5\r\nX-Hop: 1\r\nTE: trailers\r\nAccept: text/event-stream\r\nContent-Length: 2\r\n\r\nhi",
    );
    const req = toWebRequest(p);
    expect(req.url).toBe("https://svc-user.example/mcp?x=1");
    expect(req.method).toBe("POST");
    for (const h of ["connection", "keep-alive", "x-hop", "te", "content-length", "expect"]) expect(req.headers.has(h)).toBe(false);
    expect(req.headers.get("accept")).toBe("text/event-stream");
    expect(await req.text()).toBe("hi");
  });

  it("GET drops a body", async () => {
    const req = toWebRequest(await parse("GET / HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\nhi"));
    expect(req.body).toBeNull();
  });
});

describe("writeResponse", () => {
  it("a known Content-Length: exact bytes, then CLOSE", async () => {
    const { stream, out, written } = makeStream();
    await writeResponse(stream, new Response("hello", { status: 201, headers: { "content-type": "text/plain", "content-length": "5" } }), "POST");
    const text = dec(written());
    expect(text).toMatch(/^HTTP\/1\.1 201 Created\r\n/);
    expect(text).toContain("\r\nContent-Length: 5\r\n");
    expect(text).not.toContain("Transfer-Encoding");
    expect(text.endsWith("\r\n\r\nhello")).toBe(true);
    expect(out.at(-1)).toEqual({ kind: "CLOSE" });
  });

  it("an unknown length is chunked, each body read flushed as its own DATA frame before the next is produced", async () => {
    const { stream, out } = makeStream();
    let ctrl!: ReadableStreamDefaultController<Uint8Array>;
    const body = new ReadableStream<Uint8Array>({ start: (c) => void (ctrl = c) });
    const done = writeResponse(stream, new Response(body, { headers: { "content-type": "text/event-stream" } }), "GET");
    await tick();
    ctrl.enqueue(enc("data: one\n\n"));
    await tick();
    const dataFrames = () => out.filter((o) => o.kind === "DATA").map((o) => dec((o as { bytes: Uint8Array }).bytes));
    expect(dataFrames()).toHaveLength(2); // head, then the first chunk -- already on the wire
    expect(dataFrames()[0]).toContain("Transfer-Encoding: chunked");
    expect(dataFrames()[1]).toBe("b\r\ndata: one\n\n\r\n");
    ctrl.enqueue(enc("data: two\n\n"));
    ctrl.close();
    await done;
    expect(dataFrames().slice(2)).toEqual(["b\r\ndata: two\n\n\r\n", "0\r\n\r\n"]);
    expect(out.at(-1)).toEqual({ kind: "CLOSE" });
  });

  it("HEAD, 204 and 304 are bodyless", async () => {
    for (const [method, status] of [
      ["HEAD", 200],
      ["GET", 204],
      ["GET", 304],
    ] as const) {
      const { stream, written } = makeStream();
      await writeResponse(stream, new Response(status === 200 ? "ignored" : null, { status }), method);
      const text = dec(written());
      expect(text.endsWith("\r\n\r\n")).toBe(true);
      expect(text).not.toContain("Content-Length");
      expect(text).not.toContain("Transfer-Encoding");
    }
  });

  it("writes each Set-Cookie on its own line", async () => {
    const { stream, written } = makeStream();
    const headers = new Headers();
    headers.append("set-cookie", "a=1; Path=/");
    headers.append("set-cookie", "b=2");
    await writeResponse(stream, new Response("", { headers }), "GET");
    const text = dec(written());
    expect(text).toContain("\r\nset-cookie: a=1; Path=/\r\n");
    expect(text).toContain("\r\nset-cookie: b=2\r\n");
  });

  it("dropContentEncoding: drops content-encoding and its (encoded) Content-Length, re-frames as chunked", async () => {
    const { stream, written } = makeStream();
    await writeResponse(stream, new Response("plain", { headers: { "content-encoding": "gzip", "content-length": "99" } }), "GET", { dropContentEncoding: true });
    const text = dec(written());
    expect(text.toLowerCase()).not.toContain("content-encoding");
    expect(text).toContain("Transfer-Encoding: chunked");
    expect(text.endsWith("5\r\nplain\r\n0\r\n\r\n")).toBe(true);
  });

  it("a body shorter than its Content-Length RESETs the stream with INTERNAL_ERROR", async () => {
    const { stream, out } = makeStream();
    await writeResponse(stream, new Response("abc", { headers: { "content-length": "10" } }), "GET");
    expect(out.at(-1)).toEqual({ kind: "RESET", code: ErrorCode.INTERNAL_ERROR });
  });

  it("writeErrorResponse: JSON body with Content-Length", async () => {
    const { stream, written, out } = makeStream();
    await writeErrorResponse(stream, 503, "service disabled", "POST");
    const text = dec(written());
    expect(text).toMatch(/^HTTP\/1\.1 503 Service Unavailable\r\n/);
    expect(text).toContain("Content-Type: application/json");
    expect(text.endsWith('\r\n\r\n{"error":"service disabled"}')).toBe(true);
    expect(out.at(-1)).toEqual({ kind: "CLOSE" });
  });
});

describe("round trips", () => {
  it("serializeRequestHead + body -> readRequest", async () => {
    const headers = new Headers({ "content-type": "application/json", "x-a": "1" });
    const body = enc('{"jsonrpc":"2.0"}');
    const head = serializeRequestHead("POST", "/mcp", "svc-user.mcpwarp.test", headers, { length: body.length });
    const { stream, feed } = makeStream();
    feed(new Uint8Array([...head, ...body]));
    const p = await readRequest(new StreamByteReader(stream), { maxBodyBytes: 1024 });
    expect(p.host).toBe("svc-user.mcpwarp.test");
    expect(dec(p.body!)).toBe('{"jsonrpc":"2.0"}');
    expect(p.headers).toContainEqual(["x-a", "1"]);
  });

  it("writeResponse -> readResponse, Content-Length and chunked", async () => {
    for (const init of [{ headers: { "content-length": "5" } }, {}]) {
      const a = makeStream();
      await writeResponse(a.stream, new Response("hello", { status: 200, ...init }), "GET");
      const b = makeStream();
      b.feed(a.written());
      const { head, body } = await readResponse(new StreamByteReader(b.stream), "GET");
      expect(head.status).toBe(200);
      const parts: string[] = [];
      for await (const c of body) parts.push(dec(c));
      expect(parts.join("")).toBe("hello");
    }
  });
});
