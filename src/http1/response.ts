/**
 * HTTP/1.1 responses on a stream: the writer the agent uses (a web
 * `Response` -> status line, headers, Content-Length or chunked body, then
 * CLOSE) and the parser the fake edge uses (Go `http.ReadResponse` shape).
 */
import type { MixerStream } from "../stream.js";
import { HOP_BY_HOP, concatBytes, connectionTokens, latin1Encode, reasonPhrase } from "./headers.js";
import { HttpParseError, UnexpectedEof, type StreamByteReader } from "./reader.js";
import { bodyFraming, parseHeaderLines } from "./request.js";

const utf8 = new TextEncoder();
const CRLF = latin1Encode("\r\n");
const LAST_CHUNK = latin1Encode("0\r\n\r\n");

/** Responses that never carry a body: to HEAD, 1xx, 204, 304. */
export function isBodyless(method: string, status: number): boolean {
  return method === "HEAD" || status < 200 || status === 204 || status === 304;
}

function responseHeaderLines(res: Response, dropContentEncoding: boolean): string[] {
  const entries: [string, string][] = [];
  res.headers.forEach((value, name) => entries.push([name, value]));
  const listed = connectionTokens(entries);
  const lines: string[] = [];
  const getSetCookie = (res.headers as Headers & { getSetCookie?: () => string[] }).getSetCookie;
  for (const [name, value] of entries) {
    const n = name.toLowerCase();
    if (HOP_BY_HOP.has(n) || listed.has(n) || n === "content-length" || n === "transfer-encoding") continue;
    if (dropContentEncoding && n === "content-encoding") continue;
    if (n === "set-cookie" && typeof getSetCookie === "function") continue;
    lines.push(`${name}: ${value}`);
  }
  if (typeof getSetCookie === "function") {
    for (const cookie of getSetCookie.call(res.headers)) lines.push(`set-cookie: ${cookie}`);
  }
  return lines;
}

function knownLength(res: Response): number | undefined {
  const raw = res.headers.get("content-length");
  if (raw === null || !/^[0-9]+$/.test(raw.trim())) return undefined;
  const n = Number(raw.trim());
  return Number.isSafeInteger(n) ? n : undefined;
}

/**
 * Writes `res` onto `stream` as HTTP/1.1: status line, headers (hop-by-hop
 * dropped; `content-encoding` too with `dropContentEncoding`), then the
 * body -- Content-Length when the response declares one (a shorter body
 * RESETs the stream with INTERNAL_ERROR), else chunked with one chunk per
 * body read (each flushed on its own, so SSE streams through) -- then
 * half-closes. Never sends a 1xx. If the stream ends mid-copy, the body is
 * cancelled and this returns quietly. If the body itself fails, the stream
 * is RESET with INTERNAL_ERROR and the body's error is rethrown.
 */
export async function writeResponse(stream: MixerStream, res: Response, method: string, opts?: { dropContentEncoding?: boolean }): Promise<void> {
  const dropContentEncoding = opts?.dropContentEncoding ?? false;
  const bodyless = isBodyless(method, res.status);
  const lines = [`HTTP/1.1 ${res.status} ${res.statusText || reasonPhrase(res.status)}`, ...responseHeaderLines(res, dropContentEncoding)];
  const length = bodyless ? undefined : res.body === null ? 0 : dropContentEncoding ? undefined : knownLength(res);
  if (!bodyless) lines.push(length !== undefined ? `Content-Length: ${length}` : "Transfer-Encoding: chunked");
  if (bodyless) void res.body?.cancel().catch(() => {});
  const reader = bodyless ? null : res.body?.getReader() ?? null;
  try {
    await stream.write(latin1Encode(lines.join("\r\n") + "\r\n\r\n"));
    if (reader && length !== undefined) {
      let remaining = length;
      while (remaining > 0) {
        const { done, value } = await reader.read();
        if (done) {
          stream.reset("INTERNAL_ERROR", "response body shorter than Content-Length");
          return;
        }
        if (value.length === 0) continue;
        const piece = value.length > remaining ? value.subarray(0, remaining) : value;
        remaining -= piece.length;
        await stream.write(piece);
      }
      void reader.cancel().catch(() => {});
    } else if (reader) {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value.length === 0) continue;
        await stream.write(concatBytes([latin1Encode(value.length.toString(16)), CRLF, value, CRLF]));
      }
      await stream.write(LAST_CHUNK);
    }
  } catch (e) {
    if (reader) void reader.cancel().catch(() => {});
    if (stream.getState() === "closed") return;
    stream.reset("INTERNAL_ERROR", "response body failed");
    throw e;
  }
  await stream.closeWrite();
}

/** A JSON error reply (`{"error": message}`) with Content-Length, then half-close. To HEAD: the GET Content-Length, no body (RFC 9110 9.3.2). */
export async function writeErrorResponse(stream: MixerStream, status: number, message: string, method: string): Promise<void> {
  const body = utf8.encode(JSON.stringify({ error: message }));
  const bodyless = isBodyless(method, status);
  const head = [`HTTP/1.1 ${status} ${reasonPhrase(status)}`, "Content-Type: application/json", `Content-Length: ${body.length}`];
  try {
    await stream.write(latin1Encode(head.join("\r\n") + "\r\n\r\n"));
    if (!bodyless) await stream.write(body);
  } catch {
    return;
  }
  await stream.closeWrite();
}

export interface ParsedResponseHead {
  status: number;
  statusText: string;
  headers: [string, string][];
}

/**
 * For the fake edge: parses a response head and its body framing (bodyless
 * for HEAD/1xx/204/304, Content-Length, chunked, else until EOF). The body
 * iterator throws if the stream fails or ends early.
 */
export async function readResponse(r: StreamByteReader, method: string): Promise<{ head: ParsedResponseHead; body: AsyncIterable<Uint8Array> }> {
  const raw = await r.readHead(64 * 1024);
  const lines = raw.split("\r\n");
  const m = /^HTTP\/1\.[01] (\d{3})(?: (.*))?$/.exec(lines.shift()!);
  if (!m) throw new HttpParseError(400, "malformed status line");
  const status = Number(m[1]);
  const headers = parseHeaderLines(lines);
  const head: ParsedResponseHead = { status, statusText: m[2] ?? "", headers };

  async function* body(): AsyncGenerator<Uint8Array> {
    if (isBodyless(method, status)) return;
    const framing = bodyFraming(headers);
    if (framing === "chunked") {
      // Chunk by chunk, so each one is visible as soon as it arrives (SSE).
      for (;;) {
        const line = await r.readLine(4096);
        const size = parseInt(line.split(";", 1)[0]!.trim(), 16);
        if (!Number.isFinite(size)) throw new HttpParseError(400, "malformed chunk size");
        if (size === 0) {
          while ((await r.readLine(4096)) !== "") {
            // trailers dropped
          }
          return;
        }
        const data = await r.readExactly(size);
        if ((await r.readLine(4096)) !== "") throw new HttpParseError(400, "missing CRLF after chunk data");
        yield data;
      }
    } else if (framing !== "none") {
      let remaining = framing.length;
      while (remaining > 0) {
        const chunk = await r.readChunk();
        if (chunk === null) throw new UnexpectedEof("response body shorter than Content-Length");
        if (chunk.length > remaining) throw new HttpParseError(400, "response body longer than Content-Length");
        remaining -= chunk.length;
        yield chunk;
      }
    } else {
      for (;;) {
        const chunk = await r.readChunk();
        if (chunk === null) return;
        yield chunk;
      }
    }
  }
  return { head, body: body() };
}
