/**
 * HTTP/1.1 requests as the mcpwarp edge writes them onto a stream (Go
 * `Request.Write`: Content-Length or chunked, then the edge half-closes):
 * a parser for the agent side and a head serializer for the fake edge.
 */
import { HOP_BY_HOP, concatBytes, connectionTokens, isToken, latin1Encode } from "./headers.js";
import { HttpParseError, UnexpectedEof, type StreamByteReader } from "./reader.js";

export interface ParsedRequest {
  method: string;
  /** Origin-form path + query, e.g. "/mcp?x=1"; absolute-form is cut to that, not normalised; the fragment dropped; raw bytes percent-encoded (percentEncodeTarget). */
  target: string;
  /** In order, names as received; `Expect` already removed. */
  headers: [string, string][];
  host: string;
  /** null when the request had no body framing. */
  body: Uint8Array | null;
  /** The request carried `Expect: 100-continue` (stripped: no 1xx is ever sent back). */
  expectContinue: boolean;
}

export const MAX_HEAD_BYTES = 64 * 1024;
export const HEAD_DEADLINE_MS = 10_000;
export const DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024;

const MAX_CHUNK_LINE = 4096;

function bad(message: string): HttpParseError {
  return new HttpParseError(400, message);
}

/** Origin-form as is; absolute-form validated, then cut to its path + query as written (not normalised). */
function parseTarget(raw: string): string {
  if (raw.startsWith("/")) return raw;
  const authority = /^https?:\/\/[^/?#]*/i.exec(raw);
  if (authority) {
    try {
      new URL(raw);
    } catch {
      throw bad("malformed request target");
    }
    const rest = raw.slice(authority[0].length).split("#", 1)[0]!;
    return rest.startsWith("/") ? rest : `/${rest}`;
  }
  throw bad("unsupported request target form");
}

/** RFC 3986 unreserved, sub-delims, ":" "@" (pchar), "/" "?" (query), "[" "]" and "%" (an escape is kept, never decoded). */
const TARGET_SAFE = /[A-Za-z0-9\-._~!$&'()*+,;=:@/?[\]%]/;

/**
 * The head is decoded as latin1, so each char of `target` is one received
 * byte. Any byte outside TARGET_SAFE (raw UTF-8, controls, ...) becomes
 * `%XX`, exactly once, so later URL parsing never UTF-8-encodes it again.
 */
export function percentEncodeTarget(target: string): string {
  let out = "";
  for (const ch of target) {
    if (TARGET_SAFE.test(ch)) out += ch;
    else out += "%" + ch.charCodeAt(0).toString(16).toUpperCase().padStart(2, "0");
  }
  return out;
}

/**
 * The path component of a raw request-target, not normalised: the query
 * and fragment cut off; for absolute-form, the part after the authority.
 */
export function requestPath(target: string): string {
  const authority = /^[a-z][a-z0-9+.-]*:\/\/[^/?#]*/i.exec(target);
  const rest = authority ? target.slice(authority[0].length) : target;
  const end = rest.search(/[?#]/);
  const path = end < 0 ? rest : rest.slice(0, end);
  return authority && path === "" ? "/" : path;
}

/** Parses header lines (after the start line) per RFC 9112 section 5. */
export function parseHeaderLines(lines: string[]): [string, string][] {
  const out: [string, string][] = [];
  for (const line of lines) {
    if (line.startsWith(" ") || line.startsWith("\t")) throw bad("obsolete header line folding");
    const colon = line.indexOf(":");
    if (colon <= 0) throw bad("malformed header line");
    const name = line.slice(0, colon);
    if (!isToken(name)) throw bad(`invalid header name ${JSON.stringify(name)}`);
    out.push([name, line.slice(colon + 1).trim()]);
  }
  return out;
}

/** Reads a chunked body (RFC 9112 section 7.1); trailers are read and dropped. */
export async function readChunkedBody(r: StreamByteReader, maxBodyBytes: number): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const line = await r.readLine(MAX_CHUNK_LINE);
    const sizeText = line.split(";", 1)[0]!.trim();
    if (!/^[0-9a-fA-F]+$/.test(sizeText)) throw bad("malformed chunk size");
    const size = parseInt(sizeText, 16);
    if (size === 0) break;
    total += size;
    if (total > maxBodyBytes) throw new HttpParseError(413, "request body too large");
    parts.push(await r.readExactly(size));
    if ((await r.readLine(MAX_CHUNK_LINE)) !== "") throw bad("missing CRLF after chunk data");
  }
  for (;;) {
    if ((await r.readLine(MAX_CHUNK_LINE)) === "") break;
  }
  return concatBytes(parts);
}

/** Body framing headers, validated: chunked, a Content-Length, or none. */
export function bodyFraming(headers: [string, string][]): "chunked" | { length: number } | "none" {
  const te = headers.filter(([n]) => n.toLowerCase() === "transfer-encoding").map(([, v]) => v);
  const cl = headers.filter(([n]) => n.toLowerCase() === "content-length").map(([, v]) => v);
  if (te.length > 0 && cl.length > 0) throw bad("both Transfer-Encoding and Content-Length");
  if (te.length > 0) {
    if (te.length !== 1 || te[0]!.trim().toLowerCase() !== "chunked") throw bad("unsupported Transfer-Encoding");
    return "chunked";
  }
  if (cl.length > 0) {
    const values = cl.flatMap((v) => v.split(",")).map((v) => v.trim());
    if (values.some((v) => !/^[0-9]+$/.test(v))) throw bad("malformed Content-Length");
    if (new Set(values).size !== 1) throw bad("conflicting Content-Length values");
    const length = Number(values[0]);
    if (!Number.isSafeInteger(length)) throw bad("malformed Content-Length");
    return { length };
  }
  return "none";
}

/** A validated request head and its body framing; the body is still unread. */
export type RequestHead = Omit<ParsedRequest, "body"> & { framing: "chunked" | { length: number } | "none" };

/**
 * Reads and validates one request head (64 KiB cap); `onHead` runs once its
 * bytes are in. Throws HttpParseError / UnexpectedEof, or the stream's own error.
 */
export async function readRequestHead(r: StreamByteReader, opts?: { onHead?: () => void }): Promise<RequestHead> {
  const head = await r.readHead(MAX_HEAD_BYTES);
  opts?.onHead?.();
  const lines = head.split("\r\n");
  const start = lines.shift()!;
  const m = /^([^ ]+) ([^ ]+) HTTP\/1\.([01])$/.exec(start);
  if (!m || !isToken(m[1]!)) throw bad("malformed request line");
  const method = m[1]!;
  const target = percentEncodeTarget(parseTarget(m[2]!).split("#", 1)[0]!);
  const all = parseHeaderLines(lines);

  const hosts = all.filter(([n]) => n.toLowerCase() === "host");
  if (hosts.length !== 1 || !hosts[0]![1]) throw bad("missing or duplicate Host header");
  const host = hosts[0]![1];

  let expectContinue = false;
  const headers: [string, string][] = [];
  for (const [name, value] of all) {
    if (name.toLowerCase() === "expect") {
      if (value.toLowerCase() === "100-continue") expectContinue = true;
      continue;
    }
    headers.push([name, value]);
  }

  return { method, target, headers, host, expectContinue, framing: bodyFraming(all) };
}

/** Reads the body `head.framing` announces (null for none). Throws HttpParseError, or the stream's own error. */
export async function readRequestBody(r: StreamByteReader, head: RequestHead, maxBodyBytes: number): Promise<Uint8Array | null> {
  const framing = head.framing;
  try {
    if (framing === "chunked") return await readChunkedBody(r, maxBodyBytes);
    if (framing === "none") return null;
    if (framing.length > maxBodyBytes) throw new HttpParseError(413, "request body too large");
    return await r.readExactly(framing.length);
  } catch (e) {
    if (e instanceof UnexpectedEof) throw bad("stream ended inside the request body");
    throw e;
  }
}

/**
 * Reads one request: head (64 KiB cap), then the body per its framing.
 * `onHead` runs once the head is in. Throws HttpParseError / UnexpectedEof,
 * or the stream's own error.
 */
export async function readRequest(r: StreamByteReader, opts: { maxBodyBytes: number; onHead?: () => void }): Promise<ParsedRequest> {
  const { framing, ...head } = await readRequestHead(r, opts);
  return { ...head, body: await readRequestBody(r, { ...head, framing }, opts.maxBodyBytes) };
}

/** Request headers minus hop-by-hop, `Connection`-listed, `Expect` and `Content-Length`. */
export function forwardableHeaders(headers: [string, string][]): [string, string][] {
  const listed = connectionTokens(headers);
  return headers.filter(([name]) => {
    const n = name.toLowerCase();
    return !HOP_BY_HOP.has(n) && !listed.has(n) && n !== "expect" && n !== "content-length";
  });
}

/** A web `Request` for the handler: URL `https://<Host><target>`, forwardable headers, the body unless GET/HEAD. Never sets `duplex`. */
export function toWebRequest(p: ParsedRequest, signal?: AbortSignal): Request {
  const headers = new Headers();
  for (const [name, value] of forwardableHeaders(p.headers)) headers.append(name, value);
  const init: RequestInit = { method: p.method, headers };
  if (p.body !== null && p.method !== "GET" && p.method !== "HEAD") init.body = p.body as BodyInit;
  if (signal) init.signal = signal;
  return new Request(`https://${p.host}${p.target}`, init);
}

/** For the fake edge: a request head in Go `Request.Write` shape (Host first, then the headers, then the framing header). */
export function serializeRequestHead(
  method: string,
  target: string,
  host: string,
  headers: Headers,
  framing: "none" | { length: number } | "chunked",
): Uint8Array {
  let head = `${method} ${target} HTTP/1.1\r\nHost: ${host}\r\n`;
  headers.forEach((value, name) => {
    const n = name.toLowerCase();
    if (n === "host" || n === "content-length" || n === "transfer-encoding") return;
    head += `${name}: ${value}\r\n`;
  });
  if (framing === "chunked") head += "Transfer-Encoding: chunked\r\n";
  else if (framing !== "none") head += `Content-Length: ${framing.length}\r\n`;
  head += "\r\n";
  return latin1Encode(head);
}
