/** HTTP/1.1 helpers shared by the request/response codecs: hop-by-hop headers, latin1, reason phrases. */

/** Connection-scoped headers never forwarded across a hop (RFC 9110 section 7.6.1, plus the classic proxy set). */
export const HOP_BY_HOP: ReadonlySet<string> = new Set([
  "connection",
  "keep-alive",
  "proxy-connection",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

/** Header names listed in `Connection` (lower-cased), which are hop-by-hop too. */
export function connectionTokens(headers: Iterable<[string, string]>): Set<string> {
  const out = new Set<string>();
  for (const [name, value] of headers) {
    if (name.toLowerCase() !== "connection") continue;
    for (const t of value.split(",")) {
      const token = t.trim().toLowerCase();
      if (token) out.add(token);
    }
  }
  return out;
}

const TOKEN_RE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

/** RFC 9110 token (method and header-name syntax). */
export function isToken(s: string): boolean {
  return TOKEN_RE.test(s);
}

/** Bytes to a string, one char per byte (no TextDecoder label dependence). */
export function latin1Decode(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i += 8192) {
    out += String.fromCharCode.apply(null, bytes.subarray(i, i + 8192) as unknown as number[]);
  }
  return out;
}

/** A string to bytes, one byte per char (high bits dropped). */
export function latin1Encode(s: string): Uint8Array {
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i) & 0xff;
  return out;
}

const REASONS: Record<number, string> = {
  200: "OK",
  201: "Created",
  202: "Accepted",
  204: "No Content",
  206: "Partial Content",
  301: "Moved Permanently",
  302: "Found",
  303: "See Other",
  304: "Not Modified",
  307: "Temporary Redirect",
  308: "Permanent Redirect",
  400: "Bad Request",
  401: "Unauthorized",
  403: "Forbidden",
  404: "Not Found",
  405: "Method Not Allowed",
  406: "Not Acceptable",
  408: "Request Timeout",
  409: "Conflict",
  410: "Gone",
  413: "Content Too Large",
  415: "Unsupported Media Type",
  422: "Unprocessable Content",
  429: "Too Many Requests",
  431: "Request Header Fields Too Large",
  500: "Internal Server Error",
  501: "Not Implemented",
  502: "Bad Gateway",
  503: "Service Unavailable",
  504: "Gateway Timeout",
};

export function reasonPhrase(status: number): string {
  return REASONS[status] ?? "Unknown";
}

export function concatBytes(parts: Uint8Array[]): Uint8Array {
  let len = 0;
  for (const p of parts) len += p.length;
  const out = new Uint8Array(len);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}
