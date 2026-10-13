/** Small helpers shared by stream.ts, conn.ts and socket.ts. */

/**
 * True for an ArrayBuffer (or SharedArrayBuffer) from any realm. `instanceof
 * ArrayBuffer` is false for one made by another global (a WebSocket from a
 * Firefox chrome window read inside a `Cu.Sandbox`, an iframe, `node:vm`);
 * the built-in toString tag is not. An object spoofing `Symbol.toStringTag`
 * also passes; WebSocket message data can't be one.
 */
export function isArrayBufferAnyRealm(v: unknown): v is ArrayBufferLike {
  const tag = Object.prototype.toString.call(v);
  return tag === "[object ArrayBuffer]" || tag === "[object SharedArrayBuffer]";
}

/** `e.message` when `e` carries a string one (an Error from any realm), else `String(e)`; `fallback` if either throws. */
export function errorMessage(e: unknown, fallback = "unknown error"): string {
  try {
    const message = (e as { message?: unknown } | null | undefined)?.message;
    return typeof message === "string" ? message : String(e);
  } catch {
    return fallback;
  }
}

/** Truncates `message` to at most `maxBytes` UTF-8 bytes, backing off to the nearest character boundary. */
export function truncateUtf8(message: string, maxBytes: number): string {
  const bytes = new TextEncoder().encode(message);
  if (bytes.length <= maxBytes) return message;
  let end = maxBytes;
  while (end > 0 && (bytes[end]! & 0xc0) === 0x80) end--;
  return new TextDecoder().decode(bytes.subarray(0, end));
}
