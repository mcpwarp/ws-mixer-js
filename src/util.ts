/** Small helpers shared by stream.ts and conn.ts. */

/** Truncates `message` to at most `maxBytes` UTF-8 bytes, backing off to the nearest character boundary. */
export function truncateUtf8(message: string, maxBytes: number): string {
  const bytes = new TextEncoder().encode(message);
  if (bytes.length <= maxBytes) return message;
  let end = maxBytes;
  while (end > 0 && (bytes[end]! & 0xc0) === 0x80) end--;
  return new TextDecoder().decode(bytes.subarray(0, end));
}
