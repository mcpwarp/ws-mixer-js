/** WHATWG streams view of a MixerStream, for code that wants ReadableStream/WritableStream. */
import type { MixerStream } from "./stream.js";

/**
 * Wraps `stream` as `{ readable, writable }`. Cancelling the readable or
 * aborting the writable RESETs the stream with CANCEL; closing the writable
 * half-closes it (closeWrite). Uses the global ReadableStream/WritableStream
 * at call time; throws TypeError where they are missing.
 */
export function toWebStreams(stream: MixerStream): { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> } {
  const g = globalThis as Record<string, any>;
  const RS = g.ReadableStream as typeof ReadableStream | undefined;
  const WS = g.WritableStream as typeof WritableStream | undefined;
  if (typeof RS !== "function" || typeof WS !== "function") {
    throw new TypeError("toWebStreams needs global ReadableStream/WritableStream");
  }
  const readable = new RS<Uint8Array>({
    async pull(controller) {
      try {
        const chunk = await stream.read();
        if (chunk === null) controller.close();
        else controller.enqueue(chunk);
      } catch (e) {
        controller.error(e);
      }
    },
    cancel(reason) {
      stream.reset("CANCEL", String(reason ?? "cancelled"));
    },
  });
  const writable = new WS<Uint8Array>({
    write(chunk) {
      return stream.write(chunk);
    },
    close() {
      return stream.closeWrite();
    },
    abort(reason) {
      stream.reset("CANCEL", String(reason ?? "aborted"));
    },
  });
  return { readable, writable };
}
