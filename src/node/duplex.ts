/** A Node `Duplex` over a MixerStream, for Node APIs that want a socket-like stream. */
import { Duplex } from "node:stream";
import type { MixerStream } from "../stream.js";

/** The part of a MixerStream the Duplex uses. */
export type DuplexSource = Pick<MixerStream, "read" | "write" | "closeWrite" | "getState" | "reset">;

class MixerDuplex extends Duplex {
  /** Node's HTTP server reads these off its sockets. */
  remoteAddress = "127.0.0.1";
  remotePort = 0;
  private pumping = false;

  constructor(private readonly stream: DuplexSource) {
    super({ allowHalfOpen: true });
  }

  override _read(): void {
    if (this.pumping) return;
    this.pumping = true;
    void this.pump();
  }

  private async pump(): Promise<void> {
    try {
      for (;;) {
        const chunk = await this.stream.read();
        if (chunk === null) {
          this.push(null);
          return;
        }
        if (!this.push(chunk)) {
          this.pumping = false;
          return;
        }
      }
    } catch (e) {
      this.destroy(e as Error);
    }
  }

  override _write(chunk: Uint8Array, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    this.stream.write(chunk).then(() => callback(), callback);
  }

  override _final(callback: (error?: Error | null) => void): void {
    void this.stream.closeWrite().then(() => callback());
  }

  override _destroy(err: Error | null, callback: (error?: Error | null) => void): void {
    if (this.stream.getState() !== "closed") this.stream.reset("CANCEL", err?.message ?? "destroyed");
    callback(err);
  }

  setTimeout(): this {
    return this;
  }

  setNoDelay(): this {
    return this;
  }

  setKeepAlive(): this {
    return this;
  }
}

/**
 * Wraps `stream` as a half-open-capable Node Duplex: reading pulls
 * `stream.read()` (credit is returned as Node consumes), writing awaits
 * `stream.write()`, `end()` half-closes (closeWrite), `destroy()` RESETs
 * with CANCEL unless the stream already ended.
 */
export function toDuplex(stream: DuplexSource): Duplex {
  return new MixerDuplex(stream);
}
