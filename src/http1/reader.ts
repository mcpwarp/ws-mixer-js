/** Buffered byte reader over a stream's `read()`, for parsing HTTP/1.1 heads and bodies. */
import { concatBytes, latin1Decode } from "./headers.js";

/** Anything with MixerStream's `read()` shape. */
export interface ByteSource {
  read(): Promise<Uint8Array | null>;
}

/** A malformed or over-limit HTTP message; `status` is the reply it maps to. */
export class HttpParseError extends Error {
  override name = "HttpParseError";
  constructor(
    readonly status: 400 | 413 | 431,
    message: string,
  ) {
    super(message);
  }
}

/** The stream ended (clean EOF) in the middle of a message. */
export class UnexpectedEof extends Error {
  override name = "UnexpectedEof";
  constructor(message = "unexpected end of stream") {
    super(message);
  }
}

const CR = 13;
const LF = 10;

export class StreamByteReader {
  private buf: Uint8Array = new Uint8Array(0);
  private eof = false;

  constructor(private readonly source: ByteSource) {}

  /** Bytes read from the source but not yet consumed. */
  buffered(): number {
    return this.buf.length;
  }

  private async fill(): Promise<boolean> {
    if (this.eof) return false;
    const chunk = await this.source.read();
    if (chunk === null) {
      this.eof = true;
      return false;
    }
    this.buf = this.buf.length === 0 ? chunk : concatBytes([this.buf, chunk]);
    return true;
  }

  private take(n: number): Uint8Array {
    const out = this.buf.subarray(0, n);
    this.buf = this.buf.subarray(n);
    return out;
  }

  /**
   * Reads up to the blank line ending a message head and returns the head
   * (latin1, without the blank line); leftover bytes stay buffered. Throws
   * HttpParseError(431) past `maxBytes` without the terminator,
   * UnexpectedEof on EOF before it.
   */
  async readHead(maxBytes: number): Promise<string> {
    let scanFrom = 0;
    for (;;) {
      for (let i = Math.max(0, scanFrom - 3); i + 3 < this.buf.length; i++) {
        if (this.buf[i] === CR && this.buf[i + 1] === LF && this.buf[i + 2] === CR && this.buf[i + 3] === LF) {
          if (i > maxBytes) throw new HttpParseError(431, "request head too large");
          const head = latin1Decode(this.buf.subarray(0, i));
          this.buf = this.buf.subarray(i + 4);
          return head;
        }
      }
      if (this.buf.length > maxBytes) throw new HttpParseError(431, "request head too large");
      scanFrom = this.buf.length;
      if (!(await this.fill())) throw new UnexpectedEof("stream ended before the end of the message head");
    }
  }

  /** Exactly `n` bytes; UnexpectedEof if the stream ends first. */
  async readExactly(n: number): Promise<Uint8Array> {
    while (this.buf.length < n) {
      if (!(await this.fill())) throw new UnexpectedEof();
    }
    return this.take(n).slice();
  }

  /** One line up to CRLF (latin1, CRLF excluded). HttpParseError(400) past `maxBytes`. */
  async readLine(maxBytes: number): Promise<string> {
    let scanFrom = 0;
    for (;;) {
      for (let i = Math.max(0, scanFrom - 1); i + 1 < this.buf.length; i++) {
        if (this.buf[i] === CR && this.buf[i + 1] === LF) {
          const line = latin1Decode(this.buf.subarray(0, i));
          this.buf = this.buf.subarray(i + 2);
          return line;
        }
      }
      if (this.buf.length > maxBytes) throw new HttpParseError(400, "line too long");
      scanFrom = this.buf.length;
      if (!(await this.fill())) throw new UnexpectedEof();
    }
  }

  /** The next available bytes (buffered first), or null at EOF. */
  async readChunk(): Promise<Uint8Array | null> {
    if (this.buf.length === 0 && !(await this.fill())) return null;
    return this.take(this.buf.length);
  }
}
