/**
 * ws-mixer.v1 error code table (OVERVIEW.md section 2.8). Shared by stream
 * RESET frames and the connection-level `error` control message.
 *
 * 0x0000_0000-0x0000_0fff is reserved for ws-mixer; codes >= 0x1000_0000 are
 * free for the layer above and are never produced by this package. Unknown
 * codes received on the wire are treated as INTERNAL_ERROR (see codeName).
 */
export const ErrorCode = {
  NO_ERROR: 0x00,
  PROTOCOL_ERROR: 0x01,
  INTERNAL_ERROR: 0x02,
  FLOW_CONTROL_ERROR: 0x03,
  FRAME_SIZE_ERROR: 0x04,
  STREAM_CLOSED: 0x05,
  REFUSED_STREAM: 0x06,
  CANCEL: 0x07,
  STREAM_LIMIT: 0x08,
  ENHANCE_YOUR_CALM: 0x09,
  UNSUPPORTED: 0x0a,
  UNAUTHORIZED: 0x0b,
  GOING_AWAY: 0x0c,
  KEEPALIVE_TIMEOUT: 0x0d,
  // Connection-level only, never emitted by ws-mixer itself: exists for the
  // application above to close a connection for its own reason, carried in
  // error.message / the WS close reason (WS close 4014).
  APPLICATION_CLOSE: 0x0e,
} as const;

export type ErrorCodeValue = (typeof ErrorCode)[keyof typeof ErrorCode];

const codeNames: Record<number, string> = {
  [ErrorCode.NO_ERROR]: "NO_ERROR",
  [ErrorCode.PROTOCOL_ERROR]: "PROTOCOL_ERROR",
  [ErrorCode.INTERNAL_ERROR]: "INTERNAL_ERROR",
  [ErrorCode.FLOW_CONTROL_ERROR]: "FLOW_CONTROL_ERROR",
  [ErrorCode.FRAME_SIZE_ERROR]: "FRAME_SIZE_ERROR",
  [ErrorCode.STREAM_CLOSED]: "STREAM_CLOSED",
  [ErrorCode.REFUSED_STREAM]: "REFUSED_STREAM",
  [ErrorCode.CANCEL]: "CANCEL",
  [ErrorCode.STREAM_LIMIT]: "STREAM_LIMIT",
  [ErrorCode.ENHANCE_YOUR_CALM]: "ENHANCE_YOUR_CALM",
  [ErrorCode.UNSUPPORTED]: "UNSUPPORTED",
  [ErrorCode.UNAUTHORIZED]: "UNAUTHORIZED",
  [ErrorCode.GOING_AWAY]: "GOING_AWAY",
  [ErrorCode.KEEPALIVE_TIMEOUT]: "KEEPALIVE_TIMEOUT",
  [ErrorCode.APPLICATION_CLOSE]: "APPLICATION_CLOSE",
};

const namesToCode: Record<string, number> = Object.fromEntries(
  Object.entries(codeNames).map(([code, name]) => [name, Number(code)]),
);

/** Renders an error code's wire name, e.g. "FLOW_CONTROL_ERROR". Unrecognized codes render "INTERNAL_ERROR". */
export function codeName(code: number): string {
  return codeNames[code] ?? "INTERNAL_ERROR";
}

/** Looks up an error code by its wire name (e.g. "STREAM_LIMIT"). Returns undefined if unknown. */
export function parseErrorCode(name: string): number | undefined {
  return namesToCode[name];
}

/** ws_close = 4000 + error_code, with NO_ERROR mapping to 1000 (OVERVIEW.md section 2.8). */
export function closeCode(code: number): number {
  return code === ErrorCode.NO_ERROR ? 1000 : 4000 + code;
}

/**
 * WsMixerError is the single error type this package throws or emits.
 * `fatal` marks a close code the client SDK must never reconnect after
 * (UNSUPPORTED, UNAUTHORIZED, and the client's own auth/handshake failures).
 */
export class WsMixerError extends Error {
  readonly code: number;
  override name: string = "WsMixerError";
  readonly fatal: boolean;
  readonly streamId?: number;
  /**
   * The peer's observed WS close-frame code, set only when this error was
   * built from an actually-observed close frame (e.g. MixerConn.onSocketClose)
   * rather than a locally-raised protocol violation.
   */
  readonly wsCode?: number;
  /** The peer's observed WS close-frame reason, verbatim, under the same condition as `wsCode`. */
  readonly closeReason?: string;

  constructor(
    code: number,
    message: string,
    opts?: { fatal?: boolean; streamId?: number; wsCode?: number; closeReason?: string },
  ) {
    super(message);
    this.code = code;
    this.fatal = opts?.fatal ?? false;
    this.streamId = opts?.streamId;
    this.wsCode = opts?.wsCode;
    this.closeReason = opts?.closeReason;
  }

  get codeName(): string {
    return codeName(this.code);
  }

  toString(): string {
    return `WsMixerError[${this.codeName}]: ${this.message}`;
  }
}

/** A connection-fatal error: desynchronizes shared connection state. Always error{code} + WS close. */
export class ConnError extends WsMixerError {
  readonly lastStreamId?: number;
  constructor(code: number, message: string, opts?: { streamId?: number; lastStreamId?: number; fatal?: boolean }) {
    super(code, message, opts);
    this.name = "ConnError";
    this.lastStreamId = opts?.lastStreamId;
  }
}

/** A stream-scoped error: produces RESET(code, message) on one stream; the connection stays up. */
export class StreamError extends WsMixerError {
  constructor(code: number, streamId: number, message: string) {
    super(code, message, { streamId });
    this.name = "StreamError";
  }
}
