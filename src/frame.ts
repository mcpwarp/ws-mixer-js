/**
 * ws-mixer.v1 mux frame codec (WIRE.md section 2.2-2.4). Mirrors
 * `go/wsmixer/frame.go` exactly: same header layout, same validation, same
 * error split between connection-fatal (ConnError) and stream-scoped
 * (StreamError).
 */
import { ConnError, ErrorCode, StreamError } from "./errors.js";

export const FrameType = {
  OPEN: 0x00,
  DATA: 0x01,
  WINDOW: 0x02,
  CLOSE: 0x03,
  RESET: 0x04,
} as const;

export type FrameTypeValue = (typeof FrameType)[keyof typeof FrameType];

const frameTypeNames: Record<number, string> = {
  [FrameType.OPEN]: "OPEN",
  [FrameType.DATA]: "DATA",
  [FrameType.WINDOW]: "WINDOW",
  [FrameType.CLOSE]: "CLOSE",
  [FrameType.RESET]: "RESET",
};

/** Renders a frame type's wire name, or "UNKNOWN(0xNN)" outside the v1 set. */
export function frameTypeName(t: number): string {
  const name = frameTypeNames[t];
  if (name) return name;
  return `UNKNOWN(0x${t.toString(16).padStart(2, "0")})`;
}

function frameTypeKnown(t: number): boolean {
  return t in frameTypeNames;
}

const FRAME_HEADER_SIZE = 8;
/** Largest legal WebSocket message: 8-byte header plus a 64 KiB payload. */
export const MAX_MESSAGE_SIZE = FRAME_HEADER_SIZE + 65536;
/** Control-channel (stream 0) message size cap. */
export const MAX_STREAM_ZERO_PAYLOAD = 16384;
/** Recommended DATA chunk size: a sender-side default, not a wire limit. */
export const MAX_CHUNK = 16384;
const STREAM_ID_HIGH_BIT = 0x8000_0000;
/** Largest legal cumulative send-credit window: 2^31-1 (WIRE.md section 2.6 decision 1). */
export const MAX_SEND_WINDOW = 0x7fff_ffff;

/** One decoded ws-mixer mux frame: the 8-byte header plus its payload. */
export interface Frame {
  type: number;
  flags: number;
  streamId: number;
  payload: Uint8Array;
}

/**
 * Decodes one WebSocket message into a Frame per WIRE.md sections
 * 2.2-2.4. Throws ConnError (connection-fatal) or StreamError (scoped to the
 * frame's stream id).
 */
export function decodeFrame(msg: Uint8Array): Frame {
  if (msg.length < FRAME_HEADER_SIZE) {
    throw new ConnError(
      ErrorCode.PROTOCOL_ERROR,
      `frame header too short: ${msg.length} bytes, need at least ${FRAME_HEADER_SIZE}`,
    );
  }
  if (msg.length > MAX_MESSAGE_SIZE) {
    throw new ConnError(
      ErrorCode.FRAME_SIZE_ERROR,
      `message of ${msg.length} bytes exceeds the ${MAX_MESSAGE_SIZE} byte limit`,
    );
  }

  const view = new DataView(msg.buffer, msg.byteOffset, msg.byteLength);
  const type = view.getUint8(0);
  const flags = view.getUint8(1);
  // bytes 2..4 are reserved; ignore on receive.
  const streamId = view.getUint32(4);
  const payload = msg.subarray(FRAME_HEADER_SIZE);

  if ((streamId & STREAM_ID_HIGH_BIT) !== 0) {
    throw new ConnError(ErrorCode.PROTOCOL_ERROR, `stream id 0x${streamId.toString(16)} has the reserved high bit set`);
  }

  const f: Frame = { type, flags, streamId, payload };

  if (!frameTypeKnown(type)) {
    // Unknown type: structurally accepted. Caller ignores it and counts it.
    return f;
  }

  if (
    (type === FrameType.OPEN || type === FrameType.CLOSE || type === FrameType.WINDOW || type === FrameType.RESET) &&
    streamId === 0
  ) {
    throw new ConnError(ErrorCode.PROTOCOL_ERROR, `${frameTypeName(type)} is not legal on stream 0 (control channel)`);
  }

  switch (type) {
    case FrameType.OPEN:
      if (streamId % 2 === 0) {
        throw new ConnError(
          ErrorCode.PROTOCOL_ERROR,
          `OPEN for even stream id ${streamId}: only the server opens streams and server ids are always odd`,
        );
      }
      break;

    case FrameType.DATA:
      if (streamId === 0 && payload.length > MAX_STREAM_ZERO_PAYLOAD) {
        throw new ConnError(
          ErrorCode.ENHANCE_YOUR_CALM,
          `stream 0 payload of ${payload.length} bytes exceeds the ${MAX_STREAM_ZERO_PAYLOAD} byte control-channel limit`,
        );
      }
      break;

    case FrameType.WINDOW: {
      if (payload.length !== 4) {
        throw new ConnError(ErrorCode.FRAME_SIZE_ERROR, `WINDOW payload is ${payload.length} bytes, must be exactly 4`);
      }
      const increment = new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getUint32(0);
      if (increment === 0) {
        throw new StreamError(ErrorCode.PROTOCOL_ERROR, streamId, "WINDOW increment of 0 is not legal (range is 1..2^31-1)");
      }
      if ((increment & STREAM_ID_HIGH_BIT) !== 0) {
        throw new ConnError(ErrorCode.FLOW_CONTROL_ERROR, `WINDOW increment ${increment} would push the send window past 2^31-1`);
      }
      break;
    }

    case FrameType.RESET:
      if (payload.length < 4) {
        throw new ConnError(ErrorCode.FRAME_SIZE_ERROR, `RESET payload is ${payload.length} bytes, must be at least 4`);
      }
      break;
  }

  return f;
}

/** Returns the 4-byte increment carried by a WINDOW frame. Caller must have checked f.type === FrameType.WINDOW. */
export function windowIncrement(f: Frame): number {
  return new DataView(f.payload.buffer, f.payload.byteOffset, f.payload.byteLength).getUint32(0);
}

/** Returns the error code carried by a RESET frame. Caller must have checked f.type === FrameType.RESET. */
export function resetCode(f: Frame): number {
  return new DataView(f.payload.buffer, f.payload.byteOffset, 4).getUint32(0);
}

const utf8Decoder = new TextDecoder("utf-8", { fatal: false });

/**
 * Returns the (UTF-8 sanitized) message carried by a RESET frame. Invalid
 * UTF-8 is replaced with U+FFFD by TextDecoder, matching the wire spec's
 * "replace chars, do not kill the connection" rule.
 */
export function resetMessage(f: Frame): string {
  return utf8Decoder.decode(f.payload.subarray(4));
}

/** Serializes a Frame to its wire form: 8-byte header followed by the payload. */
export function encodeFrame(f: { type: number; streamId: number; payload?: Uint8Array }): Uint8Array {
  const payload = f.payload ?? new Uint8Array(0);
  const out = new Uint8Array(FRAME_HEADER_SIZE + payload.length);
  const view = new DataView(out.buffer);
  view.setUint8(0, f.type);
  view.setUint8(1, 0);
  view.setUint16(2, 0);
  view.setUint32(4, f.streamId >>> 0);
  out.set(payload, FRAME_HEADER_SIZE);
  return out;
}

export function encodeOpen(streamId: number): Uint8Array {
  return encodeFrame({ type: FrameType.OPEN, streamId });
}

export function encodeData(streamId: number, payload: Uint8Array): Uint8Array {
  return encodeFrame({ type: FrameType.DATA, streamId, payload });
}

export function encodeWindow(streamId: number, increment: number): Uint8Array {
  const payload = new Uint8Array(4);
  new DataView(payload.buffer).setUint32(0, increment >>> 0);
  return encodeFrame({ type: FrameType.WINDOW, streamId, payload });
}

export function encodeClose(streamId: number): Uint8Array {
  return encodeFrame({ type: FrameType.CLOSE, streamId });
}

const utf8Encoder = new TextEncoder();

export function encodeReset(streamId: number, code: number, message = ""): Uint8Array {
  const msgBytes = utf8Encoder.encode(message);
  const payload = new Uint8Array(4 + msgBytes.length);
  new DataView(payload.buffer).setUint32(0, code >>> 0);
  payload.set(msgBytes, 4);
  return encodeFrame({ type: FrameType.RESET, streamId, payload });
}
