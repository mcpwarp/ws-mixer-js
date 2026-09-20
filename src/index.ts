/** Public API of @mcpwarp/ws-mixer: the ws-mixer.v1 JS/TypeScript client SDK. */
export { connect, MixerClient, fullJitterDelay, SUBPROTOCOL, SDK_VERSION } from "./client.js";
export type {
  ConnectOptions,
  ReconnectOptions,
  TokenProvider,
  DisconnectReason,
  DisconnectPhase,
  DisconnectPayload,
  CloseOptions,
} from "./client.js";

export { MixerConn } from "./conn.js";
export type { ConnOptions, WSLike } from "./conn.js";

export { MixerStream } from "./stream.js";
export type { StreamState } from "./stream.js";

export { ErrorCode, WsMixerError, ConnError, StreamError, codeName, parseErrorCode, closeCode } from "./errors.js";
export type { ErrorCodeValue } from "./errors.js";

export {
  FrameType,
  decodeFrame,
  encodeFrame,
  encodeOpen,
  encodeData,
  encodeWindow,
  encodeClose,
  encodeReset,
  windowIncrement,
  resetCode,
  resetMessage,
  frameTypeName,
  MAX_MESSAGE_SIZE,
  MAX_STREAM_ZERO_PAYLOAD,
  MAX_CHUNK,
  MAX_SEND_WINDOW,
} from "./frame.js";
export type { Frame, FrameTypeValue } from "./frame.js";

export { parseControl, encodeControl, knownDrainReason } from "./control.js";
export type {
  ControlMessage,
  AgentInfo,
  ServerInfo,
  HelloMsg,
  WelcomeMsg,
  PingMsg,
  PongMsg,
  DrainMsg,
  ErrorMsg,
  AppMsg,
} from "./control.js";
