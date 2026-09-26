/**
 * ws-mixer.v1 control channel (stream 0) message types and hand-written
 * validators (WIRE.md section 2.7). Mirrors `go/wsmixer/control.go` and
 * `control_messages.go`: dispatch on `t`, then explicit per-field checks —
 * never a JSON Schema validator on the runtime path (decision 9).
 */
import { ConnError, ErrorCode } from "./errors.js";
import { MAX_STREAM_ZERO_PAYLOAD } from "./frame.js";

const T_HELLO = "hello";
const T_WELCOME = "welcome";
const T_PING = "ping";
const T_PONG = "pong";
const T_DRAIN = "drain";
const T_ERROR = "error";
const T_APP = "app";

// Field bounds from WIRE.md section 2.7's field tables.
const MAX_TOKEN_LEN = 4096;
const MAX_AGENT_FIELD_LEN = 128;
const WINDOW_MIN = 16384;
const WINDOW_MAX = 0x7fff_ffff;
const MAX_STREAMS_MIN = 1;
const MAX_STREAMS_MAX = 100000;
const MAX_SESSION_LEN = 64;
const PING_INTERVAL_MIN = 5000;
const MAX_ERROR_MESSAGE = 1024;
const MAX_DRAIN_MESSAGE = 256;
const MAX_ID_VALUE = Number.MAX_SAFE_INTEGER; // 2^53-1
const MAX_STREAM_ID_VALUE = 0x7fff_ffff; // 31-bit stream ids

const CAPABILITY_RE = /^[a-z0-9_.-]{1,64}$/;

export interface AgentInfo {
  sdk: string;
  sdk_version: string;
  runtime?: string;
  os?: string;
}

export interface ServerInfo {
  name?: string;
  version?: string;
  node?: string;
}

export interface HelloMsg {
  t: "hello";
  v: number;
  token: string;
  agent: AgentInfo;
  window?: number;
  max_streams?: number;
  capabilities?: string[];
  meta?: unknown;
}

export interface WelcomeMsg {
  t: "welcome";
  v: number;
  session: string;
  window: number;
  max_streams: number;
  ping_interval: number;
  ping_timeout: number;
  server?: ServerInfo;
  capabilities?: string[];
  meta?: unknown;
}

export interface PingMsg {
  t: "ping";
  id: number;
  ts?: number;
}

export interface PongMsg {
  t: "pong";
  id: number;
  ts?: number;
}

export interface DrainMsg {
  t: "drain";
  reason: string;
  last_stream_id: number;
  deadline_ms?: number;
  retry_after_ms?: number;
  message?: string;
}

export interface ErrorMsg {
  t: "error";
  code: number;
  message: string;
  stream_id?: number;
  last_stream_id?: number;
}

export interface AppMsg {
  t: "app";
  body: Record<string, unknown>;
}

export type ControlMessage = HelloMsg | WelcomeMsg | PingMsg | PongMsg | DrainMsg | ErrorMsg | AppMsg;

const KNOWN_DRAIN_REASONS = new Set(["rollout", "overload", "id_exhausted", "replaced", "maintenance", "client_requested"]);

/** Reports whether reason is one of the documented drain reasons. An unknown reason is tolerated at runtime. */
export function knownDrainReason(reason: string): boolean {
  return KNOWN_DRAIN_REASONS.has(reason);
}

function connErr(message: string, code: number = ErrorCode.PROTOCOL_ERROR): ConnError {
  return new ConnError(code, message);
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function jsonKind(v: unknown): string {
  if (v === null) return "null";
  if (typeof v === "boolean") return "boolean";
  if (typeof v === "number") return "number";
  if (typeof v === "string") return "string";
  if (Array.isArray(v)) return "array";
  if (typeof v === "object") return "object";
  return typeof v;
}

function requireField(top: Record<string, unknown>, name: string): unknown {
  if (!(name in top)) throw connErr(`missing required field "${name}"`);
  return top[name];
}

function fieldString(raw: unknown, name: string): string {
  if (typeof raw !== "string") throw connErr(`field "${name}" must be a string`);
  return raw;
}

/**
 * A JSON integer field. Rejects floats and non-finite values ("No floats
 * anywhere in the control channel", WIRE.md section 2.7) and rejects
 * anything outside Number's safe integer range.
 */
function fieldInt(raw: unknown, name: string): number {
  if (typeof raw !== "number" || !Number.isFinite(raw)) throw connErr(`field "${name}" must be an integer`);
  if (!Number.isInteger(raw)) throw connErr(`field "${name}" must be an integer, not a float`);
  return raw;
}

function fieldObject(raw: unknown, name: string): Record<string, unknown> {
  if (!isPlainObject(raw)) throw connErr(`field "${name}" must be an object`);
  return raw;
}

function fieldStringArray(raw: unknown, name: string): string[] {
  if (!Array.isArray(raw)) throw connErr(`field "${name}" must be an array of strings`);
  return raw.map((item) => {
    if (typeof item !== "string") throw connErr(`field "${name}" must be an array of strings`);
    return item;
  });
}

function requireString(top: Record<string, unknown>, name: string): string {
  return fieldString(requireField(top, name), name);
}

function requireInt(top: Record<string, unknown>, name: string): number {
  return fieldInt(requireField(top, name), name);
}

function validateCapabilities(caps: string[]): void {
  for (const c of caps) {
    if (!CAPABILITY_RE.test(c)) throw connErr(`capability "${c}" does not match ^[a-z0-9_.-]{1,64}$`);
  }
}

function parseAgentInfo(obj: Record<string, unknown>): AgentInfo {
  const sdk = fieldString(requireField(obj, "sdk"), "agent.sdk");
  const sdk_version = fieldString(requireField(obj, "sdk_version"), "agent.sdk_version");
  if ([...sdk].length > MAX_AGENT_FIELD_LEN || [...sdk_version].length > MAX_AGENT_FIELD_LEN) {
    throw connErr(`agent.sdk / agent.sdk_version must be <= ${MAX_AGENT_FIELD_LEN} characters`);
  }
  const a: AgentInfo = { sdk, sdk_version };
  if ("runtime" in obj) a.runtime = fieldString(obj.runtime, "agent.runtime");
  if ("os" in obj) a.os = fieldString(obj.os, "agent.os");
  return a;
}

function parseHello(top: Record<string, unknown>): HelloMsg {
  const v = requireInt(top, "v");
  if (v !== 1) throw connErr(`hello.v=${v} does not match ws-mixer.v1`, ErrorCode.UNSUPPORTED);

  const token = requireString(top, "token");
  if (token.length < 1 || token.length > MAX_TOKEN_LEN) {
    throw connErr(`hello.token length ${token.length} out of range 1..${MAX_TOKEN_LEN}`);
  }

  const agent = parseAgentInfo(fieldObject(requireField(top, "agent"), "agent"));

  let window = 262144;
  if ("window" in top) {
    window = fieldInt(top.window, "window");
    if (window < WINDOW_MIN || window > WINDOW_MAX) {
      throw connErr(`hello.window ${window} out of range ${WINDOW_MIN}..${WINDOW_MAX}`);
    }
  }

  let max_streams = 64;
  if ("max_streams" in top) {
    max_streams = fieldInt(top.max_streams, "max_streams");
    if (max_streams < MAX_STREAMS_MIN || max_streams > MAX_STREAMS_MAX) {
      throw connErr(`hello.max_streams ${max_streams} out of range ${MAX_STREAMS_MIN}..${MAX_STREAMS_MAX}`);
    }
  }

  const m: HelloMsg = { t: "hello", v, token, agent, window, max_streams };
  if ("capabilities" in top) {
    const capabilities = fieldStringArray(top.capabilities, "capabilities");
    validateCapabilities(capabilities);
    m.capabilities = capabilities;
  }
  if ("meta" in top) m.meta = top.meta;
  return m;
}

function parseWelcome(top: Record<string, unknown>): WelcomeMsg {
  const v = requireInt(top, "v");
  if (v !== 1) throw connErr(`welcome.v=${v} does not match ws-mixer.v1`, ErrorCode.UNSUPPORTED);

  const session = requireString(top, "session");
  if (session.length > MAX_SESSION_LEN) throw connErr(`welcome.session length ${session.length} exceeds ${MAX_SESSION_LEN}`);

  const window = requireInt(top, "window");
  if (window < WINDOW_MIN || window > WINDOW_MAX) throw connErr(`welcome.window ${window} out of range ${WINDOW_MIN}..${WINDOW_MAX}`);

  const max_streams = requireInt(top, "max_streams");
  if (max_streams < MAX_STREAMS_MIN || max_streams > MAX_STREAMS_MAX) {
    throw connErr(`welcome.max_streams ${max_streams} out of range ${MAX_STREAMS_MIN}..${MAX_STREAMS_MAX}`);
  }

  const ping_interval = requireInt(top, "ping_interval");
  if (ping_interval < PING_INTERVAL_MIN) throw connErr(`welcome.ping_interval ${ping_interval} is below the ${PING_INTERVAL_MIN}ms floor`);

  const ping_timeout = requireInt(top, "ping_timeout");
  if (ping_timeout < 2 * ping_interval) {
    throw connErr(`welcome.ping_timeout (${ping_timeout}) must be at least 2x ping_interval (${ping_interval})`);
  }

  const m: WelcomeMsg = { t: "welcome", v, session, window, max_streams, ping_interval, ping_timeout };

  if ("server" in top) {
    const obj = fieldObject(top.server, "server");
    const si: ServerInfo = {};
    if ("name" in obj) si.name = fieldString(obj.name, "server.name");
    if ("version" in obj) si.version = fieldString(obj.version, "server.version");
    if ("node" in obj) si.node = fieldString(obj.node, "server.node");
    m.server = si;
  }
  if ("capabilities" in top) {
    const capabilities = fieldStringArray(top.capabilities, "capabilities");
    validateCapabilities(capabilities);
    m.capabilities = capabilities;
  }
  if ("meta" in top) m.meta = top.meta;
  return m;
}

function parsePingPong(top: Record<string, unknown>, t: "ping" | "pong"): PingMsg | PongMsg {
  const id = requireInt(top, "id");
  if (id < 0 || id > MAX_ID_VALUE) throw connErr(`${t}.id ${id} out of range 0..2^53-1`);
  const m: PingMsg | PongMsg = { t, id };
  if ("ts" in top) m.ts = fieldInt(top.ts, "ts");
  return m;
}

function parseDrain(top: Record<string, unknown>): DrainMsg {
  const reason = requireString(top, "reason");
  const lastStreamId = requireInt(top, "last_stream_id");
  if (lastStreamId < 0 || lastStreamId > MAX_STREAM_ID_VALUE) {
    throw connErr(`drain.last_stream_id ${lastStreamId} out of range 0..2^31-1`);
  }
  const m: DrainMsg = { t: "drain", reason, last_stream_id: lastStreamId };
  if ("deadline_ms" in top) m.deadline_ms = fieldInt(top.deadline_ms, "deadline_ms");
  if ("retry_after_ms" in top) m.retry_after_ms = fieldInt(top.retry_after_ms, "retry_after_ms");
  if ("message" in top) {
    m.message = fieldString(top.message, "message");
    if (m.message.length > MAX_DRAIN_MESSAGE) throw connErr(`drain.message length ${m.message.length} exceeds ${MAX_DRAIN_MESSAGE}`);
  }
  return m;
}

function parseErrorMsg(top: Record<string, unknown>): ErrorMsg {
  const code = requireInt(top, "code");
  if (code < 0 || code > 0xffffffff) throw connErr(`error.code ${code} out of range`);
  const message = requireString(top, "message");
  if (message.length > MAX_ERROR_MESSAGE) throw connErr(`error.message length ${message.length} exceeds ${MAX_ERROR_MESSAGE}`);
  const m: ErrorMsg = { t: "error", code, message };
  if ("stream_id" in top) {
    const v = fieldInt(top.stream_id, "stream_id");
    if (v < 0 || v > MAX_STREAM_ID_VALUE) throw connErr(`error.stream_id ${v} out of range 0..2^31-1`);
    m.stream_id = v;
  }
  if ("last_stream_id" in top) {
    const v = fieldInt(top.last_stream_id, "last_stream_id");
    if (v < 0 || v > MAX_STREAM_ID_VALUE) throw connErr(`error.last_stream_id ${v} out of range 0..2^31-1`);
    m.last_stream_id = v;
  }
  return m;
}

function parseApp(top: Record<string, unknown>): AppMsg {
  const body = requireField(top, "body");
  if (!isPlainObject(body)) throw connErr(`app.body must be a JSON object, got ${jsonKind(body)}`);
  return { t: "app", body };
}

/**
 * Validates and decodes one stream-0 DATA payload (already UTF-8 decoded to
 * a string). Implements WIRE.md section 2.7's envelope table plus every
 * message type's per-field checks. Every validation failure is a ConnError
 * with PROTOCOL_ERROR (or ENHANCE_YOUR_CALM for the oversize case).
 */
export function parseControl(raw: string | Uint8Array): ControlMessage {
  const bytes = typeof raw === "string" ? utf8Encoder.encode(raw) : raw;
  if (bytes.length > MAX_STREAM_ZERO_PAYLOAD) {
    throw new ConnError(
      ErrorCode.ENHANCE_YOUR_CALM,
      `stream 0 payload of ${bytes.length} bytes exceeds the ${MAX_STREAM_ZERO_PAYLOAD} byte control-channel limit`,
    );
  }
  let text: string;
  if (typeof raw === "string") {
    text = raw;
  } else {
    try {
      text = utf8Decoder.decode(raw);
    } catch (e) {
      throw connErr(`malformed UTF-8 on stream 0: ${(e as Error).message}`);
    }
  }

  let top: unknown;
  try {
    top = JSON.parse(text);
  } catch (e) {
    throw connErr(`malformed JSON on stream 0: ${(e as Error).message}`);
  }
  if (!isPlainObject(top)) {
    throw connErr(`control message must be a JSON object, got ${jsonKind(top)}`);
  }

  if (!("t" in top)) throw connErr(`control message missing required field "t"`);
  const t = fieldString(top.t, "t");

  switch (t) {
    case T_HELLO:
      return parseHello(top);
    case T_WELCOME:
      return parseWelcome(top);
    case T_PING:
      return parsePingPong(top, "ping");
    case T_PONG:
      return parsePingPong(top, "pong");
    case T_DRAIN:
      return parseDrain(top);
    case T_ERROR:
      return parseErrorMsg(top);
    case T_APP:
      return parseApp(top);
    default:
      throw connErr(`unknown control message type "${t}"`);
  }
}

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

/** Serializes a control message to its stream-0 DATA payload bytes. */
export function encodeControl(msg: ControlMessage): Uint8Array {
  return utf8Encoder.encode(JSON.stringify(msg));
}
