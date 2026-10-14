/**
 * The mcpwarp control envelope, carried in ws-mixer `app` message bodies
 * (`{"mcpwarp": {...}}`): what the agent sends (register/unregister) and
 * what the tunnel sends back (registered/unregistered/disable/enable/
 * conflict/error).
 * Shapes match mcpwarp-saas/apps/tunnel/internal/agentconn and the Go CLI's
 * internal/appproto/envelope.go.
 */
import { McpwarpError } from "./errors.js";

export const MCPWARP_V = 1;
export const DEFAULT_URL = "wss://connect.mcpwarp.io";
/** Service-name syntax; names are also limited to SERVICE_NAME_MAX characters. */
export const SERVICE_NAME_RE = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/;
/** Longest service name the tunnel accepts, in characters. */
export const SERVICE_NAME_MAX = 30;

/**
 * Whether `name` is a service name the tunnel accepts: a string of at most
 * SERVICE_NAME_MAX characters matching SERVICE_NAME_RE (lowercase letters,
 * digits and inner dashes). `expose()` and the tunnel controller refuse
 * anything else before dialing; use this to validate user input up front.
 * False for a non-string.
 */
export function isValidServiceName(name: unknown): name is string {
  return typeof name === "string" && name.length <= SERVICE_NAME_MAX && SERVICE_NAME_RE.test(name);
}
/**
 * What the tunnel accepts as an integration id, checked after lowercasing:
 * zod's `z.uuid()` (RFC 9562/4122 text form, version 1-8, variant 8/9/a/b,
 * plus the nil and max uuids). Anything else it drops silently.
 */
export const INTEGRATION_ID_RE = /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$/;
export type ServiceKind = "http" | "stdio";

/** `registered.errors[].code` values. */
export const RegisterErrorCode = {
  INVALID_NAME: "INVALID_NAME",
  USERNAME_REQUIRED: "USERNAME_REQUIRED",
  QUOTA_EXCEEDED: "QUOTA_EXCEEDED",
  CONFLICT: "CONFLICT",
  SERVER_DISABLED: "SERVER_DISABLED",
  INTERNAL: "INTERNAL",
  NO_RESPONDERS: "NO_RESPONDERS",
  TIMEOUT: "TIMEOUT",
  ABORTED: "ABORTED",
  CONNECTION_LOST: "CONNECTION_LOST",
  MALFORMED_REPLY: "MALFORMED_REPLY",
  BAD_REQUEST: "BAD_REQUEST",
  UNAUTHENTICATED: "UNAUTHENTICATED",
  FORBIDDEN: "FORBIDDEN",
  NOT_IMPLEMENTED: "NOT_IMPLEMENTED",
  UNAVAILABLE: "UNAVAILABLE",
  SERVICE_ERROR: "SERVICE_ERROR",
} as const;

/** Retried on the same connection with backoff; never fatal. */
export const TRANSIENT_REGISTER_ERRORS: ReadonlySet<string> = new Set([
  RegisterErrorCode.INTERNAL,
  RegisterErrorCode.NO_RESPONDERS,
  RegisterErrorCode.TIMEOUT,
  RegisterErrorCode.ABORTED,
  RegisterErrorCode.CONNECTION_LOST,
  RegisterErrorCode.MALFORMED_REPLY,
  RegisterErrorCode.UNAVAILABLE,
  RegisterErrorCode.SERVICE_ERROR,
]);

/** `op:"error"` codes. */
export const AppErrorCode = {
  BAD_REQUEST: "BAD_REQUEST",
  UNSUPPORTED_VERSION: "UNSUPPORTED_VERSION",
  UNKNOWN_OP: "UNKNOWN_OP",
  TOO_MANY_SERVICES: "TOO_MANY_SERVICES",
  OVERLOADED: "OVERLOADED",
  CONNECTION_LIMIT: "CONNECTION_LIMIT",
} as const;

export interface RegisteredService {
  name: string;
  id: string;
  url: string;
  created: boolean;
}

export interface ServiceError {
  name: string;
  code: string;
  message: string;
  id?: string;
}

export type InboundEnvelope =
  | { op: "registered"; services: RegisteredService[]; errors: ServiceError[] }
  | { op: "unregistered"; services: { name: string; id: string }[] }
  | { op: "disable"; id: string; reason: string }
  | { op: "enable"; id: string; name: string }
  /** Unsolicited: another connection of the same account registered `name` and took the service over. */
  | { op: "conflict"; id: string; name: string; reason: string }
  | { op: "error"; code: string; message: string }
  | { op: "unknown"; rawOp: unknown; v: unknown };

export function encodeRegister(name: string, kind: ServiceKind): Record<string, unknown> {
  return { mcpwarp: { v: MCPWARP_V, op: "register", services: [{ name, kind }] } };
}

export function encodeUnregister(name: string): Record<string, unknown> {
  return { mcpwarp: { v: MCPWARP_V, op: "unregister", services: [{ name }] } };
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function malformed(message: string): McpwarpError {
  return new McpwarpError("MALFORMED_ENVELOPE", message);
}

function reqString(o: Record<string, unknown>, key: string, where: string): string {
  const v = o[key];
  if (typeof v !== "string") throw malformed(`${where}.${key} must be a string`);
  return v;
}

function optString(o: Record<string, unknown>, key: string, where: string, fallback: string): string {
  const v = o[key];
  if (v === undefined) return fallback;
  if (typeof v !== "string") throw malformed(`${where}.${key} must be a string`);
  return v;
}

function list(o: Record<string, unknown>, key: string, where: string): Record<string, unknown>[] {
  const v = o[key];
  if (v === undefined || v === null) return [];
  if (!Array.isArray(v) || !v.every(isObject)) throw malformed(`${where}.${key} must be an array of objects`);
  return v;
}

/**
 * Decodes an `app` body. undefined when it has no object-valued `mcpwarp`
 * key (not ours: ignore). `v !== 1` or an unknown op -> `{op:"unknown"}`.
 * Throws McpwarpError("MALFORMED_ENVELOPE") for a known op with wrong field
 * types or a `registered` service without a non-empty `url`.
 */
export function decodeEnvelope(body: Record<string, unknown>): InboundEnvelope | undefined {
  const env = body.mcpwarp;
  if (!isObject(env)) return undefined;
  if (env.v !== MCPWARP_V) return { op: "unknown", rawOp: env.op, v: env.v };
  switch (env.op) {
    case "registered":
      return {
        op: "registered",
        services: list(env, "services", "registered").map((s) => {
          const created = s.created ?? false;
          if (typeof created !== "boolean") throw malformed("registered.services[].created must be a boolean");
          const name = reqString(s, "name", "registered.services[]");
          const id = reqString(s, "id", "registered.services[]");
          if (typeof s.url !== "string" || s.url === "") throw malformed("registered.services[].url must be a non-empty string");
          return { name, id, url: s.url, created };
        }),
        errors: list(env, "errors", "registered").map((e) => {
          const out: ServiceError = {
            name: reqString(e, "name", "registered.errors[]"),
            code: reqString(e, "code", "registered.errors[]"),
            message: optString(e, "message", "registered.errors[]", ""),
          };
          if (e.id !== undefined) out.id = reqString(e, "id", "registered.errors[]");
          return out;
        }),
      };
    case "unregistered":
      return {
        op: "unregistered",
        services: list(env, "services", "unregistered").map((s) => ({
          name: reqString(s, "name", "unregistered.services[]"),
          id: optString(s, "id", "unregistered.services[]", ""),
        })),
      };
    case "disable":
      return { op: "disable", id: reqString(env, "id", "disable"), reason: optString(env, "reason", "disable", "disabled") };
    case "enable":
      return { op: "enable", id: reqString(env, "id", "enable"), name: reqString(env, "name", "enable") };
    case "conflict":
      return {
        op: "conflict",
        id: reqString(env, "id", "conflict"),
        name: reqString(env, "name", "conflict"),
        reason: optString(env, "reason", "conflict", "taken_over"),
      };
    case "error":
      return { op: "error", code: reqString(env, "code", "error"), message: optString(env, "message", "error", "") };
    default:
      return { op: "unknown", rawOp: env.op, v: env.v };
  }
}
