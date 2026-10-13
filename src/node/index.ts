/**
 * Public API of @mcpwarp/ws-mixer/node: the Node-only adapters. `expose()`
 * here also accepts `server` (an `http.Server` or a `(req, res)` listener);
 * `handler`/`target` behave exactly as in `@mcpwarp/ws-mixer/mcpwarp`, whose
 * whole API is re-exported here (this `expose` replaces its own).
 */
import type { RequestListener, Server } from "node:http";
import { expose as exposeNeutral, exposeWithServer, type ExposeBaseOptions, type ExposeHandle, type ExposeOptions } from "../mcpwarp/expose.js";
import { detectRuntime } from "../runtime.js";
import { serveWithNodeServer } from "./server.js";

export interface NodeExposeOptions extends ExposeBaseOptions {
  server: Server | RequestListener;
}

export function expose(opts: ExposeOptions | NodeExposeOptions): Promise<ExposeHandle> {
  if ("server" in opts && opts.server !== undefined) {
    const o = opts as NodeExposeOptions & { handler?: unknown; target?: unknown };
    if (o.handler !== undefined || o.target !== undefined) throw new TypeError("mcpwarp: pass exactly one of `handler`, `target` or `server`");
    if ((o as { targetTimeoutMs?: unknown }).targetTimeoutMs !== undefined) throw new TypeError("mcpwarp: `targetTimeoutMs` applies only to `target`");
    const server = o.server;
    if (typeof server !== "function" && typeof (server as Server)?.emit !== "function") {
      throw new TypeError("mcpwarp: `server` must be an http.Server or a (req, res) listener");
    }
    // Bun's node:http ignores sockets passed via emit("connection"): requests would hang.
    if (detectRuntime().runtime.startsWith("bun/")) {
      throw new Error(
        "mcpwarp: `server` is not supported on Bun (its node:http ignores sockets handed over via emit(\"connection\")); listen on localhost and pass `target` (or a fetch-style `handler`) instead",
      );
    }
    let destroyAll = () => {};
    return exposeWithServer(
      o,
      (ctx) => {
        const s = serveWithNodeServer(server, ctx);
        destroyAll = s.destroyAll;
        return s;
      },
      { onClose: () => destroyAll() },
    );
  }
  return exposeNeutral(opts as ExposeOptions);
}

export { toDuplex } from "./duplex.js";
export type { DuplexSource } from "./duplex.js";
export { serveWithNodeServer } from "./server.js";
export type { NodeStreamServer, ServeContext, StreamServer } from "./server.js";
// Listed rather than `export *`: ./mcpwarp's own `expose` must not shadow the one above.
export {
  McpwarpError,
  createTunnel,
  RegisterErrorCode,
  AppErrorCode,
  TRANSIENT_REGISTER_ERRORS,
  DEFAULT_URL,
  SERVICE_NAME_RE,
  encodeRegister,
  encodeUnregister,
  decodeEnvelope,
} from "../mcpwarp/index.js";
export type {
  ExposeOptions,
  ExposeBaseOptions,
  ExposeHandle,
  AbortSignalLike,
  Tunnel,
  TunnelBase,
  TunnelConfig,
  TunnelState,
  TunnelStatus,
  ExposeState,
  ExposeRegistration,
  ExposeConflict,
  FetchHandler,
  ServiceKind,
  RegisteredService,
  ServiceError,
  InboundEnvelope,
} from "../mcpwarp/index.js";
export { ErrorCode, WsMixerError, TokenUnavailableError } from "../errors.js";
export type { ErrorCodeValue } from "../errors.js";
export type { WebSocketFactory } from "../socket.js";
export type { TokenProvider, ReconnectOptions, DisconnectPayload } from "../client.js";
