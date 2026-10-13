// Type-level checks: `npm run typecheck` compiles this file, so a /node consumer can import the whole
// ./mcpwarp API plus the root types it needs from `@mcpwarp/ws-mixer/node` alone.
import { describe, expect, it } from "vitest";
import {
  AppErrorCode,
  ErrorCode,
  McpwarpError,
  RegisterErrorCode,
  TokenUnavailableError,
  WsMixerError,
  expose,
  type DisconnectPayload,
  type ErrorCodeValue,
  type ExposeBaseOptions,
  type ExposeConflict,
  type ExposeHandle,
  type ExposeOptions,
  type ExposeRegistration,
  type ExposeState,
  type FetchHandler,
  type NodeExposeOptions,
  type ReconnectOptions,
  type TokenProvider,
  type WebSocketFactory,
} from "../src/node/index.js";
import * as node from "../src/node/index.js";
import * as mcpwarp from "../src/mcpwarp/index.js";
import * as root from "../src/index.js";

describe("@mcpwarp/ws-mixer/node re-exports", () => {
  it("every runtime export of ./mcpwarp is re-exported", () => {
    for (const name of Object.keys(mcpwarp)) expect(Object.keys(node), name).toContain(name);
    for (const name of Object.keys(mcpwarp)) if (name !== "expose") expect((node as Record<string, unknown>)[name], name).toBe((mcpwarp as Record<string, unknown>)[name]);
  });

  it("re-exports ./mcpwarp (same classes and consts) and the root error classes", () => {
    expect(McpwarpError).toBe(mcpwarp.McpwarpError);
    expect(RegisterErrorCode).toBe(mcpwarp.RegisterErrorCode);
    expect(AppErrorCode).toBe(mcpwarp.AppErrorCode);
    expect(ErrorCode).toBe(root.ErrorCode);
    expect(WsMixerError).toBe(root.WsMixerError);
    expect(TokenUnavailableError).toBe(root.TokenUnavailableError);
    expect(expose).not.toBe(mcpwarp.expose);
  });

  it("types line up with ./mcpwarp and the root", () => {
    const webSocket: WebSocketFactory = root.defaultWebSocketFactory;
    const token: TokenProvider = () => "t";
    const reconnect: ReconnectOptions = { maxAttempts: 1 };
    const handler: FetchHandler = () => new Response();
    const base: ExposeBaseOptions = { token, name: "app", integration: "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10", webSocket, reconnect };
    const viaHandler: ExposeOptions = { ...base, handler };
    const viaServer: NodeExposeOptions = { ...base, server: () => {} };
    const asMcpwarp: mcpwarp.ExposeOptions = viaHandler;
    const state: ExposeState = "conflict";
    const reg: ExposeRegistration | undefined = undefined as ExposeHandle["registration"];
    const conflict: ExposeConflict = { serverId: "srv", name: "app", reason: "taken_over" };
    const asMcpwarpConflict: mcpwarp.ExposeConflict = conflict;
    const onDisconnect = (r: DisconnectPayload): boolean => r.fatal;
    const code: ErrorCodeValue = ErrorCode.UNAUTHORIZED;
    const asRoot: root.ErrorCodeValue = code;
    expect([asMcpwarp, viaServer, state, reg, asMcpwarpConflict, onDisconnect, asRoot].length).toBe(7);
  });
});
