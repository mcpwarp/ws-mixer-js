// Type-level checks: `npm run typecheck` compiles this file with the DOM lib, so `WebSocket` below is the
// browser/Electron/Zotero constructor the README tells header-less runtimes to wrap.
import { describe, expect, it } from "vitest";
import { webSocketFactory, type WebSocketFactory } from "../src/index.js";

type NodeWebSocket = typeof import("undici-types").WebSocket;

describe("webSocketFactory overloads", () => {
  it("accepts the DOM WebSocket with headers:false and Node's (undici) WebSocket with headers", () => {
    const dom: typeof WebSocket = globalThis.WebSocket;
    const headerless: WebSocketFactory = webSocketFactory(dom, { headers: false });
    const node = globalThis.WebSocket as unknown as NodeWebSocket;
    const withHeaders: WebSocketFactory = webSocketFactory(node);
    const explicit: WebSocketFactory = webSocketFactory(node, { headers: true });
    expect([headerless, withHeaders, explicit].every((f) => typeof f === "function")).toBe(true);
  });

  it("rejects a protocols-only constructor where headers would be passed", () => {
    const dom: typeof WebSocket = globalThis.WebSocket;
    // @ts-expect-error the DOM constructor cannot take {protocols, headers}
    const f = () => webSocketFactory(dom);
    expect(typeof f).toBe("function");
  });
});
