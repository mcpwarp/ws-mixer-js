/**
 * Runtime/OS detection for `hello.agent`, the `User-Agent` header and
 * `hello.meta.os`. Platform-neutral: every runtime global is looked up
 * through `globalThis` at call time, inside try/catch, and may be absent.
 */

export interface RuntimeInfo {
  /** e.g. "node/22.18.0", "deno/2.8.3", "bun/1.3.3", "browser". */
  runtime: string;
  /** e.g. "darwin/arm64", or "unknown". */
  os: string;
}

const MAX_FIELD = 64;

function cap(s: string): string {
  return s.length > MAX_FIELD ? s.slice(0, MAX_FIELD) : s;
}

function g(): Record<string, any> {
  return globalThis as Record<string, any>;
}

type Kind = "deno" | "bun" | "node" | "electron-renderer" | "other";

function kind(): Kind {
  try {
    if (typeof g().Deno?.version?.deno === "string") return "deno";
  } catch {
    // fall through
  }
  try {
    const p = g().process;
    const versions = p?.versions;
    if (typeof versions?.bun === "string") return "bun";
    if (typeof versions?.node === "string") {
      // Electron renderers/workers (Obsidian, ...) expose `process` but run Chromium's web APIs.
      if (typeof versions.electron === "string" && (p.type === "renderer" || p.type === "worker" || typeof g().document !== "undefined")) {
        return "electron-renderer";
      }
      return "node";
    }
  } catch {
    // fall through
  }
  return "other";
}

function isNativeFunction(fn: unknown): boolean {
  try {
    return /\{\s*\[native code\]\s*\}\s*$/.test(Function.prototype.toString.call(fn));
  } catch {
    return true;
  }
}

export function detectRuntime(): RuntimeInfo {
  try {
    switch (kind()) {
      case "deno": {
        const deno = g().Deno;
        return { runtime: cap(`deno/${deno.version.deno}`), os: cap(`${deno.build?.os ?? "unknown"}/${deno.build?.arch ?? "unknown"}`) };
      }
      case "bun": {
        const p = g().process;
        return { runtime: cap(`bun/${p.versions.bun}`), os: cap(`${p.platform}/${p.arch}`) };
      }
      case "node": {
        const p = g().process;
        return { runtime: cap(`node/${p.versions.node}`), os: cap(`${p.platform}/${p.arch}`) };
      }
      case "electron-renderer": {
        const p = g().process;
        return { runtime: cap(`electron/${p.versions.electron}`), os: cap(`${p.platform}/${p.arch}`) };
      }
      default:
        if (typeof g().navigator?.userAgent === "string") return { runtime: "browser", os: "unknown" };
    }
  } catch {
    // fall through
  }
  return { runtime: "unknown", os: "unknown" };
}

/**
 * The `ws` package's client class (or a subclass): a JS (non-native)
 * constructor with `ping`/`terminate` on the prototype and the statics `ws`'s
 * entry attaches (`createWebSocketStream`, `Server`, `WebSocketServer`). It
 * takes `(url, protocols, { headers })`, not the `{protocols, headers}` init
 * object. Bun's native WebSocket also has `ping`/`terminate` but none of the
 * statics, and only reads headers from the init object.
 */
export function isWsPackageConstructor(ctor: unknown): boolean {
  try {
    const c = ctor as Record<string, any>;
    const proto = c?.prototype;
    return (
      typeof ctor === "function" &&
      typeof proto?.ping === "function" &&
      typeof proto?.terminate === "function" &&
      (typeof c.createWebSocketStream === "function" || typeof c.WebSocketServer === "function" || typeof c.Server === "function") &&
      !isNativeFunction(ctor)
    );
  } catch {
    return false;
  }
}

/**
 * True when `ctor` sends headers on the upgrade: Deno's and Bun's
 * WebSocket and Node's (undici, a JS class) via `{protocols, headers}`, and
 * the `ws` package via its options (see `isWsPackageConstructor`). A
 * host-native constructor under a Node-like `process` (an Electron
 * renderer's Chromium WebSocket) and every browser-like runtime: false.
 */
export function webSocketAcceptsHeaders(ctor: unknown): boolean {
  if (isWsPackageConstructor(ctor)) return true;
  switch (kind()) {
    case "deno":
    case "bun":
      return true;
    case "node":
      return typeof g().document === "undefined" && !isNativeFunction(ctor);
    default:
      return false;
  }
}
