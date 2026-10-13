/**
 * expose(): publish one local MCP server at its public mcpwarp URL. One
 * call = one ws-mixer client = one registered service. Registration follows
 * the connection (re-registered on every welcome); each inbound stream is
 * one HTTP request, served in the background so streams never wait on
 * each other.
 */
import { MixerClient, SDK_VERSION, type DisconnectPayload, type ReconnectOptions, type TokenProvider } from "../client.js";
import { Emitter } from "../emitter.js";
import { ErrorCode, WsMixerError } from "../errors.js";
import { DEFAULT_MAX_BODY_BYTES } from "../http1/request.js";
import { detectRuntime } from "../runtime.js";
import type { WebSocketFactory } from "../socket.js";
import { DEFAULT_URL, INTEGRATION_ID_RE, SERVICE_NAME_MAX, SERVICE_NAME_RE, type ServiceKind } from "./envelope.js";
import { McpwarpError } from "./errors.js";
import { DEFAULT_TARGET_TIMEOUT_MS, MCP_PATH, serveWithHandler, serveWithTarget, type FetchHandler, type ServeContext, type StreamServer } from "./serve.js";
import { Session, type ExposeConflict, type ExposeRegistration, type ExposeState } from "./session.js";

export interface ExposeBaseOptions {
  /** Default "wss://connect.mcpwarp.io". Routed by Host; the path is ignored. */
  url?: string;
  /** PAT (`mcpwarp_pat_...`) or a connect-audience JWT; static or a provider (called on every dial). */
  token: TokenProvider;
  /** Public URL slug: ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$, <= 30 chars. Validated synchronously (TypeError). */
  name: string;
  /** Default "http". */
  kind?: ServiceKind;
  /**
   * Your integration's registration UUID (usage attribution), in RFC 4122
   * text form (what zod's `z.uuid()` accepts: version 1-8, variant 8/9/a/b,
   * or the nil/max uuid; any case). Sent lowercased. Anything else is a
   * TypeError, since the tunnel would drop it silently.
   */
  integration: string;
  webSocket?: WebSocketFactory;
  /**
   * Overrides what the SDK auto-detects for `hello.agent`, the `User-Agent`
   * header and `meta.os`; useful in sandboxes where neither `process` nor
   * `navigator` exists (detected as "unknown"), e.g. Zotero:
   * `agent: { runtime: "zotero", os: Services.appinfo.OS }`. Each field, when
   * given, must be 1-64 printable ASCII characters (TypeError otherwise).
   */
  agent?: { runtime?: string; os?: string };
  /** `maxAttempts` (default Infinity) also bounds the attempts before the first registration. */
  reconnect?: ReconnectOptions;
  /** Request body cap, default 4 MiB (413 above). */
  maxRequestBodyBytes?: number;
  /**
   * Aborting it before the first registration rejects `expose()` with an
   * AbortError and closes the client; aborting it later closes the handle
   * (same as `handle.close()`).
   */
  signal?: AbortSignal;
  /**
   * Called before every scheduled retry: a reconnect after a failed dial or
   * a lost connection, or a re-sent register after a transient register
   * error. `expose()` keeps retrying until it registers (or `signal`
   * aborts, or `reconnect.maxAttempts` runs out), so this is where to log.
   */
  onRetry?(info: { attempt: number; delayMs: number; reason: string }): void;
}

export type ExposeOptions = ExposeBaseOptions &
  (
    | { handler: FetchHandler; target?: never; targetTimeoutMs?: never }
    | {
        target: string | URL;
        /** How long to wait for the target's response headers before answering `504`. Default 30000. */
        targetTimeoutMs?: number;
        handler?: never;
      }
  );

export interface ExposeHandle {
  /**
   * The public URL from the latest successful register. Undefined only when
   * `expose()` resolved because the service was refused as disabled (state
   * "disabled") and has not registered since (a take-over before the first
   * registration rejects `expose()` instead: "TAKEN_OVER").
   */
  readonly url: string | undefined;
  /** Undefined in the same case, when the refusal carried no id. */
  readonly serverId: string | undefined;
  /**
   * The payload of the latest `registered` event, readable right after
   * `await expose()` (the first `registered` fires before the caller holds
   * the handle). `created` is true only when that register created the
   * service. Undefined when `expose()` resolved as "disabled" and the
   * service has not registered since.
   */
  readonly registration: ExposeRegistration | undefined;
  /**
   * Always "/mcp": the only request path forwarded (exact, case-sensitive,
   * query ignored). Any other path is answered 404 `{"error":"not_found"}`
   * by the SDK before the app, even while disabled or closing.
   */
  readonly path: "/mcp";
  /** After "conflict", `url`/`serverId`/`registration` keep the last registration's values, which belong to the new holder now. */
  readonly state: ExposeState;
  /** Escape hatch (stats, events). */
  readonly client: MixerClient;
  on(event: "registered", listener: (e: ExposeRegistration) => void): this;
  on(event: "disabled", listener: (e: { serverId: string | undefined; reason: string }) => void): this;
  on(event: "enabled", listener: (e: { serverId: string }) => void): this;
  /**
   * Another connection of the same account registered this name and the
   * tunnel moved the service there (it can arrive up to ~30 s later when the
   * other connection is on another tunnel pod). State becomes "conflict" for
   * good: the SDK stops serving (stray streams are RESET with CANCEL) and
   * never registers again, also across reconnects. Restarting the loser is
   * up to a human (or the app); `close()` still works.
   */
  on(event: "conflict", listener: (e: ExposeConflict) => void): this;
  /** Non-fatal; no listener is fine. */
  on(event: "error", listener: (err: McpwarpError | Error) => void): this;
  /** Every transport disconnect. */
  on(event: "disconnect", listener: (reason: DisconnectPayload) => void): this;
  /** Once, terminal. After a fatal failure `error` is a McpwarpError, the transport's error or disconnect payload as `cause`. */
  on(event: "close", listener: (e: { error?: Error }) => void): this;
  once(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener: (...args: any[]) => void): this;
  /** unregister (2 s budget), then client.close() (drain{client_requested}, 5 s grace, close 1000). Idempotent. */
  close(): Promise<void>;
}

class Handle extends Emitter implements ExposeHandle {
  readonly path = MCP_PATH;
  closePromise: Promise<void> | null = null;
  closeImpl!: () => Promise<void>;

  constructor(
    readonly client: MixerClient,
    private readonly session: Session,
  ) {
    super();
  }

  get url(): string | undefined {
    return this.session.url;
  }

  get serverId(): string | undefined {
    return this.session.serverId;
  }

  get registration(): ExposeRegistration | undefined {
    return this.session.registration;
  }

  get state(): ExposeState {
    return this.session.state;
  }

  close(): Promise<void> {
    if (!this.closePromise) this.closePromise = this.closeImpl();
    return this.closePromise;
  }

  /** Calls every listener even when an earlier one throws; returns what they threw. */
  emitAll(event: string, payload: unknown): unknown[] {
    const thrown: unknown[] = [];
    for (const entry of this.rawEntries(event)) {
      if (entry.once) this.dropEntry(event, entry);
      try {
        entry.fn.call(this, payload);
      } catch (e) {
        thrown.push(e);
      }
    }
    return thrown;
  }
}

/** Printable ASCII, capped like detectRuntime's own fields; it also goes into the User-Agent header. */
const AGENT_FIELD_RE = /^[\x20-\x7e]{1,64}$/;

function validate(opts: ExposeBaseOptions): void {
  if (typeof opts.name !== "string" || !SERVICE_NAME_RE.test(opts.name) || opts.name.length > SERVICE_NAME_MAX) {
    throw new TypeError(`mcpwarp: invalid service \`name\`: lowercase letters, digits and inner dashes, at most ${SERVICE_NAME_MAX} characters`);
  }
  if (typeof opts.integration !== "string" || opts.integration.length === 0) {
    throw new TypeError("mcpwarp: `integration` (your integration's registration UUID) is required");
  }
  if (!INTEGRATION_ID_RE.test(opts.integration.toLowerCase())) {
    throw new TypeError("mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else");
  }
  if (opts.kind !== undefined && opts.kind !== "http" && opts.kind !== "stdio") {
    throw new TypeError('mcpwarp: `kind` must be "http" or "stdio"');
  }
  if (opts.token === undefined || opts.token === null || opts.token === "") {
    throw new TypeError("mcpwarp: `token` is required");
  }
  if (opts.agent !== undefined && (typeof opts.agent !== "object" || opts.agent === null)) {
    throw new TypeError("mcpwarp: `agent` must be an object");
  }
  for (const k of ["runtime", "os"] as const) {
    const v: unknown = opts.agent?.[k];
    if (v !== undefined && (typeof v !== "string" || !AGENT_FIELD_RE.test(v))) {
      throw new TypeError(`mcpwarp: \`agent.${k}\` must be 1-64 printable ASCII characters`);
    }
  }
}

const MAX_TIMER_MS = 2_147_483_647;

function abortError(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  if (reason instanceof Error && reason.name === "AbortError") return reason;
  const message = "mcpwarp: expose() aborted before the service was registered";
  const DOMEx = (globalThis as Record<string, any>).DOMException as (new (message: string, name: string) => Error) | undefined;
  if (typeof DOMEx === "function") return new DOMEx(message, "AbortError");
  return Object.assign(new Error(message), { name: "AbortError" });
}

/** A fatal transport error as the McpwarpError `expose()` rejects / the handle closes with; the original is `cause`. */
function fatalMcpwarpError(err: Error, serviceName: string): McpwarpError {
  if (err instanceof McpwarpError) return err;
  const code = err instanceof WsMixerError && err.code === ErrorCode.UNAUTHORIZED ? "UNAUTHORIZED" : "DISCONNECTED_FATAL";
  return new McpwarpError(code, err.message, { fatal: true, serviceName, cause: err });
}

/**
 * Resolves once the service is registered (or disabled from the dashboard:
 * state "disabled", url undefined). Rejects with a fatal McpwarpError if
 * the first connection or first registration fails fatally (a rejected
 * token: code "UNAUTHORIZED", the transport's WsMixerError as `cause`; a
 * `conflict` before the first `registered`: code "TAKEN_OVER"), or
 * with an AbortError when `signal` aborts first. Non-fatal failures (unreachable
 * URL, an upgrade the tunnel refuses, transient register errors) are
 * retried, reported through `onRetry`.
 */
export function expose(opts: ExposeOptions): Promise<ExposeHandle> {
  const hasHandler = typeof (opts as { handler?: unknown }).handler === "function";
  const hasTarget = (opts as { target?: unknown }).target !== undefined;
  if (hasHandler === hasTarget) throw new TypeError("mcpwarp: pass exactly one of `handler` or `target`");
  if (hasHandler && (opts as { targetTimeoutMs?: unknown }).targetTimeoutMs !== undefined) {
    throw new TypeError("mcpwarp: `targetTimeoutMs` applies only to `target`");
  }
  if (hasTarget) {
    const target = (opts as { target: string | URL }).target;
    try {
      new URL(String(target));
    } catch {
      throw new TypeError("mcpwarp: `target` is not a valid URL");
    }
    const timeoutMs = (opts as { targetTimeoutMs?: unknown }).targetTimeoutMs ?? DEFAULT_TARGET_TIMEOUT_MS;
    // setTimeout fires at once for delays above 2^31-1 ms.
    if (typeof timeoutMs !== "number" || !(timeoutMs > 0) || timeoutMs > MAX_TIMER_MS) {
      throw new TypeError(`mcpwarp: targetTimeoutMs must be a positive number of at most ${MAX_TIMER_MS} ms, got ${typeof timeoutMs === "number" ? String(timeoutMs) : typeof timeoutMs}`);
    }
    return exposeWithServer(opts, (ctx) => serveWithTarget(target, ctx, timeoutMs));
  }
  const handler = (opts as { handler: FetchHandler }).handler;
  return exposeWithServer(opts, (ctx) => serveWithHandler(handler, ctx));
}

/** @internal Shared by the `handler`/`target` adapters and `./node`'s `server`. */
export function exposeWithServer(
  opts: ExposeBaseOptions,
  makeServer: (ctx: ServeContext) => StreamServer,
  hooks?: { onClose?: () => void },
): Promise<ExposeHandle> {
  validate(opts);
  const signal = opts.signal;
  if (signal?.aborted) return Promise.reject(abortError(signal));
  const integration = opts.integration.toLowerCase();
  // Assigned right below; the session's callbacks only run once the client exists.
  let client!: MixerClient;
  let handle!: Handle;
  let settled = false;
  let finished = false;
  let resolveStart!: (h: ExposeHandle) => void;
  let rejectStart!: (e: Error) => void;
  const started = new Promise<ExposeHandle>((resolve, reject) => {
    resolveStart = resolve;
    rejectStart = reject;
  });

  // A throwing "error" listener has nowhere left to report to.
  const reportError = (err: unknown): void => {
    handle.emitAll("error", err);
  };
  // Listener failures must never unwind into the transport (its delivery loop, timers, socket callbacks).
  const emitSafely = (event: string, payload: unknown): void => {
    for (const e of handle.emitAll(event, payload)) reportError(e);
  };
  const onRetry = (info: { attempt: number; delayMs: number; reason: string }): void => {
    try {
      opts.onRetry?.(info);
    } catch (e) {
      reportError(e);
    }
  };

  const onAbort = (): void => {
    if (!settled) {
      const err = abortError(signal!);
      void client.close().catch(() => {});
      finish(err);
    } else {
      void handle.close();
    }
  };

  const finish = (error?: Error): void => {
    if (finished) return;
    finished = true;
    signal?.removeEventListener("abort", onAbort);
    session.markClosed();
    hooks?.onClose?.();
    emitSafely("close", error ? { error } : {});
    if (!settled) {
      settled = true;
      rejectStart(error ?? new McpwarpError("CLOSED", "mcpwarp: closed before the service was registered"));
    }
  };

  const session = new Session({
    name: opts.name,
    kind: opts.kind ?? "http",
    sendApp: (body) => client.sendApp(body),
    setTimeout: (fn, ms) => setTimeout(fn, ms),
    clearTimeout: (h) => clearTimeout(h as Parameters<typeof clearTimeout>[0]),
    emit: (event, payload) => {
      if (event === "state") return;
      if (event === "retry") {
        onRetry(payload as { attempt: number; delayMs: number; reason: string });
        return;
      }
      // Taken over before `registered` arrived: the handle could never serve, so fail like any other first-registration fatal.
      if (event === "conflict" && !settled) {
        void client.close().catch(() => {});
        finish(new McpwarpError("TAKEN_OVER", `mcpwarp: service ${JSON.stringify(opts.name)} was taken over by another connection before it registered`, { fatal: true, serviceName: opts.name }));
        return;
      }
      emitSafely(event, payload);
      if (!settled && (event === "registered" || event === "disabled")) {
        settled = true;
        resolveStart(handle);
      }
    },
    fatal: (err) => {
      void client.close().catch(() => {});
      finish(err);
    },
  });

  const ctx: ServeContext = {
    isAvailable: () => session.state !== "disabled" && session.state !== "closing" && session.state !== "closed",
    unavailableReason: () => (session.state === "disabled" ? "service disabled" : "service closing"),
    maxBodyBytes: opts.maxRequestBodyBytes ?? DEFAULT_MAX_BODY_BYTES,
    onError: reportError,
  };
  const serve = makeServer(ctx);

  client = new MixerClient(opts.url ?? DEFAULT_URL, {
    token: opts.token,
    meta: { client: "sdk-js", version: SDK_VERSION, os: opts.agent?.os ?? detectRuntime().os, integration },
    agent: opts.agent && { runtime: opts.agent.runtime, os: opts.agent.os },
    headers: { "X-Mcpwarp-Integration": integration },
    webSocket: opts.webSocket,
    reconnect: opts.reconnect,
    onConnect: () => session.onWelcome(),
    onApp: (body) => session.onApp(body),
    // Never awaited: the delivery loop must not wait for one request before the next.
    onStream: (stream) => {
      // The tunnel routes nothing here after a conflict; whatever still arrives is not ours to answer.
      if (session.state === "conflict") {
        stream.reset("CANCEL", "service taken over by another connection");
        return;
      }
      void serve(stream);
    },
    onDisconnect: (reason) => {
      // A drain-superseded connection (or the drain's parallel dial) can end
      // while the replacement is the active, registered connection.
      if (!client.currentConn()) session.onDisconnect();
      emitSafely("disconnect", reason);
      // Exhausted reconnects emit no client "fatal". Deferred so a fatal close
      // code's own "fatal" event (emitted right after this report) wins.
      if (reason.fatal && !handle.closePromise) {
        const err = new McpwarpError("DISCONNECTED_FATAL", reason.message, { fatal: true, serviceName: opts.name, cause: reason });
        void Promise.resolve().then(() => finish(err));
      }
    },
  });
  handle = new Handle(client, session);
  handle.closeImpl = async () => {
    await session.beginClose();
    await client.close().catch(() => {});
    finish();
  };
  client.on("reconnecting", (info: { attempt: number; delayMs: number; cause: string }) => {
    if (info.cause !== "drain") onRetry({ attempt: info.attempt, delayMs: info.delayMs, reason: info.cause });
  });
  client.on("fatal", (err: Error) => finish(fatalMcpwarpError(err, opts.name)));
  signal?.addEventListener("abort", onAbort, { once: true });
  client.start().catch((err: Error) => finish(fatalMcpwarpError(err, opts.name)));
  return started;
}
