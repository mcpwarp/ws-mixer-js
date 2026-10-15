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
import { DEFAULT_URL, INTEGRATION_ID_RE, isValidServiceName, SERVICE_NAME_MAX, type ServiceKind } from "./envelope.js";
import { McpwarpError } from "./errors.js";
import { DEFAULT_TARGET_TIMEOUT_MS, MCP_PATH, serveWithHandler, serveWithTarget, type FetchHandler, type ServeContext, type StreamServer } from "./serve.js";
import { Session, type ErrorOrigin, type ExposeConflict, type ExposeRegistration, type ExposeState, type RegisterRetryKind } from "./session.js";

type RetryInfo = { attempt: number; delayMs: number; reason: string };

/**
 * @internal What createTunnel() observes of one exposure, from the start (before `expose()` settles):
 * the handle's events, `onRetry` calls, and how each was classified.
 */
export type ExposeSignal =
  | { event: "registered"; registration: ExposeRegistration }
  | { event: "disabled"; reason: string }
  | { event: "enabled" }
  | { event: "conflict" }
  /** `origin` absent: a handler, listener or `onRetry` failure. */
  | { event: "error"; error: unknown; origin?: ErrorOrigin }
  /** `superseded`: the report is about a drained connection whose replacement welcomed, or a replacement dial failing while the drained one serves. */
  | { event: "disconnect"; reason: DisconnectPayload; superseded: boolean }
  /** "reconnect": a redial after a failure; "drain": a drain's replacement dial. */
  | { event: "retry"; info: RetryInfo; kind: RegisterRetryKind | "reconnect" | "drain" }
  | { event: "close"; error?: Error };

/** The part of an `AbortSignal` that `expose()` reads; any real `AbortSignal` fits. */
export interface AbortSignalLike {
  readonly aborted: boolean;
  readonly reason?: unknown;
  addEventListener(type: "abort", listener: () => void, options?: { once?: boolean }): void;
  removeEventListener(type: "abort", listener: () => void): void;
}

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
  signal?: AbortSignalLike;
  /**
   * Called before every scheduled retry: a reconnect after a failed dial or
   * a lost connection (zero delays included), a drain's replacement dial
   * (`{ attempt: 0, delayMs: <0-2 s jitter>, reason: "drain" }`), the one-shot
   * token-refresh redial (`{ attempt: 1, delayMs: 0, reason: "token-refresh" }`),
   * or a re-sent register after a transient register error or `OVERLOADED`
   * (reason "register failed: <CODE>: <message>"). `expose()` keeps retrying
   * until it registers (or `signal` aborts, or `reconnect.maxAttempts` runs
   * out), so this is where to log.
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
   * other connection is on another tunnel pod), or the tunnel drained this
   * connection with reason "replaced" (reason "replaced"). Either way the
   * connection is closed at once (error{NO_ERROR}, close 1000, in-flight
   * requests cut) and nothing reconnects or registers again. State becomes
   * "conflict" for good; the handle stays readable. Restarting the loser is
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
  /**
   * unregister (2 s budget), then client.close() (drain{client_requested}, 5 s grace, close 1000). Idempotent.
   * In state "conflict" there is nothing to unregister and the connection closes at once (error{NO_ERROR}, close 1000);
   * so does a `conflict` or drain{replaced} that arrives during the unregister wait.
   */
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

/** @internal Also run by createTunnel() up front. */
export function validateIntegration(integration: unknown): void {
  if (typeof integration !== "string" || integration.length === 0) {
    throw new TypeError("mcpwarp: `integration` (your integration's registration UUID) is required");
  }
  if (!INTEGRATION_ID_RE.test(integration.toLowerCase())) {
    throw new TypeError("mcpwarp: `integration` must be an RFC 4122 UUID in text form; the server silently ignores anything else");
  }
}

/** @internal Also run by createTunnel() up front. */
export function validateAgent(agent: unknown): void {
  if (agent !== undefined && (typeof agent !== "object" || agent === null)) {
    throw new TypeError("mcpwarp: `agent` must be an object");
  }
  for (const k of ["runtime", "os"] as const) {
    const v: unknown = (agent as Record<string, unknown> | undefined)?.[k];
    if (v !== undefined && (typeof v !== "string" || !AGENT_FIELD_RE.test(v))) {
      throw new TypeError(`mcpwarp: \`agent.${k}\` must be 1-64 printable ASCII characters`);
    }
  }
}

function validate(opts: ExposeBaseOptions): void {
  if (!isValidServiceName(opts.name)) {
    throw new TypeError(`mcpwarp: invalid service \`name\`: lowercase letters, digits and inner dashes, at most ${SERVICE_NAME_MAX} characters`);
  }
  validateIntegration(opts.integration);
  if (opts.kind !== undefined && opts.kind !== "http" && opts.kind !== "stdio") {
    throw new TypeError('mcpwarp: `kind` must be "http" or "stdio"');
  }
  if (opts.token === undefined || opts.token === null || opts.token === "") {
    throw new TypeError("mcpwarp: `token` is required");
  }
  validateAgent(opts.agent);
  validateReconnect(opts.reconnect);
}

/** Checked here so a bad value is a validation TypeError, not the client's RangeError at construction. */
function validateReconnect(reconnect: unknown): void {
  if (reconnect === undefined) return;
  if (typeof reconnect !== "object" || reconnect === null) throw new TypeError("mcpwarp: `reconnect` must be an object");
  const r = reconnect as Record<string, unknown>;
  const check = (key: string, ok: (n: number) => boolean, rule: string): void => {
    const v = r[key];
    if (v !== undefined && (typeof v !== "number" || !ok(v))) throw new TypeError(`mcpwarp: \`reconnect.${key}\` must be ${rule}`);
  };
  check("base", (n) => Number.isFinite(n) && n >= 0, "a finite number >= 0");
  check("cap", (n) => Number.isFinite(n) && n >= 0, "a finite number >= 0");
  check("connectTimeout", (n) => Number.isFinite(n) && n > 0, "a positive finite number");
  check("maxAttempts", (n) => n >= 0, "a number >= 0 (Infinity for no limit)");
  check("stableAfter", (n) => Number.isFinite(n) && n >= 0, "a finite number >= 0");
}

const MAX_TIMER_MS = 2_147_483_647;

/** @internal The `/mcp` 503 reasons; also read by the bridging fixture runner. */
export const UNAVAILABLE_REASONS = { disabled: "service disabled", closing: "service closing" } as const;

function abortError(signal: AbortSignalLike): Error {
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
  const code = err instanceof WsMixerError && (err.code === ErrorCode.UNAUTHORIZED || err.httpStatus === 401) ? "UNAUTHORIZED" : "DISCONNECTED_FATAL";
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
  return exposeWithServer(opts, adapterServer(opts));
}

/** @internal Validates the `handler`/`target` choice and returns its server factory; also used by createTunnel(). */
export function adapterServer(opts: { handler?: unknown; target?: unknown; targetTimeoutMs?: unknown }): (ctx: ServeContext) => StreamServer {
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
    return (ctx) => serveWithTarget(target, ctx, timeoutMs);
  }
  const handler = (opts as { handler: FetchHandler }).handler;
  return (ctx) => serveWithHandler(handler, ctx);
}

/** @internal Shared by the `handler`/`target` adapters and `./node`'s `server`. */
export function exposeWithServer(
  opts: ExposeBaseOptions,
  makeServer: (ctx: ServeContext) => StreamServer,
  hooks?: { onClose?: () => void; onClientClose?: (closed: Promise<void>) => void; observe?: (signal: ExposeSignal) => void },
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
  let pendingDrainRetry: RetryInfo | undefined;
  let resolveStart!: (h: ExposeHandle) => void;
  let rejectStart!: (e: Error) => void;
  const started = new Promise<ExposeHandle>((resolve, reject) => {
    resolveStart = resolve;
    rejectStart = reject;
  });

  const observe = (s: ExposeSignal): void => {
    try {
      hooks?.observe?.(s);
    } catch {
      // createTunnel() guards its own listeners; nothing here may unwind into the transport
    }
  };
  // A throwing "error" listener has nowhere left to report to.
  const reportError = (err: unknown, origin?: ErrorOrigin): void => {
    observe({ event: "error", error: err, origin });
    handle.emitAll("error", err);
  };
  // Listener failures must never unwind into the transport (its delivery loop, timers, socket callbacks).
  const emitSafely = (event: string, payload: unknown): void => {
    for (const e of handle.emitAll(event, payload)) reportError(e);
  };
  const onRetry = (info: RetryInfo, kind: RegisterRetryKind | "reconnect" | "drain"): void => {
    observe({ event: "retry", info, kind });
    try {
      opts.onRetry?.(info);
    } catch (e) {
      reportError(e);
    }
  };

  // Closes the client outside handle.close() (abort, take-over, fatal); onClientClose lets the caller await it.
  // A take-over skips the drain and the grace (MCPWARP.md section 6.8 Take-over close).
  const closeClient = (takeOver = false): void => {
    const closed = (takeOver ? client.closeNow() : client.close()).catch(() => {});
    hooks?.onClientClose?.(closed);
  };

  const onAbort = (): void => {
    if (!settled) {
      const err = abortError(signal!);
      closeClient();
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
    observe({ event: "close", error });
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
    emit: (event, payload, detail) => {
      if (event === "state") return;
      if (event === "retry") {
        onRetry(payload as RetryInfo, detail as RegisterRetryKind);
        return;
      }
      // Taken over before `registered` arrived: the handle could never serve, so fail like any other first-registration fatal.
      // Settled first: the take-over close ends the connection synchronously, and its disconnect must not read as a lost connection.
      if (event === "conflict" && !settled) {
        finish(new McpwarpError("TAKEN_OVER", `mcpwarp: service ${JSON.stringify(opts.name)} was taken over by another connection before it registered`, { fatal: true, serviceName: opts.name }));
        closeClient(true);
        return;
      }
      if (event === "error") {
        reportError(payload, detail as ErrorOrigin);
      } else {
        if (event === "registered") observe({ event, registration: payload as ExposeRegistration });
        else if (event === "disabled") observe({ event, reason: (payload as { reason: string }).reason });
        else if (event === "enabled" || event === "conflict") observe({ event });
        emitSafely(event, payload);
      }
      // A take-over after settling (a `conflict` op or a drain{replaced}): the handle stays, the connection goes now.
      if (event === "conflict" && !finished) closeClient(true);
      if (!settled && (event === "registered" || event === "disabled")) {
        settled = true;
        resolveStart(handle);
      }
    },
    fatal: (err) => {
      closeClient();
      finish(err);
    },
  });

  const ctx: ServeContext = {
    isAvailable: () => session.state !== "disabled" && session.state !== "closing" && session.state !== "closed",
    unavailableReason: () => (session.state === "disabled" ? UNAVAILABLE_REASONS.disabled : UNAVAILABLE_REASONS.closing),
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
    // An own `undefined` key would override the client's default in its spread merge.
    reconnect: opts.reconnect && (Object.fromEntries(Object.entries(opts.reconnect).filter(([, v]) => v !== undefined)) as ReconnectOptions),
    // A drain's replacement dial is announced before the drain event itself, which may turn out a take-over.
    // It does not consult the backoff counter (attempt 0); the token refresh is always attempt 1.
    onReconnect: (info) => {
      const attempt = info.cause === "drain" ? 0 : info.cause === "token-refresh" ? 1 : info.attempt;
      const retry = { attempt, delayMs: info.delayMs, reason: info.cause };
      if (info.cause === "drain") pendingDrainRetry = retry;
      else onRetry(retry, "reconnect");
    },
    onConnect: () => session.onWelcome(),
    // A drained connection's messages after its replacement's welcome belong to a superseded registration.
    onAppFrom: (body, superseded) => {
      if (!superseded) session.onApp(body);
    },
    // Never awaited: the delivery loop must not wait for one request before the next.
    onStream: (stream) => {
      // The tunnel routes nothing here after a conflict; whatever still arrives is not ours to answer.
      if (session.state === "conflict") {
        stream.reset("CANCEL", "service taken over by another connection");
        return;
      }
      void serve(stream);
    },
    onDisconnectFrom: (reason, superseded) => {
      // Decided by connection identity: a drained connection that ends once
      // its replacement has welcomed (or that replacement's failing dial
      // while the drained one still serves) leaves the session alone.
      if (!superseded) session.onDisconnect();
      observe({ event: "disconnect", reason, superseded });
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
    // Read after the wait: a `conflict` op or drain{replaced} during the unregister wait also sets it.
    await (session.takenOver ? client.closeNow() : client.close()).catch(() => {});
    finish();
  };
  client.on("drain", (d: { reason: string }) => {
    const retry = pendingDrainRetry;
    pendingDrainRetry = undefined;
    if (d.reason !== "replaced") {
      if (retry) onRetry(retry, "drain");
      return;
    }
    // A take-over, not a hand-over: the client is closed in this same tick, which cancels the replacement
    // dial (still in its jitter wait or token resolution) and fails a replacement from an earlier hand-over
    // that is still waiting for its welcome. A take-over closes it from the `conflict` emit; a close()
    // already running only gets there after its unregister wait, so it is closed here.
    const closingAlready = handle.closePromise !== null;
    session.onReplaced();
    if (finished) return;
    if (closingAlready) closeClient(true);
  });
  client.on("fatal", (err: Error) => finish(fatalMcpwarpError(err, opts.name)));
  signal?.addEventListener("abort", onAbort, { once: true });
  client.start().catch((err: Error) => finish(fatalMcpwarpError(err, opts.name)));
  return started;
}
