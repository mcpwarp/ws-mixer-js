/**
 * createTunnel(): a long-lived controller over expose() for integrations
 * whose settings change at runtime (a plugin's preferences pane). Call
 * configure() on every settings change; it restarts the exposure only when
 * something relevant changed. Every start gets a new generation; callbacks
 * of an older one are ignored. Needs only timers and promises from the
 * runtime, so it runs in plugin sandboxes such as Zotero's.
 */
import { AppErrorCode, isValidServiceName, RegisterErrorCode, SERVICE_NAME_MAX, TRANSIENT_REGISTER_ERRORS } from "./envelope.js";
import { McpwarpError } from "./errors.js";
import { adapterServer, exposeWithServer, validateAgent, validateIntegration, type AbortSignalLike, type ExposeHandle, type ExposeOptions } from "./expose.js";
import type { FetchHandler, ServeContext, StreamServer } from "./serve.js";
import { REGISTER_RETRY_PREFIX } from "./session.js";

interface TunnelCommon {
  /** Your integration's registration UUID, as for expose(). */
  integration: string;
  agent?: ExposeOptions["agent"];
  webSocket?: ExposeOptions["webSocket"];
  maxRequestBodyBytes?: number;
  reconnect?: ExposeOptions["reconnect"];
  /** Called for retries of the current connection only. */
  onRetry?: ExposeOptions["onRetry"];
  /**
   * Default: silent. Never receives the token. Messages carry no "mcpwarp: "
   * prefix; tag them yourself. The technical details behind
   * `status.lastError` land here. At "error": fatal failures as
   * "CODE: message" with the McpwarpError code (a failure without one, such
   * as an invalid `name` caught before dialing, has the message only), and a
   * refused config as "INVALID_CONFIG: <which field is wrong>", never its
   * value. At "warn": retries as "retry #N in MS ms: reason", disconnects as
   * "disconnected: reason", and non-fatal errors as "CODE: message" (a
   * request handler's error has no code). Progress goes to "info".
   */
  logger?: (level: "info" | "warn" | "error", message: string) => void;
}

/** Exactly one of `handler` | `target`, as for expose(). */
export type TunnelOptions = TunnelCommon &
  (
    | { handler: FetchHandler; target?: never; targetTimeoutMs?: never }
    | { target: string | URL; targetTimeoutMs?: number; handler?: never }
  );

/** @deprecated Renamed to TunnelOptions; this alias is removed in 0.11.0. */
export type TunnelBase = TunnelOptions;

/**
 * What may change at runtime. Disabled, or an empty (after trim) token, means "off" without dialing,
 * whatever the other fields hold. A non-boolean `enabled`, or (while enabled with a token) another
 * field of the wrong type (an unset pref read as `undefined`, say), is state "error", code INVALID_CONFIG.
 */
export interface TunnelConfig {
  enabled: boolean;
  /** Sent trimmed. */
  token: string;
  name: string;
  url?: string;
  kind?: ExposeOptions["kind"];
  /**
   * Overrides the options' `webSocket` for this connection. Compared by reference, after falling
   * back to the options' one: a different effective factory restarts.
   */
  webSocket?: ExposeOptions["webSocket"];
}

export type TunnelState = "off" | "connecting" | "online" | "disabled" | "conflict" | "error";

/** `TunnelStatus.code` values; named after the McpwarpError code where one exists. */
export const TunnelStatusCode = {
  /**
   * "error": the token was rejected (a register refused as UNAUTHENTICATED, or an HTTP 401 at the
   * upgrade), or it is not allowed to publish this service (a register refused as FORBIDDEN).
   */
  UNAUTHORIZED: "UNAUTHORIZED",
  /** "conflict": another connection took the name over. */
  TAKEN_OVER: "TAKEN_OVER",
  /** "connecting": the connection was lost and is being re-established. */
  DISCONNECTED: "DISCONNECTED",
  /**
   * "connecting": a dial or a register failed and is retried, the very first attempt included.
   * Redials after a lost connection stay DISCONNECTED; a register retried then is RETRYING again.
   */
  RETRYING: "RETRYING",
  /** "disabled": the service is disabled in the mcpwarp dashboard. */
  DISABLED: "DISABLED",
  /** "error": the config was refused (a field of the wrong type, an invalid name or kind, or INVALID_NAME from the server). */
  INVALID_CONFIG: "INVALID_CONFIG",
  /** "error": the mcpwarp account has no username yet. */
  USERNAME_REQUIRED: "USERNAME_REQUIRED",
  /** "error": the account's service quota is used up. */
  QUOTA_EXCEEDED: "QUOTA_EXCEEDED",
  /** "error": the server refused the name as in use by another server (register CONFLICT). */
  CONFLICT: "CONFLICT",
  /** "error": mcpwarp no longer supports this SDK version. */
  UNSUPPORTED_VERSION: "UNSUPPORTED_VERSION",
  /**
   * "error": the connection failed for good: a fatal close or transport failure, or
   * `reconnect.maxAttempts` ran out (attempts before the first registration count too).
   */
  DISCONNECTED_FATAL: "DISCONNECTED_FATAL",
  /** A failure none of the above covers; the logger has the details. */
  INTERNAL: "INTERNAL",
} as const;
export type TunnelStatusCode = (typeof TunnelStatusCode)[keyof typeof TunnelStatusCode];

export interface TunnelStatus {
  state: TunnelState;
  /** Set while "online". */
  url?: string;
  /** Why the tunnel is not online (or why it is retrying); set together with `lastError`. */
  code?: TunnelStatusCode;
  /**
   * `code` in plain language, fit to show to end users ("The token was
   * rejected."): always a full sentence ending with a period, so it can be
   * appended to the host's own text. Cleared when the tunnel goes online;
   * non-fatal errors while online (a failing request handler, say) are only
   * logged. The technical message goes to the logger.
   */
  lastError?: string;
}

export interface Tunnel {
  /**
   * Applies `config`. A no-op when it equals the last one passed (unless a
   * conflict, error or stop() came in between); otherwise stops the current
   * connection and, when enabled with a token, starts a new one. Resolves
   * once the old connection is stopped and the new one is dialing (not when
   * it is online). Calls are applied in order; the last one wins.
   *
   * A restart first waits for the old connection to unregister (up to 2 s)
   * and drain (up to 5 s; open requests or SSE streams, which MCP clients
   * usually keep, run it to the end), so it can take ~7 s, with the status
   * "connecting" meanwhile. Debounce settings typed into text fields.
   */
  configure(config: TunnelConfig): Promise<void>;
  /** Stops (unregister + drain, see configure()) and goes "off"; configure() starts again, even with the same config. */
  stop(): Promise<void>;
  /**
   * Final: stops with the unregister + drain bounded by `timeoutMs` (default
   * 5000), then configure()/stop() do nothing. The bound can cut an
   * in-flight drain short (its worst case is ~7 s): dispose() then resolves
   * while the old connection is still closing in the background.
   */
  dispose(opts?: { timeoutMs?: number }): Promise<void>;
  /** @deprecated Renamed to dispose(); this alias is removed in 0.11.0. */
  close(opts?: { timeoutMs?: number }): Promise<void>;
  /** A snapshot copy. */
  readonly status: TunnelStatus;
  /** Calls `listener` now and on every status change; returns the unsubscribe function. */
  subscribe(listener: (s: TunnelStatus) => void): () => void;
}

interface Active {
  gen: number;
  abort(): void;
  started: Promise<ExposeHandle>;
  /** Client closes expose() ran itself before (or instead of) resolving a handle. */
  clientCloses: Promise<void>[];
}

const DEFAULT_CLOSE_TIMEOUT_MS = 5000;

/** Satisfies what expose() reads from `signal`; sandboxes may have no abort controller global. */
function abortable(): { signal: AbortSignalLike; abort(): void } {
  const listeners = new Set<() => void>();
  const signal = {
    aborted: false,
    reason: undefined,
    addEventListener(type: "abort", listener: () => void): void {
      if (type === "abort" && !signal.aborted) listeners.add(listener);
    },
    removeEventListener(type: "abort", listener: () => void): void {
      if (type === "abort") listeners.delete(listener);
    },
  };
  return {
    signal,
    abort() {
      if (signal.aborted) return;
      signal.aborted = true;
      const fns = [...listeners];
      listeners.clear();
      for (const fn of fns) fn();
    },
  };
}

/** A disabled config's token is unchecked and may be no string. */
function trimmed(token: unknown): unknown {
  return typeof token === "string" ? token.trim() : token;
}

function sameConfig(a: TunnelConfig | undefined, b: TunnelConfig, baseWebSocket: TunnelOptions["webSocket"]): boolean {
  return (
    a !== undefined &&
    a.enabled === b.enabled &&
    trimmed(a.token) === trimmed(b.token) &&
    a.name === b.name &&
    a.url === b.url &&
    a.kind === b.kind &&
    (a.webSocket ?? baseWebSocket) === (b.webSocket ?? baseWebSocket)
  );
}

function sameStatus(a: TunnelStatus, b: TunnelStatus): boolean {
  return a.state === b.state && a.url === b.url && a.code === b.code && a.lastError === b.lastError;
}

/** For the logger, which tags messages itself. */
function errorMessage(err: unknown): string {
  return (err instanceof Error ? err.message : String(err)).replace(/^mcpwarp: /, "");
}

/** errorMessage() with the McpwarpError code, for the logger. */
function technical(err: unknown): string {
  return err instanceof McpwarpError ? `${err.code}: ${errorMessage(err)}` : errorMessage(err);
}

interface Problem {
  code: TunnelStatusCode;
  lastError: string;
}

const RETRY_DIAL: Problem = { code: TunnelStatusCode.RETRYING, lastError: "Can't reach mcpwarp; retrying." };
const RETRY_REGISTER: Problem = { code: TunnelStatusCode.RETRYING, lastError: "mcpwarp couldn't register the service; retrying." };
const LOST: Problem = { code: TunnelStatusCode.DISCONNECTED, lastError: "Connection lost; retrying." };
const TAKEN_OVER: Problem = { code: TunnelStatusCode.TAKEN_OVER, lastError: "Another connection took over this name." };
const DISABLED: Problem = { code: TunnelStatusCode.DISABLED, lastError: "This service is disabled in the mcpwarp dashboard." };
const REJECTED: Problem = { code: TunnelStatusCode.UNAUTHORIZED, lastError: "The token was rejected." };
const INVALID_NAME: Problem = {
  code: TunnelStatusCode.INVALID_CONFIG,
  lastError: `The service name is invalid: use lowercase letters, digits and dashes (not at the start or end), at most ${SERVICE_NAME_MAX} characters.`,
};
const INVALID_SETTINGS: Problem = { code: TunnelStatusCode.INVALID_CONFIG, lastError: "The tunnel settings are invalid." };

/**
 * Register refusals. After an earlier registration session.ts reports them as non-fatal `error`s
 * to retry at the next welcome, but the connection stays up, so nothing retries them.
 */
const REFUSALS: ReadonlySet<string> = new Set([
  RegisterErrorCode.UNAUTHENTICATED,
  RegisterErrorCode.FORBIDDEN,
  RegisterErrorCode.INVALID_NAME,
  RegisterErrorCode.QUOTA_EXCEEDED,
  RegisterErrorCode.USERNAME_REQUIRED,
  RegisterErrorCode.CONFLICT,
]);

/** A config field of the wrong type, checked before any other use; JS hosts reading prefs get no help from the types. */
function invalidConfig(config: unknown): { detail: string; problem: Problem } | undefined {
  if (typeof config !== "object" || config === null) return { detail: "the config must be an object", problem: INVALID_SETTINGS };
  const c = config as Record<string, unknown>;
  if (typeof c.enabled !== "boolean") return { detail: "`enabled` must be a boolean", problem: INVALID_SETTINGS };
  // Disabled, or an empty token, is "off" whatever the rest says.
  if (!c.enabled || (typeof c.token === "string" && c.token.trim() === "")) return undefined;
  if (typeof c.token !== "string") return { detail: "`token` must be a string", problem: { code: TunnelStatusCode.INVALID_CONFIG, lastError: "The token is not set." } };
  if (typeof c.name !== "string") return { detail: "`name` must be a string", problem: { code: TunnelStatusCode.INVALID_CONFIG, lastError: "The service name is not set." } };
  if (c.url !== undefined && typeof c.url !== "string") return { detail: "`url` must be a string", problem: INVALID_SETTINGS };
  if (c.webSocket !== undefined && typeof c.webSocket !== "function") return { detail: "`webSocket` must be a function", problem: INVALID_SETTINGS };
  return undefined;
}

/** What a failure means for the user; the technical message goes to the logger. */
function describeProblem(err: unknown): Problem {
  if (err instanceof McpwarpError) {
    switch (err.code) {
      case "UNAUTHORIZED":
      case RegisterErrorCode.UNAUTHENTICATED:
        return REJECTED;
      case RegisterErrorCode.FORBIDDEN:
        return { code: TunnelStatusCode.UNAUTHORIZED, lastError: "The token is not allowed to publish this service." };
      case "TAKEN_OVER":
        return TAKEN_OVER;
      case RegisterErrorCode.INVALID_NAME:
        return INVALID_NAME;
      case RegisterErrorCode.USERNAME_REQUIRED:
        return { code: TunnelStatusCode.USERNAME_REQUIRED, lastError: "Choose a username in your mcpwarp account first." };
      case RegisterErrorCode.QUOTA_EXCEEDED:
        return { code: TunnelStatusCode.QUOTA_EXCEEDED, lastError: "Your mcpwarp account has no services left in its quota." };
      case RegisterErrorCode.CONFLICT:
        return { code: TunnelStatusCode.CONFLICT, lastError: "The name is in use by another server." };
      case AppErrorCode.UNSUPPORTED_VERSION:
        return { code: TunnelStatusCode.UNSUPPORTED_VERSION, lastError: "This SDK version is no longer supported; please update." };
      case "DISCONNECTED_FATAL":
        return { code: TunnelStatusCode.DISCONNECTED_FATAL, lastError: "The connection to mcpwarp failed." };
    }
    if (TRANSIENT_REGISTER_ERRORS.has(err.code)) return RETRY_REGISTER;
  }
  return { code: TunnelStatusCode.INTERNAL, lastError: "Something went wrong; the log has the details." };
}

/** A TypeError expose() threw synchronously from its validation. */
function validationProblem(cfg: TunnelConfig): Problem {
  return isValidServiceName(cfg.name) ? INVALID_SETTINGS : INVALID_NAME;
}

function willStart(config: TunnelConfig): boolean {
  return config.enabled && config.token.trim() !== "";
}

class TunnelImpl implements Tunnel {
  private generation = 0;
  private current: TunnelStatus = { state: "off" };
  private readonly listeners = new Set<(s: TunnelStatus) => void>();
  /** The latest config passed to configure(); cleared by stop(), a conflict or an error so the same config restarts. */
  private desired: TunnelConfig | undefined;
  private active: Active | undefined;
  private draining: Promise<void> = Promise.resolve();
  private queue: Promise<void> = Promise.resolve();
  /** The apply of `desired`, returned to a repeat of it. */
  private applying: Promise<void> = Promise.resolve();
  private closed = false;
  private closing: Promise<void> | undefined;

  constructor(
    private readonly base: TunnelOptions,
    private readonly makeServer: (ctx: ServeContext) => StreamServer,
  ) {}

  get status(): TunnelStatus {
    return { ...this.current };
  }

  subscribe(listener: (s: TunnelStatus) => void): () => void {
    this.notify(listener);
    if (this.closed) return () => {};
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  configure(config: TunnelConfig): Promise<void> {
    if (this.closed) return Promise.resolve();
    const invalid = invalidConfig(config);
    if (invalid) {
      this.log("error", `${invalid.problem.code}: ${invalid.detail}`);
      this.desired = undefined;
      this.detach();
      this.setStatus({ state: "error", ...invalid.problem });
      return this.enqueue(() => this.draining);
    }
    if (sameConfig(this.desired, config, this.base.webSocket)) return this.applying;
    const cfg: TunnelConfig = { enabled: config.enabled, token: config.token, name: config.name, url: config.url, kind: config.kind, webSocket: config.webSocket };
    this.desired = cfg;
    this.applying = this.enqueue(() => this.apply(cfg));
    return this.applying;
  }

  stop(): Promise<void> {
    if (this.closed) return Promise.resolve();
    this.desired = undefined;
    this.detach();
    this.setStatus({ state: "off" });
    return this.enqueue(() => this.draining);
  }

  dispose(opts?: { timeoutMs?: number }): Promise<void> {
    if (!this.closing) this.closing = this.disposeImpl(opts?.timeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS);
    return this.closing;
  }

  close(opts?: { timeoutMs?: number }): Promise<void> {
    return this.dispose(opts);
  }

  private async disposeImpl(timeoutMs: number): Promise<void> {
    this.closed = true;
    this.desired = undefined;
    this.detach();
    this.setStatus({ state: "off" });
    this.listeners.clear();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        this.log("warn", `dispose() gave up waiting after ${timeoutMs} ms`);
        resolve();
      }, timeoutMs);
    });
    try {
      await Promise.race([this.queue.then(() => this.draining), timedOut]);
    } finally {
      clearTimeout(timer);
    }
  }

  /** Runs `task` after every earlier one; the chain itself never rejects. */
  private enqueue(task: () => Promise<void>): Promise<void> {
    const run = this.queue.then(task);
    this.queue = run.catch((e: unknown) => this.log("error", errorMessage(e)));
    return this.queue;
  }

  private async apply(cfg: TunnelConfig): Promise<void> {
    if (this.closed || cfg !== this.desired) return;
    this.detach();
    this.setStatus({ state: willStart(cfg) ? "connecting" : "off" });
    await this.draining;
    if (this.closed || cfg !== this.desired) return;
    if (willStart(cfg)) this.start(cfg);
    else this.log("info", "off");
  }

  private start(cfg: TunnelConfig): void {
    const gen = ++this.generation;
    const live = (): boolean => gen === this.generation;
    const { signal, abort } = abortable();
    this.setStatus({ state: "connecting" });
    this.log("info", `connecting as ${JSON.stringify(cfg.name)}${cfg.url ? ` via ${cfg.url}` : ""}`);
    // A listener or the logger may have called stop()/dispose() just now.
    if (!live()) return;
    const clientCloses: Promise<void>[] = [];
    let started: Promise<ExposeHandle>;
    try {
      started = exposeWithServer(
        {
          url: cfg.url,
          token: cfg.token.trim(),
          name: cfg.name,
          kind: cfg.kind,
          integration: this.base.integration,
          agent: this.base.agent,
          webSocket: cfg.webSocket ?? this.base.webSocket,
          reconnect: this.base.reconnect,
          maxRequestBodyBytes: this.base.maxRequestBodyBytes,
          signal,
          onRetry: (info) => {
            if (!live()) return;
            this.log("warn", `retry #${info.attempt} in ${info.delayMs} ms: ${info.reason}`);
            // While online this is a drain replacement's retry; the old connection still serves.
            if (this.current.state === "connecting") {
              // Session's reason for a transient register error; anything else is a redial.
              if (info.reason.startsWith(REGISTER_RETRY_PREFIX)) this.setStatus({ ...this.current, ...RETRY_REGISTER });
              // After a lost connection the redials keep saying so.
              else if (this.current.code !== TunnelStatusCode.DISCONNECTED) this.setStatus({ ...this.current, ...RETRY_DIAL });
            }
            this.base.onRetry?.(info);
          },
        },
        this.makeServer,
        { onClientClose: (closed) => clientCloses.push(closed) },
      );
    } catch (e) {
      this.fail(cfg, e, true);
      return;
    }
    this.active = { gen, abort, started, clientCloses };
    started.then(
      (handle) => {
        if (!live()) return;
        this.wire(handle, cfg, live);
        switch (handle.state) {
          case "registered":
            this.log("info", `online at ${handle.url}`);
            this.setStatus({ state: "online", url: handle.url });
            return;
          case "disabled":
            this.log("warn", "the service is disabled in the mcpwarp dashboard");
            this.setStatus({ state: "disabled", ...DISABLED });
            return;
          // The handle moved on between its first `registered`/`disabled` and the resolution reaching us.
          case "conflict":
            this.conflicted(cfg);
            return;
          case "closing":
          case "closed":
            this.fail(cfg, new Error("the connection closed"));
            return;
          default:
            this.setStatus({ state: "connecting" });
        }
      },
      (err: unknown) => {
        if (!live()) return;
        this.fail(cfg, err);
      },
    );
  }

  private wire(handle: ExposeHandle, cfg: TunnelConfig, live: () => boolean): void {
    handle.on("registered", (e) => {
      if (!live()) return;
      this.log("info", `online at ${e.url}`);
      this.setStatus({ state: "online", url: e.url });
    });
    handle.on("disabled", (e) => {
      if (!live()) return;
      this.log("warn", `the service was disabled (${e.reason})`);
      this.setStatus({ state: "disabled", ...DISABLED });
    });
    handle.on("enabled", () => {
      if (!live()) return;
      this.log("info", "the service was enabled, registering again");
      this.setStatus({ state: "connecting" });
    });
    handle.on("error", (err) => {
      if (!live()) return;
      if (err instanceof McpwarpError && REFUSALS.has(err.code)) {
        this.fail(cfg, err);
        return;
      }
      this.log("warn", technical(err));
      // Request handler failures come here too; only mcpwarp's own errors explain a pending connection.
      if (this.current.state === "connecting" && err instanceof McpwarpError) this.setStatus({ ...this.current, ...describeProblem(err) });
    });
    handle.on("disconnect", (reason) => {
      if (!live() || reason.fatal) return;
      // expose() only moves the handle off registered/disabled when no connection is current.
      if (handle.state === "registered" || handle.state === "disabled") {
        this.log("info", `a replaced connection ended: ${reason.message}`);
        return;
      }
      this.log("warn", `disconnected: ${reason.message}`);
      this.setStatus({ state: "connecting", ...LOST });
    });
    handle.on("conflict", () => {
      if (!live()) return;
      this.conflicted(cfg);
    });
    handle.on("close", (e) => {
      if (!live()) return;
      this.fail(cfg, e.error ?? new Error("the connection closed"));
    });
  }

  private conflicted(cfg: TunnelConfig): void {
    this.log("warn", `another connection took over the name ${JSON.stringify(cfg.name)}; not reconnecting until configure() is called again`);
    this.forget(cfg);
    this.detach();
    this.setStatus({ state: "conflict", ...TAKEN_OVER });
  }

  /** A fatal failure of the current generation, or (`sync`) an error expose() threw synchronously. */
  private fail(cfg: TunnelConfig, err: unknown, sync = false): void {
    if (err instanceof McpwarpError && err.code === "TAKEN_OVER") {
      this.conflicted(cfg);
      return;
    }
    const problem = sync && err instanceof TypeError ? validationProblem(cfg) : describeProblem(err);
    this.log("error", technical(err));
    this.forget(cfg);
    this.detach();
    this.setStatus({ state: "error", ...problem });
  }

  private forget(cfg: TunnelConfig): void {
    if (this.desired === cfg) this.desired = undefined;
  }

  /** Invalidates the current generation synchronously and closes its connection in the background (tracked in `draining`). */
  private detach(): void {
    this.generation++;
    const a = this.active;
    this.active = undefined;
    if (!a) return;
    this.log("info", "stopping");
    a.abort();
    const closed = a.started.then(
      (handle) => handle.close(),
      () => Promise.all(a.clientCloses).then(() => {}),
    ).catch((e: unknown) => this.log("warn", `error while closing: ${errorMessage(e)}`));
    this.draining = Promise.all([this.draining, closed]).then(() => {});
  }

  private setStatus(next: TunnelStatus): void {
    const status: TunnelStatus = { state: next.state };
    if (next.url !== undefined) status.url = next.url;
    if (next.code !== undefined) status.code = next.code;
    if (next.lastError !== undefined) status.lastError = next.lastError;
    if (sameStatus(this.current, status)) return;
    this.current = status;
    for (const listener of [...this.listeners]) this.notify(listener);
  }

  private notify(listener: (s: TunnelStatus) => void): void {
    try {
      listener(this.status);
    } catch (e) {
      this.log("error", `status listener threw: ${errorMessage(e)}`);
    }
  }

  private log(level: "info" | "warn" | "error", message: string): void {
    try {
      this.base.logger?.(level, message);
    } catch {
      // a failing logger has nowhere to report to
    }
  }
}

/**
 * Creates a stopped ("off") tunnel. Throws a TypeError right away for an
 * invalid `integration`, `agent` or `handler`/`target` choice; problems with
 * a config (an invalid `name`, say) surface as state "error" instead.
 */
export function createTunnel(options: TunnelOptions): Tunnel {
  validateIntegration(options.integration);
  validateAgent(options.agent);
  return new TunnelImpl(options, adapterServer(options));
}
