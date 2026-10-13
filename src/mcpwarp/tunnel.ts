/**
 * createTunnel(): a long-lived controller over expose() for integrations
 * whose settings change at runtime (a plugin's preferences pane). Call
 * configure() on every settings change; it restarts the exposure only when
 * something relevant changed. Every start gets a new generation; callbacks
 * of an older one are ignored. Needs only timers and promises from the
 * runtime, so it runs in plugin sandboxes such as Zotero's.
 */
import { McpwarpError } from "./errors.js";
import { adapterServer, exposeWithServer, validateAgent, validateIntegration, type AbortSignalLike, type ExposeHandle, type ExposeOptions } from "./expose.js";
import type { FetchHandler, ServeContext, StreamServer } from "./serve.js";

interface TunnelCommon {
  /** Your integration's registration UUID, as for expose(). */
  integration: string;
  agent?: ExposeOptions["agent"];
  webSocket?: ExposeOptions["webSocket"];
  maxRequestBodyBytes?: number;
  reconnect?: ExposeOptions["reconnect"];
  /** Called for retries of the current connection only. */
  onRetry?: ExposeOptions["onRetry"];
  /** Default: silent. Never receives the token. */
  logger?: (level: "info" | "warn" | "error", message: string) => void;
}

/** Exactly one of `handler` | `target`, as for expose(). */
export type TunnelBase = TunnelCommon &
  (
    | { handler: FetchHandler; target?: never; targetTimeoutMs?: never }
    | { target: string | URL; targetTimeoutMs?: number; handler?: never }
  );

/** What may change at runtime. Disabled, or an empty (after trim) token, means "off" without dialing. */
export interface TunnelConfig {
  enabled: boolean;
  /** Sent trimmed. */
  token: string;
  name: string;
  url?: string;
  kind?: ExposeOptions["kind"];
}

export type TunnelState = "off" | "connecting" | "online" | "disabled" | "conflict" | "error";

export interface TunnelStatus {
  state: TunnelState;
  /** Set while "online". */
  url?: string;
  /**
   * The latest retry reason, non-fatal error, disconnect reason or fatal
   * error message. Cleared when the tunnel goes online; non-fatal errors
   * while online (a failing request handler, say) are only logged.
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
   * in-flight drain short (its worst case is ~7 s): close() then resolves
   * while the old connection is still closing in the background.
   */
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

function sameConfig(a: TunnelConfig | undefined, b: TunnelConfig): boolean {
  return a !== undefined && a.enabled === b.enabled && a.token.trim() === b.token.trim() && a.name === b.name && a.url === b.url && a.kind === b.kind;
}

function sameStatus(a: TunnelStatus, b: TunnelStatus): boolean {
  return a.state === b.state && a.url === b.url && a.lastError === b.lastError;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
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
    private readonly base: TunnelBase,
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
    if (sameConfig(this.desired, config)) return this.applying;
    const cfg: TunnelConfig = { enabled: config.enabled, token: config.token, name: config.name, url: config.url, kind: config.kind };
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

  close(opts?: { timeoutMs?: number }): Promise<void> {
    if (!this.closing) this.closing = this.closeImpl(opts?.timeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS);
    return this.closing;
  }

  private async closeImpl(timeoutMs: number): Promise<void> {
    this.closed = true;
    this.desired = undefined;
    this.detach();
    this.setStatus({ state: "off" });
    this.listeners.clear();
    let timer: ReturnType<typeof globalThis.setTimeout> | undefined;
    const timedOut = new Promise<void>((resolve) => {
      timer = globalThis.setTimeout(() => {
        this.log("warn", `mcpwarp: close() gave up waiting after ${timeoutMs} ms`);
        resolve();
      }, timeoutMs);
    });
    try {
      await Promise.race([this.queue.then(() => this.draining), timedOut]);
    } finally {
      globalThis.clearTimeout(timer);
    }
  }

  /** Runs `task` after every earlier one; the chain itself never rejects. */
  private enqueue(task: () => Promise<void>): Promise<void> {
    const run = this.queue.then(task);
    this.queue = run.catch((e: unknown) => this.log("error", `mcpwarp: ${errorMessage(e)}`));
    return this.queue;
  }

  private async apply(cfg: TunnelConfig): Promise<void> {
    if (this.closed || cfg !== this.desired) return;
    this.detach();
    this.setStatus({ state: willStart(cfg) ? "connecting" : "off" });
    await this.draining;
    if (this.closed || cfg !== this.desired) return;
    if (willStart(cfg)) this.start(cfg);
    else this.log("info", "mcpwarp: off");
  }

  private start(cfg: TunnelConfig): void {
    const gen = ++this.generation;
    const live = (): boolean => gen === this.generation;
    const { signal, abort } = abortable();
    this.setStatus({ state: "connecting" });
    this.log("info", `mcpwarp: connecting as ${JSON.stringify(cfg.name)}${cfg.url ? ` via ${cfg.url}` : ""}`);
    // A listener or the logger may have called stop()/close() just now.
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
          webSocket: this.base.webSocket,
          reconnect: this.base.reconnect,
          maxRequestBodyBytes: this.base.maxRequestBodyBytes,
          signal,
          onRetry: (info) => {
            if (!live()) return;
            this.log("warn", `mcpwarp: retry #${info.attempt} in ${info.delayMs} ms: ${info.reason}`);
            this.setStatus({ ...this.current, lastError: info.reason });
            this.base.onRetry?.(info);
          },
        },
        this.makeServer,
        { onClientClose: (closed) => clientCloses.push(closed) },
      );
    } catch (e) {
      this.fail(cfg, e);
      return;
    }
    this.active = { gen, abort, started, clientCloses };
    started.then(
      (handle) => {
        if (!live()) return;
        this.wire(handle, cfg, live);
        switch (handle.state) {
          case "registered":
            this.log("info", `mcpwarp: online at ${handle.url}`);
            this.setStatus({ state: "online", url: handle.url });
            return;
          case "disabled":
            this.log("warn", "mcpwarp: the service is disabled in the mcpwarp dashboard");
            this.setStatus({ state: "disabled" });
            return;
          // The handle moved on between its first `registered`/`disabled` and the resolution reaching us.
          case "conflict":
            this.conflicted(cfg);
            return;
          case "closing":
          case "closed":
            this.fail(cfg, new Error("mcpwarp: the connection closed"));
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
      this.log("info", `mcpwarp: online at ${e.url}`);
      this.setStatus({ state: "online", url: e.url });
    });
    handle.on("disabled", (e) => {
      if (!live()) return;
      this.log("warn", `mcpwarp: the service was disabled (${e.reason})`);
      this.setStatus({ state: "disabled" });
    });
    handle.on("enabled", () => {
      if (!live()) return;
      this.log("info", "mcpwarp: the service was enabled, registering again");
      this.setStatus({ state: "connecting" });
    });
    handle.on("error", (err) => {
      if (!live()) return;
      const message = errorMessage(err);
      this.log("warn", `mcpwarp: ${message}`);
      if (this.current.state !== "online") this.setStatus({ ...this.current, lastError: message });
    });
    handle.on("disconnect", (reason) => {
      if (!live() || reason.fatal) return;
      this.log("warn", `mcpwarp: disconnected: ${reason.message}`);
      this.setStatus({ state: "connecting", lastError: reason.message });
    });
    handle.on("conflict", () => {
      if (!live()) return;
      this.conflicted(cfg);
    });
    handle.on("close", (e) => {
      if (!live()) return;
      this.fail(cfg, e.error ?? new Error("mcpwarp: the connection closed"));
    });
  }

  private conflicted(cfg: TunnelConfig): void {
    const message = `mcpwarp: another connection took over the name ${JSON.stringify(cfg.name)}; not reconnecting until configure() is called again`;
    this.log("warn", message);
    this.forget(cfg);
    this.detach();
    this.setStatus({ state: "conflict", lastError: message });
  }

  /** A fatal failure of the current generation (or a synchronous expose() error). */
  private fail(cfg: TunnelConfig, err: unknown): void {
    if (err instanceof McpwarpError && err.code === "TAKEN_OVER") {
      this.conflicted(cfg);
      return;
    }
    const message = errorMessage(err);
    this.log("error", `mcpwarp: ${message}`);
    this.forget(cfg);
    this.detach();
    this.setStatus({ state: "error", lastError: message });
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
    this.log("info", "mcpwarp: stopping");
    a.abort();
    const closed = a.started.then(
      (handle) => handle.close(),
      () => Promise.all(a.clientCloses).then(() => {}),
    ).catch((e: unknown) => this.log("warn", `mcpwarp: error while closing: ${errorMessage(e)}`));
    this.draining = Promise.all([this.draining, closed]).then(() => {});
  }

  private setStatus(next: TunnelStatus): void {
    const status: TunnelStatus = { state: next.state };
    if (next.url !== undefined) status.url = next.url;
    if (next.lastError !== undefined) status.lastError = next.lastError;
    if (sameStatus(this.current, status)) return;
    this.current = status;
    for (const listener of [...this.listeners]) this.notify(listener);
  }

  private notify(listener: (s: TunnelStatus) => void): void {
    try {
      listener(this.status);
    } catch (e) {
      this.log("error", `mcpwarp: status listener threw: ${errorMessage(e)}`);
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
export function createTunnel(base: TunnelBase): Tunnel {
  validateIntegration(base.integration);
  validateAgent(base.agent);
  return new TunnelImpl(base, adapterServer(base));
}
