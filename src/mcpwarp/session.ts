/**
 * The registration state machine for one exposed service, independent of
 * sockets and clocks (both injected). The tunnel keeps no registration
 * state across connections, so this re-registers on every `welcome` (first
 * connect, reconnect, drain replacement) until a `conflict` hands the name
 * to another connection; see docs/MCPWARP.md.
 */
import {
  AppErrorCode,
  RegisterErrorCode,
  TRANSIENT_REGISTER_ERRORS,
  decodeEnvelope,
  encodeRegister,
  encodeUnregister,
  type InboundEnvelope,
  type ServiceKind,
} from "./envelope.js";
import { McpwarpError } from "./errors.js";

/** "conflict": another connection of the same account took the name over; sticky until close(). */
export type ExposeState = "connecting" | "registering" | "registered" | "disabled" | "conflict" | "closing" | "closed";

/** The `conflict` event: the tunnel gave the service to a newer connection that registered the same name. */
export interface ExposeConflict {
  readonly serverId: string;
  readonly name: string;
  /** "taken_over". */
  readonly reason: string;
}

/** One successful register: `created` is true when it created the service, false when it already existed. */
export interface ExposeRegistration {
  readonly url: string;
  readonly serverId: string;
  readonly created: boolean;
}

export interface SessionDeps {
  name: string;
  kind: ServiceKind;
  /** client.sendApp; rejections are swallowed (the next welcome re-registers). */
  sendApp(body: Record<string, unknown>): Promise<void>;
  setTimeout(fn: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
  /** "retry": a transient register error, re-sent after `delayMs` ({ attempt, delayMs, reason }). */
  emit(event: "registered" | "disabled" | "enabled" | "conflict" | "error" | "retry" | "state", payload: unknown): void;
  /** expose.ts closes the client and ends the handle. */
  fatal(err: McpwarpError): void;
}

/** How the `retry` reason of a transient register error starts; tunnel.ts tells it from a redial by it. */
export const REGISTER_RETRY_PREFIX = "register failed";

const RETRY_BASE_MS = 1000;
const RETRY_CAP_MS = 30_000;
const OVERLOAD_PAUSE_MS = 1000;
const UNREGISTER_BUDGET_MS = 2000;

export class Session {
  state: ExposeState = "connecting";
  url?: string;
  serverId?: string;
  registration?: ExposeRegistration;
  everRegistered = false;
  /** Set by `conflict`; survives reconnects: the session never registers again. */
  takenOver = false;

  private readonly deps: SessionDeps;
  private connected = false;
  private sentRegisterThisConn = false;
  private outstanding: "register" | "unregister" | undefined;
  private transientFailures = 0;
  private retryTimer: unknown;
  private overloadTimer: unknown;
  private unregisteredWaiter: (() => void) | null = null;

  constructor(deps: SessionDeps) {
    this.deps = deps;
  }

  /** A connection welcomed (client.onConnect, after the new conn is the active one). */
  onWelcome(): void {
    if (this.isEnding()) return;
    this.connected = true;
    this.sentRegisterThisConn = false;
    this.cancelTimers();
    if (this.takenOver) return;
    this.setState("registering");
    this.sendRegister();
  }

  onDisconnect(): void {
    if (this.isEnding()) return;
    this.connected = false;
    this.sentRegisterThisConn = false;
    this.outstanding = undefined;
    this.cancelTimers();
    if (!this.takenOver) this.setState("connecting");
  }

  onApp(body: Record<string, unknown>): void {
    let env: InboundEnvelope | undefined;
    try {
      env = decodeEnvelope(body);
    } catch (e) {
      this.deps.emit("error", e);
      return;
    }
    if (!env) return;
    switch (env.op) {
      case "registered":
        this.onRegistered(env);
        return;
      case "unregistered":
        // The tunnel answers with no services when this connection held nothing for the name.
        if (this.outstanding === "unregister" || env.services.some((s) => s.name === this.deps.name)) {
          if (this.outstanding === "unregister") this.outstanding = undefined;
          const waiter = this.unregisteredWaiter;
          this.unregisteredWaiter = null;
          waiter?.();
        }
        return;
      case "disable":
        if (this.isEnding() || this.takenOver) return;
        if (this.serverId === undefined || env.id === this.serverId) {
          this.serverId = env.id;
          this.setState("disabled");
          this.deps.emit("disabled", { serverId: this.serverId, reason: env.reason });
        }
        return;
      case "enable":
        if (this.isEnding() || this.takenOver || env.name !== this.deps.name) return;
        this.serverId = env.id;
        this.deps.emit("enabled", { serverId: env.id });
        this.setState("registering");
        this.sendRegister();
        return;
      case "conflict":
        // onApp carries no connection: safe because the tunnel never sends `conflict` to a connection it is draining.
        if (this.isEnding() || this.takenOver) return;
        if (env.name !== this.deps.name && (this.serverId === undefined || env.id !== this.serverId)) return;
        this.takenOver = true;
        this.outstanding = undefined;
        // The tunnel no longer holds the name for this connection: close() has nothing to unregister.
        this.sentRegisterThisConn = false;
        this.cancelTimers();
        this.setState("conflict");
        this.deps.emit("conflict", Object.freeze({ serverId: env.id, name: env.name, reason: env.reason }));
        return;
      case "error":
        this.onAppError(env.code, env.message);
        return;
      case "unknown":
        return;
    }
  }

  /** Sends unregister if a register went out on the live connection; resolves on `unregistered` for our name or after `budgetMs`. Never rejects. */
  async beginClose(budgetMs = UNREGISTER_BUDGET_MS): Promise<void> {
    if (this.isEnding()) return;
    const wasRegistered = this.connected && this.sentRegisterThisConn;
    this.setState("closing");
    this.cancelTimers();
    if (!wasRegistered) return;
    await new Promise<void>((resolve) => {
      const timer = this.deps.setTimeout(() => {
        this.unregisteredWaiter = null;
        resolve();
      }, budgetMs);
      this.unregisteredWaiter = () => {
        this.deps.clearTimeout(timer);
        resolve();
      };
      this.outstanding = "unregister";
      this.deps.sendApp(encodeUnregister(this.deps.name)).catch(() => {
        // the connection went away: nothing to wait for
        this.unregisteredWaiter?.();
        this.unregisteredWaiter = null;
      });
    });
  }

  /** The handle finished closing (expose.ts). */
  markClosed(): void {
    this.cancelTimers();
    this.setState("closed");
  }

  private onRegistered(env: Extract<InboundEnvelope, { op: "registered" }>): void {
    if (this.isEnding() || this.takenOver) return;
    const svc = env.services.find((s) => s.name === this.deps.name);
    if (svc) {
      this.outstanding = undefined;
      this.transientFailures = 0;
      this.url = svc.url;
      this.serverId = svc.id;
      this.registration = Object.freeze({ url: svc.url, serverId: svc.id, created: svc.created });
      this.everRegistered = true;
      this.setState("registered");
      this.deps.emit("registered", this.registration);
      return;
    }
    const err = env.errors.find((e) => e.name === this.deps.name);
    if (!err) return;
    this.outstanding = undefined;
    if (err.code === RegisterErrorCode.SERVER_DISABLED) {
      this.serverId = err.id ?? this.serverId;
      this.setState("disabled");
      this.deps.emit("disabled", { serverId: this.serverId, reason: "disabled" });
      return;
    }
    if (TRANSIENT_REGISTER_ERRORS.has(err.code)) {
      this.deps.emit("error", new McpwarpError(err.code, err.message, { serviceName: this.deps.name }));
      const delay = Math.min(RETRY_BASE_MS * 2 ** this.transientFailures, RETRY_CAP_MS);
      this.transientFailures++;
      this.deps.emit("retry", { attempt: this.transientFailures, delayMs: delay, reason: `${REGISTER_RETRY_PREFIX}: ${err.code}: ${err.message}` });
      this.clearRetry();
      this.retryTimer = this.deps.setTimeout(() => {
        this.retryTimer = undefined;
        if (!this.isEnding() && this.connected) this.sendRegister();
      }, delay);
      return;
    }
    if (!this.everRegistered) {
      this.deps.fatal(new McpwarpError(err.code, err.message, { fatal: true, serviceName: this.deps.name }));
      return;
    }
    // Registered before on an earlier connection: keep going, the next welcome retries.
    this.deps.emit("error", new McpwarpError(err.code, err.message, { serviceName: this.deps.name }));
  }

  private onAppError(code: string, message: string): void {
    if (code === AppErrorCode.UNSUPPORTED_VERSION) {
      this.deps.fatal(new McpwarpError(code, message, { fatal: true, serviceName: this.deps.name }));
      return;
    }
    if (code === AppErrorCode.OVERLOADED) {
      // Mirrors the Go CLI's OverloadPause: re-send what is outstanding once, after a pause.
      const op = this.outstanding;
      if (op && this.overloadTimer === undefined) {
        this.overloadTimer = this.deps.setTimeout(() => {
          this.overloadTimer = undefined;
          if (!this.connected) return;
          if (op === "register" && !this.isEnding()) this.sendRegister();
          else if (op === "unregister" && this.state === "closing") void this.deps.sendApp(encodeUnregister(this.deps.name)).catch(() => {});
        }, OVERLOAD_PAUSE_MS);
      }
      return;
    }
    this.deps.emit("error", new McpwarpError(code, message, { serviceName: this.deps.name }));
  }

  private sendRegister(): void {
    this.outstanding = "register";
    this.sentRegisterThisConn = true;
    this.deps.sendApp(encodeRegister(this.deps.name, this.deps.kind)).catch(() => {});
  }

  private isEnding(): boolean {
    return this.state === "closing" || this.state === "closed";
  }

  private setState(next: ExposeState): void {
    if (this.state === next) return;
    this.state = next;
    this.deps.emit("state", next);
  }

  private clearRetry(): void {
    if (this.retryTimer !== undefined) this.deps.clearTimeout(this.retryTimer);
    this.retryTimer = undefined;
  }

  private cancelTimers(): void {
    this.clearRetry();
    if (this.overloadTimer !== undefined) this.deps.clearTimeout(this.overloadTimer);
    this.overloadTimer = undefined;
  }
}
