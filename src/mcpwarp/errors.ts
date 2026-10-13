/** The mcpwarp helper's error type. */
export class McpwarpError extends Error {
  override name = "McpwarpError";
  /**
   * A RegisterErrorCode / AppErrorCode value, or "MALFORMED_ENVELOPE", "CLOSED",
   * "UNAUTHORIZED" (the edge rejected the token: ws-mixer close 4011),
   * "TAKEN_OVER" (a `conflict` arrived before the first `registered`; `expose()` rejects with it),
   * "DISCONNECTED_FATAL" (any other fatal transport failure, or `reconnect.maxAttempts` ran out;
   * also a fatal HTTP 401 at the upgrade, seen via `upgradeFailure`: it carries no ws-mixer code).
   */
  readonly code: string;
  /** True when it ended the exposure (the handle closes with it). */
  readonly fatal: boolean;
  readonly serviceName?: string;
  override readonly cause?: unknown;

  constructor(code: string, message: string, opts?: { fatal?: boolean; serviceName?: string; cause?: unknown }) {
    super(message);
    this.code = code;
    this.fatal = opts?.fatal ?? false;
    this.serviceName = opts?.serviceName;
    if (opts?.cause !== undefined) this.cause = opts.cause;
  }
}
