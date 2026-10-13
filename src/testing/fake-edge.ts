/**
 * An in-process fake of the mcpwarp edge, for testing an integration
 * end to end without a network: the agent dials it through
 * `edge.webSocket`, registers over the mcpwarp control envelope, and test
 * code sends HTTP requests to the public URL through `edge.fetch` -- each
 * one becomes a ws-mixer stream carrying raw HTTP/1.1, exactly as the real
 * edge writes it. Platform-neutral; `fetch()` additionally needs global
 * `ReadableStream`/`Request`/`Response`/`Headers`.
 *
 * Known differences from the real edge: it writes the request body while
 * already reading the response (the edge writes the whole request first),
 * and `enable` goes only to the routed (or latest) connection (the edge
 * sends it to every connection of the owner), and a second connection
 * registering a held name just takes the route (the tunnel also sends the
 * old holder `conflict`; here that is `edge.conflict()`).
 */
import type { HelloMsg } from "../control.js";
import { ErrorCode } from "../errors.js";
import { SUBPROTOCOL, type WebSocketFactory } from "../socket.js";
import type { MixerStream } from "../stream.js";
import { HOP_BY_HOP, connectionTokens } from "../http1/headers.js";
import { StreamByteReader } from "../http1/reader.js";
import { serializeRequestHead } from "../http1/request.js";
import { isBodyless, readResponse } from "../http1/response.js";
import { EdgeConnection, type FakeEdgeConnection } from "./edge-conn.js";
import { createSocketPair } from "./memory-socket.js";

export type { FakeEdgeConnection } from "./edge-conn.js";

export interface FakeEdgeOptions {
  /** Default "tester". */
  username?: string;
  /** Default "mcpwarp.test". */
  domain?: string;
  /** Default 262144. */
  window?: number;
  /** Default 64. */
  maxStreams?: number;
  /** Default 30000. */
  pingInterval?: number;
  /** Default 90000. */
  pingTimeout?: number;
  /** How long a request waits for the response head. Default 30000. */
  requestTimeoutMs?: number;
  /** Default: any non-empty token. */
  acceptToken?: (token: string) => boolean;
}

export interface FakeEdgeUpgrade {
  url: string;
  protocols: string[];
  headers: Record<string, string>;
}

interface ServiceEntry {
  conn?: EdgeConnection;
  disabled: boolean;
  everRegistered: boolean;
  failure?: { error: { code: string; message?: string; id?: string }; times: number };
  withhold: number;
  open: Set<MixerStream>;
}

const MAX_DATA = 16384;
const utf8 = new TextEncoder();

export class FakeEdge {
  readonly url: string;
  readonly upgrades: FakeEdgeUpgrade[] = [];
  readonly hellos: HelloMsg[] = [];
  /** Every `app` body received from agents, in order. */
  readonly appLog: Record<string, unknown>[] = [];
  /** Live connections, oldest first. */
  readonly connections: FakeEdgeConnection[] = [];

  private readonly opts: Required<Omit<FakeEdgeOptions, "acceptToken">> & { acceptToken: (token: string) => boolean };
  private started = false;
  private sessionCounter = 0;
  private pendingReject: { status: number; headers?: Record<string, string> } | undefined;
  private readonly services = new Map<string, ServiceEntry>();
  private readonly appWaiters = new Set<() => void>();
  private readonly pending = new Set<(err: Error) => void>();

  constructor(opts: FakeEdgeOptions = {}) {
    this.opts = {
      username: opts.username ?? "tester",
      domain: opts.domain ?? "mcpwarp.test",
      window: opts.window ?? 262144,
      maxStreams: opts.maxStreams ?? 64,
      pingInterval: opts.pingInterval ?? 30000,
      pingTimeout: opts.pingTimeout ?? 90000,
      requestTimeoutMs: opts.requestTimeoutMs ?? 30000,
      acceptToken: opts.acceptToken ?? ((token) => token.length > 0),
    };
    this.url = `wss://connect.${this.opts.domain}`;
  }

  /** Pass to `connect()`/`expose()` as `webSocket`. Throws until start(). */
  readonly webSocket: WebSocketFactory = (url, init) => {
    this.upgrades.push({ url, protocols: [...init.protocols], headers: { ...init.headers } });
    if (!this.started) throw new Error("fake edge: not started");
    const reject = this.pendingReject;
    this.pendingReject = undefined;
    const { client, server } = createSocketPair({
      protocol: init.protocols.includes(SUBPROTOCOL) ? SUBPROTOCOL : "",
      reject,
    });
    server.onOpen = () => {
      const conn = new EdgeConnection(
        server,
        {
          onHello: (_c, hello) => this.hellos.push(hello),
          onApp: (c, body) => this.onApp(c, body),
          onClosed: (c) => this.onClosed(c),
          acceptToken: (token) => this.opts.acceptToken(token),
        },
        {
          session: `fake-${++this.sessionCounter}`,
          window: this.opts.window,
          maxStreams: this.opts.maxStreams,
          pingInterval: this.opts.pingInterval,
          pingTimeout: this.opts.pingTimeout,
        },
      );
      this.connections.push(conn);
    };
    return client;
  };

  /** `request()`, bound: pass as an MCP client transport's `fetch`. */
  readonly fetch = (input: string | URL | Request, init?: RequestInit): Promise<Response> => this.request(input, init);

  async start(): Promise<void> {
    this.started = true;
  }

  /** Closes every connection (1001) and rejects pending requests. */
  async stop(): Promise<void> {
    this.started = false;
    for (const reject of [...this.pending]) reject(new Error("fake edge: stopped"));
    for (const c of [...this.connections] as EdgeConnection[]) c.close(1001);
  }

  publicUrl(name: string): string {
    return `https://${this.publicHost(name)}/mcp`;
  }

  serverId(name: string): string {
    return `srv_${name}`;
  }

  /** The next register(s) for `name` fail with `error` (`times` default 1; Infinity until cleared with null). */
  failRegister(name: string, error: { code: string; message?: string; id?: string } | null, times = 1): void {
    const s = this.service(name);
    s.failure = error ? { error, times } : undefined;
  }

  /**
   * The next register(s) for `name` (`times` default 1) route it to the
   * connection but get no `registered` reply, as when the tunnel's
   * `conflict` overtakes it (follow with `conflict(name)`).
   */
  withholdRegistered(name: string, times = 1): void {
    this.service(name).withhold = times;
  }

  /** The next dial is rejected at the HTTP upgrade (visible as `WebSocketLike.upgradeFailure`). */
  rejectNextUpgrade(status: number, headers?: Record<string, string>): void {
    this.pendingReject = { status, headers };
  }

  /** Resolves with the `count`-th app body (counting from the first ever received) whose `mcpwarp.op` is `op`. */
  waitForApp(op: string, opts?: { count?: number; timeoutMs?: number }): Promise<Record<string, unknown>> {
    const count = opts?.count ?? 1;
    const find = () => this.appLog.filter((b) => (b.mcpwarp as { op?: unknown } | undefined)?.op === op)[count - 1];
    return this.waitUntil(find, opts?.timeoutMs ?? 5000, `app op ${op} #${count}`);
  }

  /** Resolves once `name` is routed to a connection. */
  waitForRegistered(name: string, timeoutMs = 5000): Promise<{ id: string; url: string }> {
    const find = () => (this.services.get(name)?.conn ? { id: this.serverId(name), url: this.publicUrl(name) } : undefined);
    return this.waitUntil(find, timeoutMs, `registration of ${name}`);
  }

  async sendApp(body: Record<string, unknown>, conn?: FakeEdgeConnection): Promise<void> {
    this.connFor(conn).sendApp(body);
  }

  /** RESET(CANCEL)s the service's open streams, marks it disabled, sends disable{id, reason}. */
  async disable(name: string, reason = "disabled"): Promise<void> {
    const s = this.service(name);
    s.disabled = true;
    for (const st of [...s.open]) st.reset(ErrorCode.CANCEL, "service disabled");
    s.conn?.sendApp({ mcpwarp: { v: 1, op: "disable", id: this.serverId(name), reason } });
  }

  /** Marks the service enabled and sends enable{id, name}. */
  async enable(name: string): Promise<void> {
    const s = this.service(name);
    s.disabled = false;
    (s.conn ?? this.latest())?.sendApp({ mcpwarp: { v: 1, op: "enable", id: this.serverId(name), name } });
  }

  /**
   * What the tunnel does to the old holder when another connection of the
   * same account registers `name`: RESET(CANCEL)s the service's open
   * streams, unroutes it, then sends conflict{id, name, reason:"taken_over"}
   * to the connection that held it (default: the latest). No new holder is
   * simulated: requests to the name fail until something registers it.
   */
  async conflict(name: string, conn?: FakeEdgeConnection): Promise<void> {
    const s = this.service(name);
    const holder = (conn as EdgeConnection | undefined) ?? s.conn ?? this.latest();
    for (const st of [...s.open]) st.reset(ErrorCode.CANCEL, "server taken over");
    if (s.conn === holder) s.conn = undefined;
    holder?.sendApp({ mcpwarp: { v: 1, op: "conflict", id: this.serverId(name), name, reason: "taken_over" } });
  }

  async appError(code: string, message = "", conn?: FakeEdgeConnection): Promise<void> {
    this.connFor(conn).sendApp({ mcpwarp: { v: 1, op: "error", code, message } });
  }

  /** Sends drain on `conn` (default: the latest); at `deadlineMs` (default 30000) survivors are RESET and it closes 4012. */
  async drain(opts?: { reason?: string; deadlineMs?: number; conn?: FakeEdgeConnection }): Promise<void> {
    this.connFor(opts?.conn).drain(opts?.reason ?? "rollout", opts?.deadlineMs ?? 30000);
  }

  closeConnection(opts: { wsCode: number; errorCode?: number; message?: string; conn?: FakeEdgeConnection }): void {
    this.connFor(opts.conn).close(opts.wsCode, opts.errorCode, opts.message ?? "");
  }

  /** Sends one HTTP request to a public URL through the routed agent, the way the real edge does. */
  async request(input: string | URL | Request, init?: RequestInit): Promise<Response> {
    const req = input instanceof Request ? input : undefined;
    const url = new URL(req ? req.url : input instanceof URL ? input.href : String(input));
    const method = (init?.method ?? req?.method ?? "GET").toUpperCase();
    const headers = new Headers(req?.headers);
    new Headers(init?.headers).forEach((v, k) => headers.set(k, v));
    const signal = init?.signal ?? req?.signal ?? undefined;

    const name = this.serviceNameForHost(url.host);
    if (name === undefined) throw new Error(`fake edge: ${url.host} is not a service host of ${this.opts.domain}`);
    const svc = this.services.get(name);
    if (!svc?.conn) throw new Error(`fake edge: no agent registered for ${name}`);
    if (svc.disabled) {
      return new Response(JSON.stringify({ error: "service disabled" }), { status: 503, headers: { "content-type": "application/json" } });
    }
    if (signal?.aborted) throw signal.reason ?? new Error("aborted");

    // Body framing like Go's Request.Write: known length -> Content-Length, a stream -> chunked.
    let bytes: Uint8Array | null = null;
    let bodyStream: ReadableStream<Uint8Array> | null = null;
    const body = init?.body ?? null;
    if (body !== null) {
      if (typeof body === "string") {
        bytes = utf8.encode(body);
        if (!headers.has("content-type")) headers.set("content-type", "text/plain;charset=UTF-8");
      } else if (body instanceof URLSearchParams) {
        bytes = utf8.encode(body.toString());
        if (!headers.has("content-type")) headers.set("content-type", "application/x-www-form-urlencoded;charset=UTF-8");
      } else if (body instanceof ArrayBuffer) bytes = new Uint8Array(body);
      else if (ArrayBuffer.isView(body)) bytes = new Uint8Array(body.buffer, body.byteOffset, body.byteLength);
      else if (typeof (body as ReadableStream).getReader === "function") bodyStream = body as ReadableStream<Uint8Array>;
      else if (typeof (body as Blob).arrayBuffer === "function") bytes = new Uint8Array(await (body as Blob).arrayBuffer());
      else bytes = utf8.encode(String(body));
    } else if (req?.body) {
      bytes = new Uint8Array(await req.arrayBuffer());
    }

    const sendHeaders = new Headers();
    const listed = connectionTokens(headers);
    headers.forEach((v, k) => {
      if (HOP_BY_HOP.has(k) || listed.has(k) || k === "authorization" || k === "host" || k === "content-length") return;
      sendHeaders.set(k, v);
    });
    if (!sendHeaders.has("user-agent")) sendHeaders.set("user-agent", "Go-http-client/1.1");

    const stream = svc.conn.openStream();
    svc.open.add(stream);
    void stream.closed.then(() => svc.open.delete(stream));
    const framing = bodyStream ? "chunked" : bytes ? { length: bytes.length } : "none";
    const target = url.pathname + url.search;
    const host = url.host;

    // Written in the background: an agent may answer before reading the whole body.
    let requestDone = false;
    void (async () => {
      await stream.write(serializeRequestHead(method, target, host, sendHeaders, framing));
      if (bytes) {
        for (let i = 0; i < bytes.length; i += MAX_DATA) await stream.write(bytes.subarray(i, i + MAX_DATA));
      } else if (bodyStream) {
        const reader = bodyStream.getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          if (value.length === 0) continue;
          await stream.write(utf8.encode(`${value.length.toString(16)}\r\n`));
          await stream.write(value);
          await stream.write(utf8.encode("\r\n"));
        }
        await stream.write(utf8.encode("0\r\n\r\n"));
      }
      await stream.closeWrite();
      requestDone = true;
    })().catch(() => {});

    const onAbort = () => stream.reset("CANCEL", "request aborted");
    signal?.addEventListener("abort", onAbort, { once: true });
    const cleanup = () => signal?.removeEventListener("abort", onAbort);

    const reader = new StreamByteReader(stream);
    let parsed: Awaited<ReturnType<typeof readResponse>>;
    try {
      parsed = await new Promise<Awaited<ReturnType<typeof readResponse>>>((resolve, reject) => {
        const timer = setTimeout(() => {
          stream.reset("CANCEL", "response head timeout");
          reject(new Error(`fake edge: no response head within ${this.opts.requestTimeoutMs}ms`));
        }, this.opts.requestTimeoutMs);
        const settle = (err?: Error) => {
          clearTimeout(timer);
          this.pending.delete(rejectPending);
          if (err) reject(err);
        };
        const rejectPending = (err: Error) => {
          stream.reset("CANCEL", "fake edge stopped");
          settle(err);
        };
        this.pending.add(rejectPending);
        readResponse(reader, method).then(
          (r) => {
            settle();
            resolve(r);
          },
          (e: unknown) => settle(signal?.aborted ? toError(signal.reason) : toError(e)),
        );
      });
    } catch (e) {
      cleanup();
      throw e;
    }
    if (parsed.head.status < 200) {
      cleanup();
      stream.reset("CANCEL", "interim response");
      throw new Error("agent sent an interim 1xx response");
    }

    const finish = () => {
      cleanup();
      // The agent answered before consuming the whole request body: abandon the rest.
      if (!requestDone) stream.reset("CANCEL", "response complete");
    };
    const responseHeaders = new Headers();
    for (const [k, v] of parsed.head.headers) responseHeaders.append(k, v);
    const noBody = isBodyless(method, parsed.head.status);
    let out: ReadableStream<Uint8Array> | null = null;
    if (noBody) {
      finish();
    } else {
      const it = parsed.body[Symbol.asyncIterator]();
      out = new ReadableStream<Uint8Array>({
        async pull(controller) {
          try {
            const { done, value } = await it.next();
            if (done) {
              controller.close();
              finish();
            } else {
              controller.enqueue(value);
            }
          } catch (e) {
            cleanup();
            controller.error(signal?.aborted ? toError(signal.reason) : toError(e));
          }
        },
        cancel() {
          cleanup();
          stream.reset("CANCEL", "response body cancelled");
        },
      });
    }
    return new Response(out, { status: parsed.head.status, statusText: parsed.head.statusText, headers: responseHeaders });
  }

  private publicHost(name: string): string {
    return `${name}-${this.opts.username}.${this.opts.domain}`;
  }

  private serviceNameForHost(host: string): string | undefined {
    const suffix = `-${this.opts.username}.${this.opts.domain}`;
    const h = host.toLowerCase();
    return h.endsWith(suffix) && h.length > suffix.length ? h.slice(0, -suffix.length) : undefined;
  }

  private service(name: string): ServiceEntry {
    let s = this.services.get(name);
    if (!s) {
      s = { disabled: false, everRegistered: false, withhold: 0, open: new Set() };
      this.services.set(name, s);
    }
    return s;
  }

  private latest(): EdgeConnection | undefined {
    return this.connections[this.connections.length - 1] as EdgeConnection | undefined;
  }

  private connFor(conn?: FakeEdgeConnection): EdgeConnection {
    const c = (conn as EdgeConnection | undefined) ?? this.latest();
    if (!c) throw new Error("fake edge: no live connection");
    return c;
  }

  private waitUntil<T>(find: () => T | undefined, timeoutMs: number, what: string): Promise<T> {
    const found = find();
    if (found !== undefined) return Promise.resolve(found);
    return new Promise((resolve, reject) => {
      const check = () => {
        const v = find();
        if (v === undefined) return;
        clearTimeout(timer);
        this.appWaiters.delete(check);
        resolve(v);
      };
      const timer = setTimeout(() => {
        this.appWaiters.delete(check);
        reject(new Error(`fake edge: timed out waiting for ${what}`));
      }, timeoutMs);
      this.appWaiters.add(check);
    });
  }

  private notifyWaiters(): void {
    for (const w of [...this.appWaiters]) w();
  }

  private onClosed(conn: EdgeConnection): void {
    const i = this.connections.indexOf(conn);
    if (i >= 0) this.connections.splice(i, 1);
    for (const s of this.services.values()) if (s.conn === conn) s.conn = undefined;
    this.notifyWaiters();
  }

  private onApp(conn: EdgeConnection, body: Record<string, unknown>): void {
    this.appLog.push(body);
    const env = body.mcpwarp;
    if (typeof env === "object" && env !== null && !Array.isArray(env)) {
      const e = env as Record<string, unknown>;
      if (e.v !== 1) {
        conn.sendApp({ mcpwarp: { v: 1, op: "error", code: "UNSUPPORTED_VERSION", message: `unsupported version ${String(e.v)}` } });
      } else if (e.op === "register") {
        this.onRegister(conn, e.services);
      } else if (e.op === "unregister") {
        this.onUnregister(conn, e.services);
      } else {
        conn.sendApp({ mcpwarp: { v: 1, op: "error", code: "UNKNOWN_OP", message: `unknown op ${String(e.op)}` } });
      }
    }
    this.notifyWaiters();
  }

  private onRegister(conn: EdgeConnection, raw: unknown): void {
    const services: Array<Record<string, unknown>> = [];
    const errors: Array<Record<string, unknown>> = [];
    let withheld = false;
    for (const item of Array.isArray(raw) ? raw : []) {
      const name = (item as { name?: unknown })?.name;
      if (typeof name !== "string") continue;
      const s = this.service(name);
      if (s.withhold > 0) {
        s.withhold--;
        s.conn = conn;
        s.everRegistered = true;
        withheld = true;
        continue;
      }
      if (s.failure && s.failure.times > 0) {
        const { code, message, id } = s.failure.error;
        errors.push({ name, code, message: message ?? code.toLowerCase(), ...(id !== undefined ? { id } : {}) });
        s.failure.times--;
        if (s.failure.times <= 0) s.failure = undefined;
        continue;
      }
      if (s.disabled) {
        errors.push({ name, code: "SERVER_DISABLED", message: "server disabled", id: this.serverId(name) });
        continue;
      }
      s.conn = conn;
      services.push({ name, id: this.serverId(name), url: this.publicUrl(name), created: !s.everRegistered });
      s.everRegistered = true;
    }
    if (withheld && services.length === 0 && errors.length === 0) return;
    conn.sendApp({ mcpwarp: { v: 1, op: "registered", services, errors } });
  }

  private onUnregister(conn: EdgeConnection, raw: unknown): void {
    const services: Array<Record<string, unknown>> = [];
    for (const item of Array.isArray(raw) ? raw : []) {
      const name = (item as { name?: unknown })?.name;
      if (typeof name !== "string") continue;
      const s = this.services.get(name);
      // Like the tunnel: names this connection does not hold are skipped.
      if (s?.conn !== conn) continue;
      s.conn = undefined;
      services.push({ name, id: this.serverId(name) });
    }
    conn.sendApp({ mcpwarp: { v: 1, op: "unregistered", services } });
  }
}

function toError(e: unknown): Error {
  return e instanceof Error ? e : new Error(String(e));
}
