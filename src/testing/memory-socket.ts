/**
 * An in-memory WebSocket pair: the client half is a `WebSocketLike` the SDK
 * dials through a `webSocket` factory; the server half is what the fake
 * edge drives. Each direction delivers messages and the close in order, on
 * resolved promises (no re-entrancy). Platform-neutral.
 */
import type { WebSocketLike } from "../socket.js";

type Listener = (ev: any) => void;

export interface ServerSide {
  /** Set by the edge before the client opens. */
  onOpen?: () => void;
  onMessage?: (data: Uint8Array) => void;
  onClose?: (code: number, reason: string) => void;
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
  readonly closed: boolean;
}

export interface SocketPairOptions {
  /** The subprotocol the "server" echoes; "" to echo none. */
  protocol: string;
  /** Arms a rejected upgrade: the client gets `upgradeFailure`, then error + close 1006. */
  reject?: { status: number; headers?: Record<string, string | undefined> };
}

class ClientSocket implements WebSocketLike {
  readyState = 0;
  protocol = "";
  bufferedAmount = 0;
  binaryType = "blob";
  upgradeFailure?: { status: number; headers?: Record<string, string | undefined> };
  private readonly listeners = new Map<string, Set<Listener>>();
  private chain: Promise<void> = Promise.resolve();
  closedOnce = false;

  constructor(private readonly peer: () => ServerHalf) {}

  addEventListener(type: string, listener: Listener): void {
    let set = this.listeners.get(type);
    if (!set) {
      set = new Set();
      this.listeners.set(type, set);
    }
    set.add(listener);
  }

  removeEventListener(type: string, listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  dispatch(type: string, ev: unknown): void {
    for (const l of [...(this.listeners.get(type) ?? [])]) l(ev);
  }

  /** Queues a delivery to this (client) side behind earlier ones. */
  enqueue(fn: () => void): void {
    this.chain = this.chain.then(fn);
  }

  send(data: Uint8Array): void {
    if (this.readyState !== 1) throw new Error("WebSocket is not open");
    const copy = data.slice();
    this.peer().enqueue(() => this.peer().onMessage?.(copy));
  }

  close(code?: number, reason?: string): void {
    if (this.readyState >= 2) return;
    this.readyState = 2;
    const c = code ?? 1005;
    const r = reason ?? "";
    this.peer().closeFromClient(c, r);
  }
}

class ServerHalf implements ServerSide {
  onOpen?: () => void;
  onMessage?: (data: Uint8Array) => void;
  onClose?: (code: number, reason: string) => void;
  closed = false;
  private chain: Promise<void> = Promise.resolve();

  constructor(private readonly client: ClientSocket) {}

  enqueue(fn: () => void): void {
    this.chain = this.chain.then(fn);
  }

  send(data: Uint8Array): void {
    if (this.closed) return;
    const copy = data.slice();
    this.client.enqueue(() => {
      if (this.client.readyState === 1 || this.client.readyState === 2) {
        this.client.dispatch("message", { data: copy.buffer.slice(copy.byteOffset, copy.byteOffset + copy.byteLength) });
      }
    });
  }

  close(code = 1000, reason = ""): void {
    if (this.closed) return;
    this.closed = true;
    this.finishClient(code, reason);
  }

  closeFromClient(code: number, reason: string): void {
    if (!this.closed) {
      this.closed = true;
      this.enqueue(() => this.onClose?.(code, reason));
    }
    this.finishClient(code, reason);
  }

  private finishClient(code: number, reason: string): void {
    const client = this.client;
    client.enqueue(() => {
      if (client.closedOnce) return;
      client.closedOnce = true;
      client.readyState = 3;
      client.dispatch("close", { code, reason, wasClean: code !== 1006 });
    });
  }
}

export function createSocketPair(opts: SocketPairOptions): { client: WebSocketLike; server: ServerSide } {
  let server!: ServerHalf;
  const client = new ClientSocket(() => server);
  server = new ServerHalf(client);
  setTimeout(() => {
    if (client.readyState !== 0) return;
    if (opts.reject) {
      client.upgradeFailure = opts.reject;
      client.readyState = 3;
      server.closed = true;
      client.closedOnce = true;
      client.dispatch("error", { message: `Unexpected server response: ${opts.reject.status}` });
      client.dispatch("close", { code: 1006, reason: "", wasClean: false });
      return;
    }
    client.protocol = opts.protocol;
    client.readyState = 1;
    server.onOpen?.();
    client.dispatch("open", {});
  }, 0);
  return { client, server };
}
