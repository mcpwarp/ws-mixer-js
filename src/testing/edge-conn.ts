/**
 * The server side of one fake-edge connection: hello/welcome, ping/pong,
 * `app` routing, server-opened streams (odd ids) with flow control in both
 * directions (a MixerStream per stream, credit-back as the edge reads),
 * CLOSE/RESET, and drain. Platform-neutral.
 */
import { encodeControl, parseControl, type ControlMessage, type HelloMsg } from "../control.js";
import { ConnError, ErrorCode, StreamError, WsMixerError, closeCode } from "../errors.js";
import { FrameType, decodeFrame, encodeData, encodeOpen, encodeReset, resetCode, resetMessage, windowIncrement } from "../frame.js";
import { MixerStream } from "../stream.js";
import type { ServerSide } from "./memory-socket.js";

export interface EdgeConnHooks {
  onHello(conn: EdgeConnection, hello: HelloMsg): void;
  /** Called with every `app` body the agent sends. */
  onApp(conn: EdgeConnection, body: Record<string, unknown>): void;
  onClosed(conn: EdgeConnection): void;
  acceptToken(token: string): boolean;
}

export interface EdgeConnOptions {
  session: string;
  window: number;
  maxStreams: number;
  pingInterval: number;
  pingTimeout: number;
}

/** What tests see of one fake-edge connection. */
export interface FakeEdgeConnection {
  readonly session: string;
  readonly hello: HelloMsg;
  readonly closed: boolean;
}

export class EdgeConnection implements FakeEdgeConnection {
  readonly session: string;
  hello!: HelloMsg;
  closed = false;
  private welcomed = false;
  private peerWindow = 262144;
  private nextStreamId = 1;
  private highestOpened = 0;
  private draining = false;
  private drainTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly streams = new Map<number, MixerStream>();

  constructor(
    private readonly socket: ServerSide,
    private readonly hooks: EdgeConnHooks,
    private readonly opts: EdgeConnOptions,
  ) {
    this.session = opts.session;
    socket.onMessage = (data) => this.onMessage(data);
    socket.onClose = () => this.onSocketClosed();
  }

  /** Opens a stream toward the agent. Throws once closed or draining. */
  openStream(): MixerStream {
    if (this.closed || !this.welcomed) throw new Error("fake edge: connection is not usable");
    if (this.draining) throw new Error("fake edge: connection is draining");
    const id = this.nextStreamId;
    this.nextStreamId += 2;
    this.highestOpened = id;
    const stream = new MixerStream(
      id,
      {
        sendData: async (sid, chunk) => {
          if (this.closed) throw new ConnError(ErrorCode.INTERNAL_ERROR, "connection closed");
          this.socket.send(encodeData(sid, chunk));
        },
        sendControlFrame: (frame) => {
          if (!this.closed) this.socket.send(frame);
        },
        retireStream: (sid) => this.retire(sid),
      },
      this.opts.window,
      this.peerWindow,
    );
    this.streams.set(id, stream);
    this.socket.send(encodeOpen(id));
    return stream;
  }

  sendControl(msg: Record<string, unknown>): void {
    if (this.closed) return;
    this.socket.send(encodeData(0, encodeControl(msg as unknown as ControlMessage)));
  }

  sendApp(body: Record<string, unknown>): void {
    this.sendControl({ t: "app", body });
  }

  /** drain: no new streams; survivors are RESET(CANCEL) at the deadline, then error{GOING_AWAY} + close 4012 (earlier once none are left). */
  drain(reason: string, deadlineMs: number): void {
    if (this.closed || this.draining) return;
    this.draining = true;
    this.sendControl({ t: "drain", reason, last_stream_id: this.highestOpened, deadline_ms: deadlineMs });
    if (this.streams.size === 0) {
      this.closeGoingAway();
      return;
    }
    this.drainTimer = setTimeout(() => {
      for (const s of [...this.streams.values()]) s.reset(ErrorCode.CANCEL, "drain deadline");
      this.closeGoingAway();
    }, deadlineMs);
  }

  /** Closes with `wsCode`, sending error{errorCode, message} first when given. */
  close(wsCode: number, errorCode?: number, message = ""): void {
    if (this.closed) return;
    if (errorCode !== undefined) this.sendControl({ t: "error", code: errorCode, message });
    this.socket.close(wsCode, message.slice(0, 123));
    this.onSocketClosed();
  }

  private closeGoingAway(): void {
    this.close(4000 + ErrorCode.GOING_AWAY, ErrorCode.GOING_AWAY, "drained");
  }

  private retire(id: number): void {
    this.streams.delete(id);
    if (this.draining && this.streams.size === 0) {
      // Let the stream's own last frame (its CLOSE) go out first.
      void Promise.resolve().then(() => this.closeGoingAway());
    }
  }

  private fail(err: WsMixerError): void {
    this.close(closeCode(err.code), err.code, err.message);
  }

  private onMessage(data: Uint8Array): void {
    if (this.closed) return;
    try {
      const frame = decodeFrame(data);
      if (frame.streamId === 0) {
        if (frame.type === FrameType.DATA) this.onControl(frame.payload);
        return;
      }
      if (!this.welcomed) throw new ConnError(ErrorCode.PROTOCOL_ERROR, "frame before welcome");
      const stream = this.streams.get(frame.streamId);
      if (!stream) return;
      switch (frame.type) {
        case FrameType.DATA:
          stream.handleData(frame.payload);
          break;
        case FrameType.WINDOW:
          stream.handleWindow(windowIncrement(frame));
          break;
        case FrameType.CLOSE:
          stream.handleClose();
          break;
        case FrameType.RESET:
          stream.handleReset(resetCode(frame), resetMessage(frame));
          break;
      }
    } catch (e) {
      if (e instanceof StreamError) {
        const s = this.streams.get(e.streamId!);
        if (s) s.abort(e.code, e.message);
        else this.socket.send(encodeReset(e.streamId!, e.code, e.message));
        return;
      }
      this.fail(e instanceof WsMixerError ? e : new ConnError(ErrorCode.INTERNAL_ERROR, String(e)));
    }
  }

  private onControl(payload: Uint8Array): void {
    const msg = parseControl(payload);
    if (!this.welcomed) {
      if (msg.t !== "hello") throw new ConnError(ErrorCode.PROTOCOL_ERROR, `expected hello, got ${msg.t}`);
      this.hello = msg;
      this.hooks.onHello(this, msg);
      if (!this.hooks.acceptToken(msg.token)) {
        this.close(4000 + ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED, "invalid token");
        return;
      }
      this.peerWindow = msg.window ?? 262144;
      this.welcomed = true;
      this.sendControl({
        t: "welcome",
        v: 1,
        session: this.session,
        window: this.opts.window,
        max_streams: this.opts.maxStreams,
        ping_interval: this.opts.pingInterval,
        ping_timeout: this.opts.pingTimeout,
        server: { name: "fake-edge" },
      });
      return;
    }
    switch (msg.t) {
      case "ping":
        this.sendControl({ t: "pong", id: msg.id, ...(msg.ts !== undefined ? { ts: msg.ts } : {}) });
        return;
      case "app":
        this.hooks.onApp(this, msg.body);
        return;
      case "drain":
        // The agent is closing (client_requested): open nothing new.
        this.draining = true;
        return;
      case "error": {
        // `error` is the agent's last message; it closes right behind it.
        const code = closeCode(msg.code);
        this.socket.close(code === 1000 || (code >= 4000 && code <= 4999) ? code : 4000 + ErrorCode.INTERNAL_ERROR);
        this.onSocketClosed();
        return;
      }
      case "pong":
        return;
      default:
        throw new ConnError(ErrorCode.PROTOCOL_ERROR, `unexpected ${msg.t} from the agent`);
    }
  }

  private onSocketClosed(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.drainTimer !== undefined) clearTimeout(this.drainTimer);
    const err = new ConnError(ErrorCode.INTERNAL_ERROR, "fake edge: connection closed");
    for (const s of this.streams.values()) s.terminate(err);
    this.streams.clear();
    this.hooks.onClosed(this);
  }
}
