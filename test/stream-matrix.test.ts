/**
 * Generated matrix over MixerStream's teardown seam (stream.ts drainWriteQueue /
 * terminateNoThrow / _write / _destroy / closeWrite / applyReset /
 * heldWriteCallbacks, conn.ts retireStream / rejectOutstanding), driven through a
 * real MixerConn over FakeWS -- the same transport conn.test.ts uses -- so the
 * conn-side paths (outbox, retireStream, rejectOutstanding, drain hand-over's
 * streamErrorFactory) are exercised, not stubbed.
 *
 * Axes: state at trigger x trigger x 'error' listener x read buffer x pending write,
 * plus, for the states the peer already CLOSE'd, a reader axis (a reader attached
 * right after the trigger, or none ever).
 * Per cell: every write callback settles exactly once (Error unless the bytes
 * genuinely reached the wire), buffered data + 'end' before 'close' on a clean
 * read-side end, 'close' exactly once and last, no unhandled error, destroyed,
 * `stream.errored` and any write-callback error of the class, code (and, where
 * fixed, message) expectedErrored() defines. With no reader and unread data, a
 * connection trigger must leave the stream up for a reader (no 'close', no
 * write-callback error yet); the cell then destroy()s it and checks the rest.
 */
import { afterAll, describe, expect, it } from "vitest";
import { FakeWS } from "./helpers/fake-ws.js";
import { MixerConn } from "../src/conn.js";
import { encodeControl, type ControlMessage } from "../src/control.js";
import { decodeFrame, encodeClose, encodeData, encodeOpen, FrameType } from "../src/frame.js";
import { ErrorCode, StreamError, WsMixerError, codeName } from "../src/errors.js";
import type { MixerStream, StreamState } from "../src/stream.js";

const WINDOW = 16384; // hello/welcome minimum; also MAX_CHUNK, so a WINDOW+5 write leaves 5 bytes blocked on credit
const tick = () => new Promise<void>((r) => setImmediate(r));
const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function handshaken(ws: FakeWS) {
  const conn = new MixerConn(ws, { token: "t", agent: { sdk: "ws-mixer-js-test", sdk_version: "0.0.0" }, window: WINDOW });
  const handshakePromise = conn.handshake();
  await tick();
  ws.receive(
    encodeData(
      0,
      encodeControl({
        t: "welcome",
        v: 1,
        session: "01TEST",
        window: WINDOW,
        max_streams: 64,
        ping_interval: 30000,
        ping_timeout: 90000,
      } as never),
    ),
  );
  await handshakePromise;
  return conn;
}

function drainFrame(lastStreamId = 0): Uint8Array {
  return encodeData(0, encodeControl({ t: "drain", reason: "rollout", last_stream_id: lastStreamId } as ControlMessage));
}

type StateAxis = "open" | "half_closed_remote" | "half_closed_local" | "closed(CLOSE+closeWrite)";
type Trigger = "conn-abnormal-1006" | "conn-clean-close" | "drain-handover" | "app-destroy" | "app-reset";
type Listener = "listener" | "no-listener";
type BufferAxis = "buf-empty" | "buf-hello";
/**
 * none; just-issued: write() in the same tick as the trigger (still between _write and
 * conn.sendData / in the outbox); credit-blocked: WINDOW+5 bytes, last 5 parked in
 * reserveSendCredit; send-failed: the write's socket send failed (FakeWS.failNextSend)
 * just before the trigger, as when a dying socket's write errors before its 'close';
 * queued: a second stream's write is in flight (its socket send held until just after
 * the trigger) and this stream's chunk waits behind it in the connection's outbox.
 */
type WriteAxis = "no-write" | "just-issued" | "credit-blocked" | "send-failed" | "queued";
type ReaderAxis = "reader" | "no-reader";

const STATES: StateAxis[] = ["open", "half_closed_remote", "half_closed_local", "closed(CLOSE+closeWrite)"];
const TRIGGERS: Trigger[] = ["conn-abnormal-1006", "conn-clean-close", "drain-handover", "app-destroy", "app-reset"];
const LISTENERS: Listener[] = ["listener", "no-listener"];
const BUFFERS: BufferAxis[] = ["buf-empty", "buf-hello"];
const WRITES: WriteAxis[] = ["no-write", "just-issued", "credit-blocked", "send-failed", "queued"];
/** A stream still open for reading when the trigger hits always has a reader in these cells; only a peer-CLOSE'd read side gets the no-reader variant. */
const READERS = (state: StateAxis): ReaderAxis[] =>
  state === "half_closed_remote" || state === "closed(CLOSE+closeWrite)" ? ["reader", "no-reader"] : ["reader"];

const EXPECTED_STATE: Record<StateAxis, StreamState> = {
  open: "open",
  half_closed_remote: "half_closed_remote",
  half_closed_local: "half_closed_local",
  "closed(CLOSE+closeWrite)": "closed",
};

/** Combos that cannot be constructed, with the reason. Checked per cell; none of the axes' products currently need one (see note at the bottom of the file). */
function impossible(_state: StateAxis, _trigger: Trigger, _listener: Listener, _buffer: BufferAxis, _write: WriteAxis, _reader: ReaderAxis): string | null {
  return null;
}

interface ExpectedError {
  cls: "ConnError" | "StreamError";
  code: number;
  /** Checked only where the message is fixed. */
  message?: string;
}

const TRIGGER_ERROR: Record<Trigger, ExpectedError> = {
  "conn-abnormal-1006": { cls: "ConnError", code: ErrorCode.INTERNAL_ERROR },
  "conn-clean-close": { cls: "ConnError", code: ErrorCode.NO_ERROR },
  "drain-handover": { cls: "StreamError", code: ErrorCode.CANCEL, message: "connection drained" },
  "app-destroy": { cls: "StreamError", code: ErrorCode.CANCEL, message: "local destroy" },
  "app-reset": { cls: "StreamError", code: ErrorCode.CANCEL, message: "app reset" },
};
const SEND_FAILED: ExpectedError = { cls: "ConnError", code: ErrorCode.INTERNAL_ERROR, message: "simulated socket failure" };
const STREAM_CLOSED: ExpectedError = { cls: "StreamError", code: ErrorCode.STREAM_CLOSED };

/**
 * The `stream.errored` a cell must end with (null, or class + code), which is also what a
 * write callback that got an error must have seen. Never a bare WsMixerError or Error:
 * whatever failed the stream first decides it --
 *  - connection death (1006 -> ConnError(INTERNAL_ERROR), clean close -> ConnError(NO_ERROR));
 *    drain hand-over -> StreamError(CANCEL, "connection drained"); app reset() ->
 *    StreamError(CANCEL); app destroy() -> null (Node's plain destroy()), or
 *    StreamError(CANCEL, "local destroy") once a pending write fails with it;
 *  - clean read-side end (peer CLOSE before a connection trigger, nothing pending) -> null; a
 *    retired closed(CLOSE+closeWrite) stream is reached by a connection trigger only to be
 *    finished, never failed with the trigger's error;
 *  - a failure before the trigger wins: a failed socket send -> ConnError(INTERNAL_ERROR) (held
 *    past a peer CLOSE on a stream still live, it is flushed with the trigger's own error
 *    instead, if one comes), a write closeWrite() cut off -> StreamError(STREAM_CLOSED).
 *    conn.close() is graceful (drain + grace period), so a just-issued or queued write settles
 *    on its own first: sent (then as no-write), or cut off by closeWrite(); and a reader
 *    reaches 'end' first, flushing a held send failure with its own error.
 * A queued write fails the same way as a just-issued one: with its own stream's error, never
 * the connection's where the two differ (drain hand-over).
 */
function expectedErrored(state: StateAxis, trigger: Trigger, write: WriteAxis, reader: ReaderAxis): ExpectedError | null {
  const retired = state === "closed(CLOSE+closeWrite)";
  const remoteClosed = state === "half_closed_remote" || retired;
  if (write === "queued") write = "just-issued";
  if (write === "send-failed") {
    if (!remoteClosed || retired) return SEND_FAILED;
    return trigger === "conn-clean-close" && reader === "reader" ? SEND_FAILED : TRIGGER_ERROR[trigger];
  }
  if (write === "credit-blocked" && (state === "half_closed_local" || retired)) return STREAM_CLOSED;
  if (write === "just-issued" && (retired || (state === "half_closed_local" && trigger === "conn-clean-close"))) return STREAM_CLOSED;
  if (write === "just-issued" && trigger === "conn-clean-close") write = "no-write";
  switch (trigger) {
    case "conn-abnormal-1006":
    case "conn-clean-close":
    case "drain-handover":
      return retired || (remoteClosed && write === "no-write") ? null : TRIGGER_ERROR[trigger];
    case "app-destroy":
      return write === "no-write" ? null : TRIGGER_ERROR[trigger];
    case "app-reset":
      return retired ? null : TRIGGER_ERROR[trigger];
  }
}

function describeError(e: unknown): string {
  if (!e) return "null";
  if (e instanceof WsMixerError) return `${e.constructor.name}/${codeName(e.code)}(${JSON.stringify(e.message)})`;
  return `${(e as Error).constructor?.name ?? typeof e}(${JSON.stringify((e as Error).message)})`;
}

function describeExpected(w: ExpectedError | null): string {
  if (!w) return "null";
  return `${w.cls}/${codeName(w.code)}${w.message === undefined ? "" : `(${JSON.stringify(w.message)})`}`;
}

function matchesExpected(e: unknown, w: ExpectedError | null): boolean {
  if (!w) return !e;
  return (
    e instanceof WsMixerError &&
    e.constructor.name === w.cls &&
    e.code === w.code &&
    (w.message === undefined || e.message === w.message)
  );
}

interface CellResult {
  cell: string;
  failures: string[];
  errored: string;
  stateBefore: string;
}
const results: CellResult[] = [];

function cellName(state: StateAxis, trigger: Trigger, listener: Listener, buffer: BufferAxis, write: WriteAxis, reader: ReaderAxis): string {
  return `${state} | ${trigger} | ${listener} | ${buffer} | ${write} | ${reader}`;
}

async function runCell(
  state: StateAxis,
  trigger: Trigger,
  listener: Listener,
  buffer: BufferAxis,
  write: WriteAxis,
  reader: ReaderAxis,
): Promise<CellResult> {
  const cell = cellName(state, trigger, listener, buffer, write, reader);
  const uncaught: unknown[] = [];
  const onUncaught = (e: unknown) => uncaught.push(e);
  process.on("uncaughtException", onUncaught);
  process.on("unhandledRejection", onUncaught);

  const failures: string[] = [];
  const ws = new FakeWS();
  let conn: MixerConn | undefined;
  let s: MixerStream | undefined;
  try {
    conn = await handshaken(ws);
    conn.on("error", () => {});
    const streamP = new Promise<MixerStream>((resolve) => conn!.once("stream", resolve));
    ws.receive(encodeOpen(1));
    s = await streamP;
    const stream = s;

    // Record every data/end/error/close emission without adding a listener (so the
    // no-listener cells stay genuinely listener-less).
    const log: string[] = [];
    const received: Buffer[] = [];
    const origEmit = stream.emit.bind(stream);
    stream.emit = ((ev: string | symbol, ...args: unknown[]) => {
      if (ev === "data") log.push("data");
      else if (ev === "end" || ev === "close") log.push(ev);
      else if (ev === "error") {
        const e = args[0] as { constructor?: { name?: string }; code?: unknown } | undefined;
        log.push(`error:${e?.constructor?.name ?? "?"}${typeof e?.code === "string" ? `/${e.code}` : ""}`);
      }
      return origEmit(ev as never, ...(args as never[]));
    }) as typeof stream.emit;
    if (listener === "listener") stream.on("error", () => {});

    const cbCalls: Array<Error | null | undefined> = [];
    const cb = (e?: Error | null) => {
      cbCalls.push(e);
      log.push(e ? `write-cb:${e.constructor.name}` : "write-cb:ok");
    };
    let payloadLen = 0;
    let releaseHeld: (() => void) | undefined;
    const issueWrite = (payload: Buffer) => {
      payloadLen = payload.length;
      stream.write(payload, cb);
    };

    // --- build the state ---
    if (buffer === "buf-hello") ws.receive(encodeData(1, Buffer.from("hello")));
    const remoteClosed = state === "half_closed_remote" || state === "closed(CLOSE+closeWrite)";
    const localClosed = state === "half_closed_local" || state === "closed(CLOSE+closeWrite)";
    if (remoteClosed) ws.receive(encodeClose(1));
    if (write === "credit-blocked") {
      issueWrite(Buffer.alloc(WINDOW + 5, 0x57));
      await tick();
      await tick();
    } else if (write === "send-failed") {
      ws.failNextSend(new Error("simulated socket failure"));
      issueWrite(Buffer.from("WRITE"));
      await tick();
      await tick();
    } else if (write === "queued") {
      const otherP = new Promise<MixerStream>((resolve) => conn!.once("stream", resolve));
      ws.receive(encodeOpen(3));
      const other = await otherP;
      other.on("error", () => {});
      const realSend = ws.send.bind(ws);
      const held: Array<() => void> = [];
      ws.send = (data: Uint8Array, sendCb?: (err?: Error) => void) => {
        ws.sent.push(data.slice());
        held.push(() => sendCb?.());
      };
      releaseHeld = () => {
        ws.send = realSend;
        for (const release of held.splice(0)) release();
      };
      other.write(Buffer.from("OTHER"));
      await tick();
      issueWrite(Buffer.from("WRITE"));
      await tick();
      await tick();
    }
    // just-issued: write, (closeWrite), trigger -- all in one synchronous run.
    if (write === "just-issued") issueWrite(Buffer.from("WRITE"));
    if (localClosed) {
      stream.closeWrite();
      if (write !== "just-issued") await tick();
    }
    const stateBefore = stream.getState();
    if (stateBefore !== EXPECTED_STATE[state]) failures.push(`setup: state ${stateBefore}, wanted ${EXPECTED_STATE[state]}`);

    // --- trigger ---
    let cleanCloseP: Promise<void> | undefined;
    try {
      switch (trigger) {
        case "conn-abnormal-1006":
          ws.emit("close", 1006, Buffer.from(""));
          break;
        case "conn-clean-close":
          cleanCloseP = conn.close(20);
          break;
        case "drain-handover":
          ws.receive(drainFrame(1));
          conn.fail(
            new WsMixerError(ErrorCode.NO_ERROR, "superseded by a new connection"),
            (streamId) => new StreamError(ErrorCode.CANCEL, streamId, "connection drained"),
          );
          break;
        case "app-destroy":
          stream.destroy();
          break;
        case "app-reset":
          stream.reset("CANCEL", "app reset");
          break;
      }
    } catch (e) {
      failures.push(`trigger threw synchronously: ${(e as Error)?.constructor?.name}`);
    }
    releaseHeld?.();

    const connTrigger = trigger === "conn-abnormal-1006" || trigger === "conn-clean-close" || trigger === "drain-handover";
    if (reader === "reader") {
      // Reader attached only now, so buf-hello is genuinely unread at the trigger.
      stream.on("data", (c: Buffer) => received.push(c));
    } else if (buffer === "buf-hello" && connTrigger) {
      // Unread data outlives the connection: the stream must stay up for a reader that may
      // still come, holding any write error that would otherwise error its read side.
      if (cleanCloseP) await Promise.race([cleanCloseP.catch(() => {}), delay(200)]);
      await delay(30);
      if (stream.destroyed || log.includes("close")) failures.push("no reader: destroyed with its data still unread");
      if (cbCalls.some((e) => e)) failures.push("no reader: write-cb error before 'end' (errors the read side)");
      stream.destroy();
    }

    for (let i = 0; i < 40 && !log.includes("close"); i++) await delay(10);
    if (cleanCloseP) await Promise.race([cleanCloseP.catch(() => {}), delay(200)]);
    await delay(30); // let late / duplicate callbacks surface

    // --- assertions ---
    if (uncaught.length > 0) {
      failures.push(`unhandled error: ${uncaught.map((e) => (e as Error)?.constructor?.name ?? String(e)).join(",")}`);
    }

    // Bytes of this stream's DATA that reached the wire (a failNextSend'd frame is recorded by FakeWS but never flushed).
    let dataBytes = 0;
    let closedOrResetSeen = false;
    let dataAfterCloseOrReset = false;
    const frames = ws.sent.map((f) => {
      try {
        return decodeFrame(f);
      } catch {
        return null;
      }
    });
    const wantErrored = expectedErrored(state, trigger, write, reader);
    let failedSendSkipped = write !== "send-failed";
    for (const fr of frames) {
      if (!fr || fr.streamId !== 1) continue;
      if (fr.type === FrameType.DATA) {
        if (!failedSendSkipped) {
          failedSendSkipped = true;
          continue;
        }
        dataBytes += fr.payload.length;
        if (closedOrResetSeen) dataAfterCloseOrReset = true;
      } else if (fr.type === FrameType.CLOSE || fr.type === FrameType.RESET) {
        closedOrResetSeen = true;
      }
    }
    if (dataAfterCloseOrReset) failures.push("wire: DATA sent after this stream's CLOSE/RESET");

    if (write !== "no-write") {
      if (cbCalls.length === 0) failures.push("write-cb never settled");
      else if (cbCalls.length > 1) failures.push(`write-cb settled ${cbCalls.length}x`);
      else {
        const fullyOnWire = write !== "send-failed" && dataBytes === payloadLen;
        const v = cbCalls[0];
        if (!v && !fullyOnWire) failures.push("write-cb success but bytes not on wire");
        if (v && fullyOnWire) failures.push("write-cb Error but bytes fully on wire");
        if (v && !matchesExpected(v, wantErrored)) failures.push(`write-cb ${describeError(v)}, wanted ${describeExpected(wantErrored)}`);
      }
    }
    if (log.some((e) => e.includes("ERR_MULTIPLE_CALLBACK"))) failures.push("ERR_MULTIPLE_CALLBACK emitted");

    const closeCount = log.filter((e) => e === "close").length;
    const closeIdx = log.indexOf("close");
    if (closeCount === 0) failures.push("'close' never fired");
    else if (closeCount > 1) failures.push(`'close' fired ${closeCount}x`);
    else if (closeIdx !== log.length - 1) failures.push(`'close' not last (then: ${log.slice(closeIdx + 1).join(",")})`);

    if (reader === "reader" && buffer === "buf-hello" && remoteClosed && connTrigger) {
      const got = Buffer.concat(received).toString();
      if (got !== "hello") failures.push(`buffered data lost (got ${JSON.stringify(got)})`);
      const endIdx = log.indexOf("end");
      if (endIdx < 0) failures.push("'end' never fired on clean read-side end");
      else if (closeIdx >= 0 && endIdx > closeIdx) failures.push("'end' after 'close'");
    }

    if (!stream.destroyed) failures.push("destroyed=false");

    const errored = describeError(stream.errored);
    if (!matchesExpected(stream.errored, wantErrored)) failures.push(`errored ${errored}, wanted ${describeExpected(wantErrored)}`);
    return { cell, failures, errored, stateBefore };
  } finally {
    process.off("uncaughtException", onUncaught);
    process.off("unhandledRejection", onUncaught);
    // Cleanup outside the guard window: stop the conn's keepalive timers for cells whose trigger left it up.
    if (s && !s.destroyed && s.listenerCount("error") === 0) s.on("error", () => {});
    try {
      conn?.fail(new WsMixerError(ErrorCode.NO_ERROR, "matrix cleanup"));
    } catch {
      // already closed
    }
  }
}

describe.each(STATES)("state=%s", (state) => {
  describe.each(TRIGGERS)("trigger=%s", (trigger) => {
    const rows: Array<[Listener, BufferAxis, WriteAxis, ReaderAxis]> = [];
    for (const l of LISTENERS) for (const b of BUFFERS) for (const w of WRITES) for (const r of READERS(state)) rows.push([l, b, w, r]);
    it.each(rows)(
      "%s %s %s %s",
      async (listener, buffer, write, reader) => {
        const why = impossible(state, trigger, listener, buffer, write, reader);
        if (why) {
          results.push({ cell: cellName(state, trigger, listener, buffer, write, reader), failures: [], errored: `SKIPPED: ${why}`, stateBefore: "-" });
          return;
        }
        const r = await runCell(state, trigger, listener, buffer, write, reader);
        results.push(r);
        expect(r.failures, `${r.cell} (errored=${r.errored})`).toEqual([]);
      },
      1500,
    );
  });
});

afterAll(() => {
  const failing = results.filter((r) => r.failures.length > 0);
  const byMode = new Map<string, string[]>();
  for (const r of failing) {
    const key = r.failures.join(" + ");
    const list = byMode.get(key) ?? [];
    list.push(`${r.cell}  [errored=${r.errored}]`);
    byMode.set(key, list);
  }
  const erroredClasses = new Map<string, number>();
  for (const r of results) erroredClasses.set(r.errored, (erroredClasses.get(r.errored) ?? 0) + 1);
  const lines: string[] = [];
  lines.push(`MATRIX: ${results.length} cells, ${failing.length} failing, ${results.filter((r) => r.errored.startsWith("SKIPPED")).length} skipped`);
  lines.push(`errored classes: ${[...erroredClasses].map(([k, v]) => `${k}=${v}`).join(", ")}`);
  for (const [mode, cells] of [...byMode].sort((a, b) => b[1].length - a[1].length)) {
    lines.push(`\n## ${mode}  (${cells.length})`);
    for (const c of cells) lines.push(`   ${c}`);
  }
  console.log(lines.join("\n"));
});

// No impossible combos: every state admits a buffered-but-unread read side (DATA may
// precede the peer's CLOSE, and our own closeWrite() doesn't touch the read side), and
// every pending-write mode is issued while the write side is still open (before the
// state's closeWrite(), in the same tick for just-issued).
