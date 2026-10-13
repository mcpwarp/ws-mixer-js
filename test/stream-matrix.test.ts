/**
 * Generated matrix over MixerStream's teardown seam (stream.ts write queue /
 * closeWrite / reset / terminate, conn.ts retireStream / rejectOutstanding /
 * outbox), driven through a real MixerConn over FakeWS -- the same transport
 * conn.test.ts uses -- so the conn-side paths (outbox, retireStream,
 * rejectOutstanding, the superseded-conn streamErrorFactory) are exercised,
 * not stubbed.
 *
 * Axes: state at trigger x trigger x read buffer x pending write.
 * Per cell: the write settles (resolved exactly when its bytes reached the
 * socket, else rejected with the class/code the oracle defines); `closed`
 * resolves with the expected reason; reading to the end never hangs,
 * delivers buffered data unless a RESET discarded it, and ends with a clean
 * null only after the peer's CLOSE; no DATA follows this stream's
 * CLOSE/RESET on the wire; no unhandled rejection.
 */
import { afterAll, describe, expect, it } from "vitest";
import { FakeWS } from "./helpers/fake-ws.js";
import { MixerConn } from "../src/conn.js";
import { encodeControl } from "../src/control.js";
import { decodeFrame, encodeClose, encodeData, encodeOpen, encodeWindow, FrameType } from "../src/frame.js";
import { ErrorCode, StreamError, WsMixerError, codeName } from "../src/errors.js";
import type { MixerStream, StreamCloseReason, StreamState } from "../src/stream.js";

const WINDOW = 16384; // hello/welcome minimum; also MAX_CHUNK, so a WINDOW+5 write leaves 5 bytes blocked on credit
const tick = () => new Promise<void>((r) => setImmediate(r));
const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const TIMEOUT = Symbol("timeout");
const within = <T,>(p: Promise<T>, ms = 500): Promise<T | typeof TIMEOUT> => Promise.race([p, delay(ms).then((): typeof TIMEOUT => TIMEOUT)]);

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

type StateAxis = "open" | "half_closed_remote" | "half_closed_local" | "closed(CLOSE+closeWrite)";
/** conn-drain-deadline: a superseded conn's drain deadline (fail() with a per-stream CANCEL). */
type Trigger = "conn-abnormal-1006" | "conn-clean-close" | "conn-drain-deadline" | "app-close" | "app-reset";
type BufferAxis = "buf-empty" | "buf-hello";
/**
 * none; just-issued: write() in the same tick as the trigger; credit-blocked:
 * WINDOW+5 bytes, the last 5 waiting on credit (a WINDOW arrives after the
 * trigger); send-failed: the write's socket send threw just before the
 * trigger; queued: a second stream's frame holds the writer (socket buffer
 * above the high-water mark until just after the trigger) and this stream's
 * chunk waits behind it in the connection's outbox.
 */
type WriteAxis = "no-write" | "just-issued" | "credit-blocked" | "send-failed" | "queued";

const STATES: StateAxis[] = ["open", "half_closed_remote", "half_closed_local", "closed(CLOSE+closeWrite)"];
const TRIGGERS: Trigger[] = ["conn-abnormal-1006", "conn-clean-close", "conn-drain-deadline", "app-close", "app-reset"];
const BUFFERS: BufferAxis[] = ["buf-empty", "buf-hello"];
const WRITES: WriteAxis[] = ["no-write", "just-issued", "credit-blocked", "send-failed", "queued"];

interface ExpectedError {
  cls: "ConnError" | "StreamError";
  code: number;
  /** Checked only where the message is fixed. */
  message?: string;
}

const CONN_ERROR: Record<"conn-abnormal-1006" | "conn-clean-close" | "conn-drain-deadline", ExpectedError> = {
  "conn-abnormal-1006": { cls: "ConnError", code: ErrorCode.INTERNAL_ERROR },
  "conn-clean-close": { cls: "ConnError", code: ErrorCode.NO_ERROR },
  "conn-drain-deadline": { cls: "StreamError", code: ErrorCode.CANCEL, message: "connection drained" },
};
const APP_RESET: ExpectedError = { cls: "StreamError", code: ErrorCode.CANCEL, message: "app reset" };
const APP_CLOSE_RESET: ExpectedError = { cls: "StreamError", code: ErrorCode.CANCEL, message: "local close: peer had not finished sending" };
const SEND_FAILED: ExpectedError = { cls: "ConnError", code: ErrorCode.INTERNAL_ERROR, message: "simulated socket failure" };

const localClosed = (s: StateAxis) => s === "half_closed_local" || s === "closed(CLOSE+closeWrite)";
const remoteClosedAxis = (s: StateAxis) => s === "half_closed_remote" || s === "closed(CLOSE+closeWrite)";

/** The state the setup must reach: closeWrite() waits for a pending write, so CLOSE is not sent yet when one is pending. */
function expectedSetupState(state: StateAxis, write: WriteAxis): StreamState {
  const pending = write === "just-issued" || write === "credit-blocked" || write === "queued";
  if (localClosed(state) && pending) return remoteClosedAxis(state) ? "half_closed_remote" : "open";
  return ({ open: "open", half_closed_remote: "half_closed_remote", half_closed_local: "half_closed_local", "closed(CLOSE+closeWrite)": "closed" } as const)[state];
}

/** What a RESET-type trigger does to a stream not already closed: app-reset always, app-close only while the peer is still sending. */
function resetByTrigger(trigger: Trigger, remoteClosed: boolean): ExpectedError | null {
  if (trigger === "app-reset") return APP_RESET;
  if (trigger === "app-close" && !remoteClosed) return APP_CLOSE_RESET;
  return null;
}

/** Whether a pending write's bytes still reach the socket after the trigger. */
function writeCompletes(trigger: Trigger, write: WriteAxis, remoteClosed: boolean): boolean {
  if (write === "no-write" || write === "send-failed") return false;
  if (resetByTrigger(trigger, remoteClosed)) return false;
  if (trigger === "conn-abnormal-1006" || trigger === "conn-drain-deadline") return false;
  if (trigger === "conn-clean-close") return write !== "credit-blocked"; // graceful: the writer runs during the grace period; credit only comes after
  return true; // app-close after the peer's CLOSE: closeWrite() waits for the write, the WINDOW unblocks credit
}

function expectedWrite(state: StateAxis, trigger: Trigger, write: WriteAxis): ExpectedError | "ok" | null {
  if (write === "no-write") return null;
  if (write === "send-failed") return SEND_FAILED;
  const remoteClosed = remoteClosedAxis(state);
  if (writeCompletes(trigger, write, remoteClosed)) return "ok";
  const reset = resetByTrigger(trigger, remoteClosed);
  if (reset) return reset;
  return CONN_ERROR[trigger as keyof typeof CONN_ERROR];
}

type ExpectedClosed = { kind: "finished" } | { kind: "reset"; err: ExpectedError } | { kind: "connection_lost"; err: ExpectedError };

function expectedClosed(state: StateAxis, trigger: Trigger, write: WriteAxis): ExpectedClosed {
  const setup = expectedSetupState(state, write);
  const remoteClosed = remoteClosedAxis(state);
  if (setup === "closed") return { kind: "finished" };
  const reset = resetByTrigger(trigger, remoteClosed);
  if (reset) return { kind: "reset", err: reset };
  if (trigger === "app-close") return { kind: "finished" }; // peer already CLOSE'd: close() is closeWrite()
  if (trigger === "app-reset") throw new Error("unreachable");
  if (trigger === "conn-drain-deadline") return { kind: "reset", err: CONN_ERROR[trigger] };
  // A graceful close lets a pending write and the closeWrite() behind it finish during the grace period.
  if (trigger === "conn-clean-close" && remoteClosed && localClosed(state) && writeCompletes(trigger, write, remoteClosed)) return { kind: "finished" };
  return { kind: "connection_lost", err: CONN_ERROR[trigger] };
}

/** How reading to the end must finish: null (clean EOF) or an error. */
function expectedReadEnd(state: StateAxis, trigger: Trigger, write: WriteAxis): ExpectedError | null {
  const closed = expectedClosed(state, trigger, write);
  const remoteClosed = remoteClosedAxis(state);
  if (closed.kind === "finished") return null;
  if (closed.kind === "reset" && trigger !== "conn-drain-deadline") return closed.err; // RESET discards, even after CLOSE
  return remoteClosed ? null : closed.err;
}

function bufferKept(state: StateAxis, trigger: Trigger, write: WriteAxis): boolean {
  const closed = expectedClosed(state, trigger, write);
  return !(closed.kind === "reset" && trigger !== "conn-drain-deadline");
}

function describeError(e: unknown): string {
  if (!e) return "null";
  if (e instanceof WsMixerError) return `${e.constructor.name}/${codeName(e.code)}(${JSON.stringify(e.message)})`;
  return `${(e as Error).constructor?.name ?? typeof e}(${JSON.stringify((e as Error).message)})`;
}

function describeExpected(w: ExpectedError | null | "ok"): string {
  if (w === "ok") return "ok";
  if (!w) return "null";
  return `${w.cls}/${codeName(w.code)}${w.message === undefined ? "" : `(${JSON.stringify(w.message)})`}`;
}

function matchesExpected(e: unknown, w: ExpectedError | null): boolean {
  if (!w) return !e;
  return e instanceof WsMixerError && e.constructor.name === w.cls && e.code === w.code && (w.message === undefined || e.message === w.message);
}

interface CellResult {
  cell: string;
  failures: string[];
  closed: string;
}
const results: CellResult[] = [];

async function runCell(state: StateAxis, trigger: Trigger, buffer: BufferAxis, write: WriteAxis): Promise<CellResult> {
  const cell = `${state} | ${trigger} | ${buffer} | ${write}`;
  const uncaught: unknown[] = [];
  const onUncaught = (e: unknown) => uncaught.push(e);
  process.on("uncaughtException", onUncaught);
  process.on("unhandledRejection", onUncaught);

  const failures: string[] = [];
  const ws = new FakeWS();
  let conn: MixerConn | undefined;
  let closedDesc = "?";
  try {
    conn = await handshaken(ws);
    conn.on("error", () => {});
    const streamP = new Promise<MixerStream>((resolve) => conn!.once("stream", resolve));
    ws.receive(encodeOpen(1));
    const stream = await streamP;

    let writeP: Promise<Error | null> | undefined;
    let payloadLen = 0;
    const issueWrite = (payload: Uint8Array) => {
      payloadLen = payload.length;
      writeP = stream.write(payload).then(
        () => null,
        (e: Error) => e,
      );
    };

    // --- build the state ---
    if (buffer === "buf-hello") ws.receive(encodeData(1, new TextEncoder().encode("hello")));
    if (remoteClosedAxis(state)) ws.receive(encodeClose(1));
    if (write === "credit-blocked") {
      issueWrite(new Uint8Array(WINDOW + 5).fill(0x57));
      await tick();
      await tick();
    } else if (write === "send-failed") {
      ws.failNextSend(new Error("simulated socket failure"));
      issueWrite(new TextEncoder().encode("WRITE"));
      await tick();
      await tick();
    } else if (write === "queued") {
      const otherP = new Promise<MixerStream>((resolve) => conn!.once("stream", resolve));
      ws.receive(encodeOpen(3));
      const other = await otherP;
      ws.bufferedAmount = 2 * 1024 * 1024;
      void other.write("OTHER").catch(() => {});
      await tick();
      issueWrite(new TextEncoder().encode("WRITE"));
      await tick();
      await tick();
    }
    // just-issued: write, (closeWrite), trigger -- all in one synchronous run.
    if (write === "just-issued") issueWrite(new TextEncoder().encode("WRITE"));
    if (localClosed(state)) {
      void stream.closeWrite();
      if (write !== "just-issued") await tick();
    }
    const stateBefore = stream.getState();
    const wantSetup = expectedSetupState(state, write);
    if (stateBefore !== wantSetup) failures.push(`setup: state ${stateBefore}, wanted ${wantSetup}`);

    // --- trigger ---
    let cleanCloseP: Promise<void> | undefined;
    try {
      switch (trigger) {
        case "conn-abnormal-1006":
          ws.serverClose(1006);
          break;
        case "conn-clean-close":
          cleanCloseP = conn.close(20);
          break;
        case "conn-drain-deadline":
          conn.fail(new WsMixerError(ErrorCode.NO_ERROR, "superseded: drain deadline passed"), (id) => new StreamError(ErrorCode.CANCEL, id, "connection drained"));
          break;
        case "app-close":
          stream.close();
          break;
        case "app-reset":
          stream.reset("CANCEL", "app reset");
          break;
      }
    } catch (e) {
      failures.push(`trigger threw synchronously: ${(e as Error)?.constructor?.name}`);
    }
    if (write === "queued") ws.bufferedAmount = 0;
    if (cleanCloseP && (await within(cleanCloseP, 1000)) === TIMEOUT) failures.push("conn.close() never resolved");
    await delay(15);
    if (write === "credit-blocked") ws.receive(encodeWindow(1, 1000));

    // --- read to the end ---
    const received: string[] = [];
    let readEnd: Error | null | typeof TIMEOUT = TIMEOUT;
    for (let i = 0; i < 10; i++) {
      const r = await within(
        stream.read().then(
          (c) => ({ c }),
          (e: Error) => ({ e }),
        ),
      );
      if (r === TIMEOUT) break;
      if ("e" in r) {
        readEnd = r.e;
        break;
      }
      if (r.c === null) {
        readEnd = null;
        break;
      }
      received.push(new TextDecoder().decode(r.c));
    }

    const closedReason = await within(stream.closed);
    const writeResult = writeP ? await within(writeP) : null;
    await delay(20); // let late failures surface

    // --- assertions ---
    if (uncaught.length > 0) failures.push(`unhandled: ${uncaught.map((e) => describeError(e)).join(",")}`);

    const wantRead = expectedReadEnd(state, trigger, write);
    if (readEnd === TIMEOUT) failures.push("read never finished");
    else if (!matchesExpected(readEnd, wantRead)) failures.push(`read ended ${describeError(readEnd)}, wanted ${describeExpected(wantRead)}`);
    const wantData = buffer === "buf-hello" && bufferKept(state, trigger, write) ? "hello" : "";
    if (received.join("") !== wantData) failures.push(`read data ${JSON.stringify(received.join(""))}, wanted ${JSON.stringify(wantData)}`);

    const wantClosed = expectedClosed(state, trigger, write);
    if (closedReason === TIMEOUT) {
      closedDesc = "TIMEOUT";
      failures.push("closed never resolved");
    } else {
      const c = closedReason as StreamCloseReason;
      closedDesc = c.kind === "finished" ? "finished" : c.kind === "reset" ? `reset/${c.name}(${JSON.stringify(c.message)})${c.byPeer ? "/peer" : ""}` : `connection_lost/${describeError(c.error)}`;
      if (c.kind !== wantClosed.kind) failures.push(`closed ${closedDesc}, wanted ${wantClosed.kind}`);
      else if (c.kind === "reset" && wantClosed.kind === "reset") {
        if (c.code !== wantClosed.err.code || c.byPeer || (wantClosed.err.message !== undefined && c.message !== wantClosed.err.message)) {
          failures.push(`closed ${closedDesc}, wanted reset ${describeExpected(wantClosed.err)}`);
        }
      } else if (c.kind === "connection_lost" && wantClosed.kind === "connection_lost" && !matchesExpected(c.error, wantClosed.err)) {
        failures.push(`closed ${closedDesc}, wanted connection_lost ${describeExpected(wantClosed.err)}`);
      }
    }

    // Bytes of this stream's DATA that reached the socket (a send that threw is recorded by FakeWS but never went out).
    let dataBytes = 0;
    let endSeen = false;
    let dataAfterEnd = false;
    let skipFailedSend = write === "send-failed";
    for (const raw of ws.sent) {
      let fr;
      try {
        fr = decodeFrame(raw);
      } catch {
        continue;
      }
      if (fr.streamId !== 1) continue;
      if (fr.type === FrameType.DATA) {
        if (skipFailedSend) {
          skipFailedSend = false;
          continue;
        }
        dataBytes += fr.payload.length;
        if (endSeen) dataAfterEnd = true;
      } else if (fr.type === FrameType.CLOSE || fr.type === FrameType.RESET) {
        endSeen = true;
      }
    }
    if (dataAfterEnd) failures.push("wire: DATA sent after this stream's CLOSE/RESET");

    const wantWrite = expectedWrite(state, trigger, write);
    if (write !== "no-write") {
      if (writeResult === TIMEOUT) failures.push("write never settled");
      else {
        const fullyOnWire = dataBytes === payloadLen;
        if (writeResult === null && !fullyOnWire) failures.push("write resolved but bytes not on wire");
        if (writeResult !== null && fullyOnWire && write !== "send-failed") failures.push("write rejected but bytes fully on wire");
        if (wantWrite === "ok") {
          if (writeResult !== null) failures.push(`write ${describeError(writeResult)}, wanted ok`);
        } else if (!matchesExpected(writeResult, wantWrite)) {
          failures.push(`write ${describeError(writeResult)}, wanted ${describeExpected(wantWrite)}`);
        }
      }
    }
    return { cell, failures, closed: closedDesc };
  } finally {
    process.off("uncaughtException", onUncaught);
    process.off("unhandledRejection", onUncaught);
    try {
      conn?.fail(new WsMixerError(ErrorCode.NO_ERROR, "matrix cleanup"));
    } catch {
      // already closed
    }
  }
}

describe.each(STATES)("state=%s", (state) => {
  describe.each(TRIGGERS)("trigger=%s", (trigger) => {
    const rows: Array<[BufferAxis, WriteAxis]> = [];
    for (const b of BUFFERS) for (const w of WRITES) rows.push([b, w]);
    it.each(rows)(
      "%s %s",
      async (buffer, write) => {
        const r = await runCell(state, trigger, buffer, write);
        results.push(r);
        expect(r.failures, `${r.cell} (closed=${r.closed})`).toEqual([]);
      },
      5000,
    );
  });
});

afterAll(() => {
  const failing = results.filter((r) => r.failures.length > 0);
  const byMode = new Map<string, string[]>();
  for (const r of failing) {
    const key = r.failures.join(" + ");
    const list = byMode.get(key) ?? [];
    list.push(`${r.cell}  [closed=${r.closed}]`);
    byMode.set(key, list);
  }
  const closedClasses = new Map<string, number>();
  for (const r of results) closedClasses.set(r.closed, (closedClasses.get(r.closed) ?? 0) + 1);
  const lines: string[] = [];
  lines.push(`MATRIX: ${results.length} cells, ${failing.length} failing`);
  lines.push(`closed reasons: ${[...closedClasses].map(([k, v]) => `${k}=${v}`).join(", ")}`);
  for (const [mode, cells] of [...byMode].sort((a, b) => b[1].length - a[1].length)) {
    lines.push(`\n## ${mode}  (${cells.length})`);
    for (const c of cells) lines.push(`   ${c}`);
  }
  console.log(lines.join("\n"));
});
