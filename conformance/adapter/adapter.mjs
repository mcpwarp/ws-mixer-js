#!/usr/bin/env node
// ws-mixer conformance runner's JS SDK adapter (docs/CONFORMANCE.md section
// 1): a thin shell over @mcpwarp/ws-mixer's public API (client.ts's
// connect()/MixerClient, conn.ts's MixerConn, stream.ts's MixerStream) plus
// the one test-only timing hook (ConnectOptions._timing, forwarded to
// ConnOptions._timing -- this task's required SDK change). It reads
// JSON-lines commands on stdin and writes JSON-lines events on stdout, with
// no protocol logic of its own. Client role only: the JS SDK has no server
// (conformance/README.md documents `listen`/`open_stream` replying
// `unsupported`, which the runner treats as a role-inapplicable SKIP, not a
// failure).
//
// Imports the package's built dist/index.js, not src/*.ts directly: a plain
// `node` process (no loader) cannot resolve a bare `.ts` import.
// conformance/README.md documents this as a deliberate deviation from
// docs/CONFORMANCE.md section 5's "no build step; imports ../../src"
// -- the runner's --adapter shim (or the generic buildGenericAdapter
// contract, post-split) runs `npm run build` at the repo root before
// spawning this file, so dist is never stale.
import { connect, SDK_VERSION } from "../../dist/index.js";
import { fileURLToPath } from "node:url";
import fs from "node:fs";

// startTime anchors t_ms (docs/CONFORMANCE.md section 1): milliseconds since
// this adapter process started, monotonic, stamped on every data,
// stream_closed, and stream_reset event -- mirrors the Go adapter.
const startTime = process.hrtime.bigint();
function tMs() {
  return Number((process.hrtime.bigint() - startTime) / 1000000n);
}

// emitSink is where every emit()'d event actually goes -- process.stdout by
// default (the real adapter's wire format), overridable via setEmitSink so
// adapter.test.mjs can capture emitted events directly as objects. Tests
// deliberately don't intercept process.stdout itself: it's also where
// node:test's own TAP reporter writes, and hijacking it collides with that.
function defaultEmitSink(e) {
  process.stdout.write(JSON.stringify(e) + "\n");
}
let emitSink = defaultEmitSink;
function setEmitSink(fn) {
  emitSink = typeof fn === "function" ? fn : defaultEmitSink;
}

function emit(e) {
  emitSink(e);
}
function ack(seq) {
  emit({ event: "ack", seq });
}
function cmdErr(seq, message) {
  emit({ event: "error", seq, message });
}
function adapterErr(message) {
  emit({ event: "error", message });
}

const state = {
  window: 262144,
  maxStreams: 64,
  pingIntervalMs: 30000,
  pingTimeoutMs: 90000,
  helloTimeoutMs: 10000,
  timeScale: 1,
  floorMs: 300,
  allowSubfloorTiming: false,
  client: null,
  streams: new Map(),
  lastWelcome: null,
  // streamHalfClosed tracks, per stream id, which half of a graceful close
  // has happened locally: read (this side saw 'end') and write (this side
  // called closeWrite()). Once both are true the adapter emits an extra
  // stream_closed{direction:"both"} alongside the existing per-direction
  // event (docs/CONFORMANCE.md section 1.2), mirroring the Go adapter.
  streamHalfClosed: new Map(),
  // streamQueues serializes write/close_write per stream id (mirrors
  // conformance/adapters/go/main.go's streamWorkers): `write` acks
  // asynchronously (stream.write()'s callback) while `close_write` used to
  // run synchronously as soon as their command line was read, so a
  // close_write issued before an in-flight write's callback fired could
  // race ahead of it on the wire, truncating the stream. Each stream id
  // gets a queue entry here -- {items, busy, aborted} -- and
  // write/close_write append their work to it instead of running
  // immediately, so commands for one stream take effect in the order the
  // runner sent them regardless of ack timing. Bounded (STREAM_QUEUE_CAP):
  // a stalled queue (e.g. a write stuck on exhausted send credit) must not
  // wedge the stdin-reading loop, so enqueueStreamOp never lets `items`
  // grow past the cap -- it error-acks the new command with "queue_full"
  // instead.
  //
  // RESET (WIRE.md section 2.5: abortive, discards buffered data and
  // unblocks writers) deliberately does NOT go through this queue --
  // resetStream below marks it aborted and drains any operations still
  // sitting in `items` with an error ack, then calls stream.reset()
  // directly. stream.reset() itself destroys the MixerStream, which
  // unblocks a write already in flight on the queue by having Node's
  // Writable machinery invoke its pending write() callback with the
  // terminal error, rather than adapter.mjs waiting behind it.
  streamQueues: new Map(),
};

// STREAM_QUEUE_CAP bounds each stream's FIFO queue: how many write/
// close_write commands may be queued ahead of a stalled queue before
// enqueueStreamOp starts rejecting new ones with a "queue_full" error ack
// instead of blocking the stdin-reading loop.
const STREAM_QUEUE_CAP = 64;

// getOrCreateQueue returns stream id's FIFO queue, creating it lazily on
// first use (see the streamQueues field comment).
function getOrCreateQueue(id) {
  let q = state.streamQueues.get(id);
  if (!q) {
    q = { items: [], busy: false, aborted: false };
    state.streamQueues.set(id, q);
  }
  return q;
}

// pumpStreamQueue runs the next queued item for id, if any and if nothing
// is already running. op may return a promise (write) or nothing
// (close_write); either way the next queued op for this id waits for it.
function pumpStreamQueue(id, q) {
  if (q.busy) return;
  const item = q.items.shift();
  if (!item) return;
  // aborted is set by resetStream once it has drained q.items -- checked
  // here too (not just relied on as a signal drainAbortedOps already acted
  // on) so any item that still reaches the pump after abort (e.g. one
  // pushed onto this exact queue object between the drain and teardown)
  // gets a clean error ack instead of running against an already-reset
  // stream.
  if (q.aborted) {
    cmdErr(item.seq, "reset: queued command aborted by RESET");
    pumpStreamQueue(id, q);
    return;
  }
  q.busy = true;
  Promise.resolve()
    .then(() => item.op())
    .catch((err) => {
      adapterErr("stream op failed: " + (err && err.message ? err.message : String(err)));
    })
    .finally(() => {
      q.busy = false;
      pumpStreamQueue(id, q);
    });
}

// enqueueStreamOp queues op on stream id's own FIFO queue (write and
// close_write only -- see the streamQueues field comment; reset bypasses
// this entirely via resetStream). The queue is bounded (STREAM_QUEUE_CAP):
// if it has filled up behind a stalled op, enqueueStreamOp does not block
// the stdin-reading loop waiting for room -- it error-acks seq with
// "queue_full" instead.
function enqueueStreamOp(id, seq, op) {
  const q = getOrCreateQueue(id);
  if (q.items.length >= STREAM_QUEUE_CAP) {
    cmdErr(seq, `queue_full: stream ${id}'s write/close_write queue (cap ${STREAM_QUEUE_CAP}) is full`);
    return;
  }
  q.items.push({ seq, op });
  pumpStreamQueue(id, q);
}

// teardownStream removes id's bookkeeping (streams, streamHalfClosed,
// streamQueues) once a stream reaches a terminal state -- closed both ways
// (noteHalfClosed returning true), reset by either side, or the connection
// itself failing (teardownAllStreams). Without this the streamQueues entry
// (and the closure over its `items` array) would be retained for the rest
// of the process's life.
function teardownStream(id) {
  state.streams.delete(id);
  state.streamHalfClosed.delete(id);
  state.streamQueues.delete(id);
}

// teardownAllStreams tears down every still-tracked stream, used once the
// connection itself fails (onDisconnect): none of those streams will ever
// reach a graceful terminal state on their own once the connection is gone.
// Iterates the union of streams and streamQueues (mirrors the Go adapter's
// teardownAllStreams unioning streams/streamWorkers) rather than just
// streams.keys(), so an id with a queue but no app-visible stream entry
// still gets its queue drained instead of leaking.
function teardownAllStreams() {
  const ids = new Set([...state.streams.keys(), ...state.streamQueues.keys()]);
  for (const id of ids) teardownStream(id);
}

// drainAbortedOps error-acks every item still queued in q.items (queued
// write/close_write commands that never got to run) after a RESET has
// marked the queue aborted -- see resetStream and WIRE.md section 2.5
// ("RESET is abortive; it discards buffered data and unblocks writers").
function drainAbortedOps(q) {
  const pending = q.items.splice(0, q.items.length);
  for (const item of pending) {
    cmdErr(item.seq, "reset: queued command aborted by RESET");
  }
}

// resetStream runs RESET out-of-band, immediately, bypassing the
// write/close_write FIFO (see the streamQueues field comment and
// WIRE.md section 2.5): it marks the queue aborted and drains anything
// still sitting in it with an error ack, then calls stream.reset(), which
// discards buffered data, unblocks the peer, and (by destroying the
// MixerStream) unblocks a write that is already running on the queue and
// blocked on exhausted send credit -- that write's own pending callback
// still settles it, so its own error ack is reported from there, not here.
function resetStream(id, stream, code, message, seq) {
  const q = state.streamQueues.get(id);
  if (q) {
    q.aborted = true;
    drainAbortedOps(q);
  }
  teardownStream(id);
  try {
    stream.reset(code, message);
    ack(seq);
  } catch (e) {
    cmdErr(seq, "reset: " + (e && e.message ? e.message : String(e)));
  }
}

// scaleMs applies --time-scale to a duration this adapter receives verbatim
// from a command, mirroring the Go adapter's identical helper
// (docs/CONFORMANCE.md section 3.4). floor_ms is the runner's own effective
// timing floor, carried in set_options.floor_ms (coordination contract with
// conformance/runner; falls back to 300ms if the runner never sent one).
function scaleMs(ms) {
  const d = Math.round(ms * state.timeScale);
  return Math.max(state.floorMs || 300, d);
}

// noteHalfClosed records that direction ("read" or "write") of stream id has
// closed locally, and reports whether both directions are now closed.
function noteHalfClosed(id, direction) {
  const c = state.streamHalfClosed.get(id) || { read: false, write: false };
  c[direction] = true;
  state.streamHalfClosed.set(id, c);
  return c.read && c.write;
}

function wireStream(stream) {
  state.streams.set(stream.id, stream);
  emit({ event: "stream_opened", id: stream.id });
  stream.on("data", (chunk) => {
    emit({ event: "data", id: stream.id, data_b64: Buffer.from(chunk).toString("base64"), t_ms: tMs() });
  });
  stream.on("end", () => {
    emit({ event: "stream_closed", id: stream.id, direction: "read", t_ms: tMs() });
    if (noteHalfClosed(stream.id, "read")) {
      // Both directions closed: the stream is fully terminal (WIRE.md
      // section 2.5) -- tear down its bookkeeping now.
      emit({ event: "stream_closed", id: stream.id, direction: "both", t_ms: tMs() });
      teardownStream(stream.id);
    }
  });
  // stream.ts's own doc comments: a PEER-received RESET (handleReset) emits
  // 'reset' synchronously *and* (since something is listening for 'error'
  // here) destroy()s with the error, which Node surfaces as an 'error'
  // event on a later tick; a LOCALLY autonomous abort (e.g. this side's own
  // dispatch detecting an illegal DATA-after-CLOSE, stream.ts's reset())
  // only ever surfaces its code/message through that same destroy(err) path
  // -- it never emits 'reset' at all (mutation-testing gap: see
  // spec/fixtures/sequences/data_after_close_toward_client.json). So both
  // listeners are needed for full coverage; reported carries whichever
  // fires first so a peer-received RESET (both fire) is reported exactly
  // once.
  let reported = false;
  const reportReset = (code, message) => {
    if (reported || typeof code !== "number") return;
    reported = true;
    emit({ event: "stream_reset", id: stream.id, code, name: codeNameOf(code), message, t_ms: tMs() });
    teardownStream(stream.id);
  };
  stream.on("reset", ({ code, message }) => reportReset(code, message));
  stream.on("error", (err) => {
    // Only a StreamError is a stream_reset: rejectOutstanding() (conn.ts)
    // destroys every live stream with the *connection's* terminal ConnError
    // the same way, which must not be misreported as a per-stream reset --
    // that case is already covered by this adapter's own `disconnected`
    // event.
    if (err.name !== "StreamError") return;
    reportReset(err.code, err.message);
  });
}

// codeNameOf mirrors ws-mixer-go/wsmixer's ErrorCode.String() / the runner's own
// codes.Name -- kept tiny and local rather than importing errors.ts's
// codeName, since a RESET's numeric code is all a peer ever needs here.
const CODE_NAMES = {
  0: "NO_ERROR", 1: "PROTOCOL_ERROR", 2: "INTERNAL_ERROR", 3: "FLOW_CONTROL_ERROR",
  4: "FRAME_SIZE_ERROR", 5: "STREAM_CLOSED", 6: "REFUSED_STREAM", 7: "CANCEL",
  8: "STREAM_LIMIT", 9: "ENHANCE_YOUR_CALM", 10: "UNSUPPORTED", 11: "UNAUTHORIZED",
  12: "GOING_AWAY", 13: "KEEPALIVE_TIMEOUT", 14: "APPLICATION_CLOSE",
};
function codeNameOf(code) {
  return CODE_NAMES[code] ?? "INTERNAL_ERROR";
}

async function handleCommand(cmd) {
  const { seq, cmd: name } = cmd;
  switch (name) {
    case "set_options": {
      if (cmd.window !== undefined) state.window = cmd.window;
      if (cmd.max_streams !== undefined) state.maxStreams = cmd.max_streams;
      if (cmd.ping_interval_ms !== undefined) state.pingIntervalMs = cmd.ping_interval_ms;
      if (cmd.ping_timeout_ms !== undefined) state.pingTimeoutMs = cmd.ping_timeout_ms;
      if (cmd.hello_timeout_ms !== undefined) state.helloTimeoutMs = cmd.hello_timeout_ms;
      if (cmd.time_scale !== undefined) state.timeScale = cmd.time_scale;
      if (cmd.floor_ms !== undefined) state.floorMs = cmd.floor_ms;
      if (cmd.allow_subfloor_timing !== undefined) state.allowSubfloorTiming = cmd.allow_subfloor_timing;
      ack(seq);
      return;
    }

    case "listen":
    case "open_stream":
      // Role-inapplicable: the JS SDK has no server (conformance/README.md).
      // The runner never issues these once it has seen `ready.roles`, but
      // reply the documented unsupported shape defensively anyway --
      // coordination contract (c): {"ok":false,"unsupported":true,"error":
      // "..."} so the runner (conformance/runner/adapter/adapter.go's
      // isUnsupported) treats this as a SKIP, not a FAIL.
      emit({
        event: "error",
        seq,
        ok: false,
        unsupported: true,
        error: "unsupported",
        message: `unsupported command ${name}: the JS SDK has no server role`,
      });
      return;

    case "connect": {
      try {
        const timing = {
          helloTimeout: state.helloTimeoutMs,
        };
        if (state.allowSubfloorTiming) {
          timing.minPingInterval = 1;
          timing.minPingTimeout = 1;
        }
        // docs/CONFORMANCE.md section 1: "one adapter process = one
        // connection" and section 1.3 puts reconnect/backoff policy out of
        // scope for the adapter protocol entirely -- so reconnect defaults
        // to disabled (maxAttempts:0), matching "one connection" rather than
        // "keep trying". Without that, a single connection attempt that
        // spuriously fails (e.g. a scaled hello_timeout_ms racing real
        // process/goroutine scheduling around the raw actor sending welcome)
        // triggers MixerClient's own infinite background retry loop against
        // a raw actor that only ever accepts one connection -- silently
        // hanging the test instead of failing fast. A scenario that actually
        // wants reconnect (drain_reconnect, application_close) opts in
        // explicitly via connect.reconnect.enabled.
        const reconnectCmd = cmd.reconnect;
        const reconnect =
          reconnectCmd && reconnectCmd.enabled
            ? {
                maxAttempts: reconnectCmd.maxAttempts ?? Infinity,
                // baseMs/capMs (docs/CONFORMANCE.md's connect command):
                // override the SDK's default base/cap backoff, e.g. so
                // application_close's connected-phase 4014 (which now starts
                // its reconnect at the cap, WIRE.md section 2.9) doesn't wait
                // up to the SDK's 60s default cap and blow the runner's
                // fixed per-step await timeout. Absent or non-positive ->
                // the SDK's own defaults (undefined is dropped by
                // ReconnectOptions, never forwarded as 0/negative).
                ...(reconnectCmd.baseMs > 0 ? { base: reconnectCmd.baseMs } : {}),
                ...(reconnectCmd.capMs > 0 ? { cap: reconnectCmd.capMs } : {}),
              }
            : { maxAttempts: 0 };
        let connectedOnce = false;
        const client = await connect(cmd.url, {
          token: cmd.token,
          agent: { sdk: "ws-mixer-js-conformance", sdk_version: SDK_VERSION },
          window: state.window,
          maxStreams: state.maxStreams,
          reconnect,
          onStream: (stream) => wireStream(stream),
          onConnect: (welcome) => {
            state.lastWelcome = welcome;
            if (connectedOnce) {
              // A reconnect (not the initial connect): MixerClient's
              // onConnect fires on every successful handshake, including
              // ones after the client's own internal reconnect loop redials
              // (client.ts's connectOnce calls opts.onConnect on every
              // success, not just the first) -- coordination contract (b).
              const conn = client && client.currentConn ? client.currentConn() : null;
              emit({ event: "reconnected", session: conn ? conn.session : "", welcome });
            }
            connectedOnce = true;
          },
          onApp: (body) => emit({ event: "app", body }),
          onDrain: (d) => emit({ event: "drain", reason: d.reason, last_stream_id: d.lastStreamId, deadline_ms: d.deadlineMs, message: d.message }),
          onDisconnect: (info) => {
            // The connection itself is gone: none of its still-open streams
            // will ever reach a graceful terminal state on their own, so
            // tear them all down rather than leaking their queue entries
            // forever (mirrors the Go adapter's teardownAllStreams).
            teardownAllStreams();
            // phase is always present (docs/CONFORMANCE.md section 5's
            // `disconnected` field table); close_reason only when a reason
            // was actually received from the peer -- never re-truncated or
            // normalised, and never this side's own echoed reason (see
            // CLIENT-SDK.md's closeReason row).
            const e = { event: "disconnected", phase: info.phase, message: info.message, fatal: info.fatal };
            if (info.wsCode !== undefined) e.ws_code = info.wsCode;
            if (info.closeReason) e.close_reason = info.closeReason;
            if (info.protocolError) {
              e.error_code = info.code;
              e.error_name = info.name;
            } else if (info.errorCode !== undefined) {
              e.error_code = info.errorCode;
              // Prefer the SDK's own errorName when it supplied one (it
              // already derives the wire name from a bare 4xxx close code
              // too, not just an explicit error{} -- see DisconnectReason's
              // own errorName doc); codeNameOf is only a fallback for the
              // (currently theoretical) case where errorCode is present but
              // errorName isn't.
              e.error_name = info.errorName ?? codeNameOf(info.errorCode);
            }
            emit(e);
          },
          _timing: timing,
        });
        state.client = client;
        const conn = client.currentConn();
        ack(seq);
        emit({ event: "connected", seq, session: conn ? conn.session : "", welcome: state.lastWelcome ?? {} });
      } catch (e) {
        cmdErr(seq, "connect: " + (e && e.message ? e.message : String(e)));
      }
      return;
    }

    case "write": {
      const stream = state.streams.get(cmd.id);
      if (!stream) return cmdErr(seq, `write: no such stream ${cmd.id}`);
      const buf = Buffer.from(cmd.data_b64, "base64");
      // Queued on this stream's own FIFO queue (see enqueueStreamOp): write
      // acks asynchronously, so a close_write for the same id that arrives
      // before this ack must still wait behind this write's bytes actually
      // reaching the wire.
      enqueueStreamOp(
        cmd.id,
        seq,
        () =>
          new Promise((resolve) => {
            stream.write(buf, (err) => {
              if (err) cmdErr(seq, "write: " + err.message);
              else ack(seq);
              resolve();
            });
          }),
      );
      return;
    }

    case "close_write": {
      const stream = state.streams.get(cmd.id);
      if (!stream) return cmdErr(seq, `close_write: no such stream ${cmd.id}`);
      enqueueStreamOp(cmd.id, seq, () => {
        stream.closeWrite();
        ack(seq);
        emit({ event: "stream_closed", id: cmd.id, direction: "write", t_ms: tMs() });
        if (noteHalfClosed(cmd.id, "write")) {
          // Both directions closed: the stream is fully terminal
          // (WIRE.md section 2.5) -- tear down its bookkeeping now.
          emit({ event: "stream_closed", id: cmd.id, direction: "both", t_ms: tMs() });
          teardownStream(cmd.id);
        }
      });
      return;
    }

    case "reset": {
      // RESET is abortive (WIRE.md section 2.5) and runs out-of-band,
      // immediately -- it must not wait behind a blocked write on the
      // per-stream FIFO the way write/close_write do. See resetStream.
      const stream = state.streams.get(cmd.id);
      if (!stream) return cmdErr(seq, `reset: no such stream ${cmd.id}`);
      resetStream(cmd.id, stream, cmd.code, cmd.message ?? "", seq);
      return;
    }

    case "send_app": {
      if (!state.client) return cmdErr(seq, "send_app before connection established");
      try {
        await state.client.sendApp(cmd.body ?? {});
        ack(seq);
      } catch (e) {
        cmdErr(seq, "send_app: " + e.message);
      }
      return;
    }

    case "drain": {
      // docs/CONFORMANCE.md section 1.1: "Client may only use
      // client_requested." The public JS API has no standalone
      // "send drain, stay connected" primitive (see conformance/README.md) --
      // MixerClient.close() already sends exactly
      // drain{reason:"client_requested"} then a graceful close, which is the
      // closest first-class equivalent.
      if (!state.client) return cmdErr(seq, "drain before connection established");
      ack(seq);
      state.client.close().catch(() => {});
      return;
    }

    case "close": {
      if (!state.client) return cmdErr(seq, "close before connection established");
      // docs/CONFORMANCE.md's `close` command: `code` absent or `0`
      // (NO_ERROR) is the SDK's own graceful shutdown -- plain
      // `client.close()`, drain{client_requested} -> grace ->
      // error{NO_ERROR} -> close 1000 (WIRE.md section 2.10 step 14). `14`
      // is the only other accepted value -- an immediate close via the
      // SDK's application-close API (CLIENT-SDK.md's "Application close"
      // row, D-2026-09-25-01): `error{code:14,message}` + WS close `4014`,
      // no drain. Any other value is a scenario error.
      if (cmd.code !== undefined && cmd.code !== 0 && cmd.code !== 14) {
        return cmdErr(seq, "close: only 0 or 14");
      }
      const opts = cmd.code === 14 ? { message: cmd.message ?? "" } : undefined;
      state.client.close(opts).catch(() => {});
      ack(seq);
      return;
    }

    case "shutdown":
      ack(seq);
      process.exit(0);
      return;

    default:
      cmdErr(seq, "unsupported command " + name);
  }
}

// main() wires up the real process (stdin command loop, the "ready" event,
// and the top-level error handlers below) -- split out from module load so
// adapter.test.mjs can import this file's internal functions (state,
// enqueueStreamOp, resetStream, ...) without also starting a real stdin
// read loop and emitting "ready" into the test's own stdout.
async function main() {
  // Belt and braces: docs/CONFORMANCE.md section 1's `error` event covers
  // "the adapter itself broke" too, so a bug here should be a visible,
  // structured event on stdout rather than a silent process exit the
  // runner has to infer from a closed pipe.
  process.on("unhandledRejection", (reason) => {
    adapterErr("unhandledRejection: " + (reason && reason.message ? reason.message : String(reason)));
  });
  process.on("uncaughtException", (err) => {
    adapterErr("uncaughtException: " + err.message);
  });

  emit({ event: "ready", sdk: "ws-mixer-js", sdk_version: SDK_VERSION, roles: ["client"] });

  const readline = await import("node:readline");
  const rl = readline.createInterface({ input: process.stdin });
  rl.on("line", (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    let cmd;
    try {
      cmd = JSON.parse(trimmed);
    } catch (e) {
      adapterErr("bad command json: " + e.message);
      return;
    }
    void handleCommand(cmd);
  });
}

// Only run main() when this file is executed directly (`node adapter.mjs`,
// how the conformance runner spawns it) -- not when adapter.test.mjs
// imports it to drive the internal queue functions against a fake stream.
// Compares realpaths rather than raw strings so a symlinked invocation
// (e.g. `node /some/symlink/adapter.mjs`) still resolves to the same file as
// import.meta.url; if either path can't be resolved (missing file,
// permission error), treat this as the main module rather than silently
// skipping startup.
function isMainModule() {
  if (!process.argv[1]) return false;
  try {
    return (
      fs.realpathSync(fileURLToPath(import.meta.url)) ===
      fs.realpathSync(process.argv[1])
    );
  } catch {
    return true;
  }
}

if (isMainModule()) {
  await main();
}

export {
  state,
  STREAM_QUEUE_CAP,
  enqueueStreamOp,
  teardownStream,
  teardownAllStreams,
  resetStream,
  noteHalfClosed,
  handleCommand,
  setEmitSink,
};
