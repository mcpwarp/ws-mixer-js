// Unit tests for adapter.mjs's internal per-stream FIFO queue
// (streamQueues/enqueueStreamOp/pumpStreamQueue/resetStream/teardownStream --
// mirrors conformance/adapters/go/main.go's streamWorkers and its
// race_test.go), run with `node --test conformance/adapters/js/` (no deps,
// node:test/node:assert only). Drives handleCommand and the exported
// internals directly against fake stream objects -- no real MixerClient
// connection -- so these are fast, deterministic, unit-level tests of the
// queue's own behavior: FIFO ordering, queue_full, RESET draining pending
// ops with error acks, and teardown clearing the bookkeeping maps.
//
// adapter.mjs only runs its stdin command loop / emits "ready" when
// executed directly (see its `if (process.argv[1] === ...)` guard) -- an
// import here just pulls in the exported functions/state without starting
// any of that.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  state,
  STREAM_QUEUE_CAP,
  handleCommand,
  teardownStream,
  teardownAllStreams,
  setEmitSink,
} from "./adapter.mjs";

function b64(s) {
  return Buffer.from(s).toString("base64");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// waitUntil polls cond until it's true (or timeoutMs passes), since the
// queue's own pump loop runs on promise microtasks/callbacks this test
// doesn't otherwise get a signal for.
async function waitUntil(cond, timeoutMs = 2000) {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) {
      throw new Error("waitUntil: timed out waiting for condition");
    }
    await sleep(5);
  }
}

// resetState clears every map adapter.mjs's module-level `state` carries
// between commands, so each test starts from a clean slate despite state
// being a singleton shared across the whole imported module.
function resetState() {
  state.streams.clear();
  state.streamQueues.clear();
  state.streamHalfClosed.clear();
}

// withCapturedStdout installs adapter.mjs's setEmitSink for the duration of
// fn (async or not), collecting every event its emit() produced, in order,
// as plain objects -- letting a test assert on acks/errors directly.
// Deliberately does NOT intercept the real process.stdout: that's also
// where node:test's own TAP reporter writes, and hijacking it collides
// with that (garbled/interleaved TAP output, dropped test results).
async function withCapturedStdout(fn) {
  const events = [];
  setEmitSink((e) => events.push(e));
  try {
    await fn();
  } finally {
    setEmitSink(null); // restores adapter.mjs's default (real stdout) sink
  }
  return events;
}

test("FIFO order: write/write/close_write for one stream run in the order they were issued", async () => {
  resetState();
  const order = [];
  const fakeStream = {
    write(buf, cb) {
      // Async, like the real stream.write()'s callback -- so a naive
      // dispatch-without-queuing implementation could let these race.
      setImmediate(() => {
        order.push(buf.toString());
        cb(null);
      });
    },
    closeWrite() {
      order.push("close_write");
    },
    reset() {},
  };
  state.streams.set(1, fakeStream);

  const events = await withCapturedStdout(async () => {
    handleCommand({ cmd: "write", seq: 1, id: 1, data_b64: b64("a") });
    handleCommand({ cmd: "write", seq: 2, id: 1, data_b64: b64("b") });
    handleCommand({ cmd: "close_write", seq: 3, id: 1 });
    await waitUntil(() => order.length === 3);
  });

  assert.deepEqual(order, ["a", "b", "close_write"]);
  const acks = events.filter((e) => e.event === "ack").map((e) => e.seq);
  assert.deepEqual(acks, [1, 2, 3]);
});

test("queue_full: an enqueue past STREAM_QUEUE_CAP gets a queue_full error ack instead of blocking", async () => {
  resetState();
  let release;
  const blocked = new Promise((resolve) => {
    release = resolve;
  });
  const fakeStream = {
    write(buf, cb) {
      blocked.then(() => cb(null));
    },
    closeWrite() {},
    reset() {},
  };
  state.streams.set(2, fakeStream);

  const events = await withCapturedStdout(async () => {
    // Occupies the queue (busy) until release() is called below -- every
    // later write for this id just piles up in q.items behind it.
    handleCommand({ cmd: "write", seq: 100, id: 2, data_b64: b64("x") });
    await sleep(20); // let the pump actually pick it up (busy = true)

    for (let i = 0; i < STREAM_QUEUE_CAP; i++) {
      handleCommand({ cmd: "write", seq: 200 + i, id: 2, data_b64: b64("x") });
    }
    // The queue is now exactly full; this one must be rejected.
    handleCommand({ cmd: "write", seq: 9999, id: 2, data_b64: b64("x") });

    release();
    await waitUntil(() => {
      const q = state.streamQueues.get(2);
      return !q || (q.items.length === 0 && !q.busy);
    });
  });

  const errors = events.filter((e) => e.event === "error");
  assert.equal(errors.length, 1, `want exactly one queue_full error, got: ${JSON.stringify(errors)}`);
  assert.equal(errors[0].seq, 9999);
  assert.match(errors[0].message, /queue_full/);

  // Every other queued write (200..200+STREAM_QUEUE_CAP-1) drained
  // normally once release() ran.
  const acks = events.filter((e) => e.event === "ack").map((e) => e.seq);
  assert.equal(acks.length, 1 + STREAM_QUEUE_CAP);
});

test("reset drains pending queued ops with error acks and tears down bookkeeping", async () => {
  resetState();
  let release;
  const blocked = new Promise((resolve) => {
    release = resolve;
  });
  const resetCalls = [];
  const fakeStream = {
    write(buf, cb) {
      blocked.then(() => cb(null));
    },
    closeWrite() {},
    reset(code, message) {
      resetCalls.push([code, message]);
    },
  };
  state.streams.set(3, fakeStream);

  const events = await withCapturedStdout(async () => {
    handleCommand({ cmd: "write", seq: 300, id: 3, data_b64: b64("a") }); // becomes busy, blocks
    await sleep(20);
    handleCommand({ cmd: "write", seq: 301, id: 3, data_b64: b64("b") }); // queued
    handleCommand({ cmd: "write", seq: 302, id: 3, data_b64: b64("c") }); // queued
    handleCommand({ cmd: "reset", seq: 303, id: 3, code: 7, message: "abort" });
    await sleep(20);
  });
  release(); // let the busy write's own (now-irrelevant) callback settle

  const bySeq = new Map(events.map((e) => [e.seq, e]));
  assert.equal(bySeq.get(301)?.event, "error");
  assert.match(bySeq.get(301).message, /reset: queued command aborted by RESET/);
  assert.equal(bySeq.get(302)?.event, "error");
  assert.match(bySeq.get(302).message, /reset: queued command aborted by RESET/);
  assert.equal(bySeq.get(303)?.event, "ack");
  assert.deepEqual(resetCalls, [[7, "abort"]]);

  assert.equal(state.streamQueues.has(3), false);
  assert.equal(state.streams.has(3), false);
});

test("resetStream emits an error ack (not a thrown exception) when stream.reset() throws", async () => {
  resetState();
  const fakeStream = {
    write(buf, cb) {
      cb(null);
    },
    closeWrite() {},
    reset() {
      throw new Error("boom");
    },
  };
  state.streams.set(4, fakeStream);

  const events = await withCapturedStdout(async () => {
    handleCommand({ cmd: "reset", seq: 400, id: 4, code: 7, message: "abort" });
  });

  assert.equal(events.length, 1);
  assert.equal(events[0].event, "error");
  assert.equal(events[0].seq, 400);
  assert.match(events[0].message, /^reset: boom$/);
});

test("teardownStream/teardownAllStreams clear every bookkeeping map for their id(s)", () => {
  resetState();
  state.streams.set(10, {});
  state.streamQueues.set(10, { items: [], busy: false, aborted: false });
  state.streamHalfClosed.set(10, { read: true, write: false });

  teardownStream(10);
  assert.equal(state.streams.has(10), false);
  assert.equal(state.streamQueues.has(10), false);
  assert.equal(state.streamHalfClosed.has(10), false);

  state.streams.set(11, {});
  state.streamQueues.set(11, { items: [], busy: false, aborted: false });
  state.streams.set(12, {});
  state.streamQueues.set(12, { items: [], busy: false, aborted: false });
  // id 13 has a queue but no streams entry -- teardownAllStreams must
  // iterate the union of both maps (mirrors the Go adapter's
  // streams/streamWorkers union), not just streams.keys(), or this queue
  // would leak past the connection failing.
  state.streamQueues.set(13, { items: [], busy: false, aborted: false });

  teardownAllStreams();
  assert.equal(state.streams.size, 0);
  assert.equal(state.streamQueues.size, 0);
});

test("close: an out-of-range code (MixerClient.close()'s synchronous RangeError) replies with a command error, not an unhandled rejection", async () => {
  resetState();
  let closeCalledWith = "not called";
  // Mirrors MixerClient.close()'s real behavior: opts.code is validated
  // SYNCHRONOUSLY, throwing before any Promise even exists -- the case this
  // test exists for is that throw escaping handleCommand's "close" case
  // uncaught, which (handleCommand is void'd by its caller) would surface
  // only as an unhandled rejection and kill the whole adapter process.
  state.client = {
    close(opts) {
      if (opts && opts.code !== undefined && (!Number.isInteger(opts.code) || opts.code < 0 || opts.code > 999)) {
        throw new RangeError(`ws-mixer: close() code must be an integer in [0, 999]; got ${opts.code}`);
      }
      closeCalledWith = opts;
      return Promise.resolve();
    },
  };

  const events = await withCapturedStdout(async () => {
    await handleCommand({ cmd: "close", seq: 1, code: 99999 });
  });

  assert.equal(events.length, 1);
  assert.equal(events[0].event, "error");
  assert.equal(events[0].seq, 1);
  assert.match(events[0].message, /^close: /);
  assert.equal(closeCalledWith, "not called"); // never reached a real close attempt

  state.client = null;
});

test("close: code 0 (NO_ERROR) takes the graceful path -- plain client.close(), not an application-initiated close({code:0})", async () => {
  resetState();
  let closeCalledWith = "not called";
  state.client = {
    close(opts) {
      closeCalledWith = opts;
      return Promise.resolve();
    },
  };

  const events = await withCapturedStdout(async () => {
    await handleCommand({ cmd: "close", seq: 1, code: 0, message: "conformance graceful_close" });
  });

  assert.equal(events.length, 1);
  assert.equal(events[0].event, "ack");
  assert.equal(events[0].seq, 1);
  // Graceful path: client.close() called with no opts at all (drain then
  // close), not client.close({code:0, ...}) (which would skip the drain --
  // spec/conformance/scenarios/graceful_close.json is the only pair
  // scenario exercising WIRE.md section 2.10 step 14's drain sequence).
  assert.equal(closeCalledWith, undefined);

  state.client = null;
});
