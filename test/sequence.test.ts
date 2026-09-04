/**
 * Replays spec/fixtures/sequences/*.json against a real MixerConn over a
 * FakeWS transport. Per spec/README.md, `role` names which side the
 * transcript is written from; this SDK is client-only, so only the
 * `role:"client"` fixtures (the majority of the corpus) can be replayed
 * directly. The `role:"server"` fixtures are scripted from the Go server's
 * perspective (its `recv`/`send` are the client's `send`/`recv` and would
 * need every step's polarity inverted plus server-only assertions dropped)
 * and are skipped below, one `it.skip` per file with that reason -- per the
 * task's explicit instruction to skip them with a reason rather than
 * silently ignore them.
 */
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { FakeWS } from "./helpers/fake-ws.js";
import { MixerConn } from "../src/conn.js";
import { encodeControl } from "../src/control.js";
import { decodeFrame, encodeClose, encodeData, encodeOpen, encodeReset, encodeWindow, frameTypeName, resetCode, windowIncrement, FrameType } from "../src/frame.js";
import { codeName } from "../src/errors.js";
import type { MixerStream } from "../src/stream.js";
import { resolveSpecDir } from "./helpers/spec-dir.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const specResult = resolveSpecDir(here);
const seqDir = path.join(specResult.dir, "fixtures/sequences");

interface Step {
  recv?: Record<string, unknown>;
  send?: Record<string, unknown>;
  wait_ms?: number;
  expect?: { error_code?: string | null; close_code?: number | null; stream_state?: string | null; stream_reset_code?: string };
}

interface SequenceFixture {
  description: string;
  role: "server" | "client";
  steps: Step[];
}

function loadAll(): Array<{ file: string; fixture: SequenceFixture }> {
  return readdirSync(seqDir)
    .filter((f) => f.endsWith(".json"))
    .map((file) => ({ file, fixture: JSON.parse(readFileSync(path.join(seqDir, file), "utf8")) as SequenceFixture }));
}

function isFrameLike(v: Record<string, unknown>): v is { type: string; stream_id: number; payload_hex?: string; payload_length?: number; increment?: number; code?: number; message?: string } {
  return "type" in v;
}

function encodeStepFrame(v: ReturnType<typeof isFrameLike> extends never ? never : { type: string; stream_id: number; payload_hex?: string; payload_length?: number; increment?: number; code?: number; message?: string }): Uint8Array {
  const id = v.stream_id;
  switch (v.type) {
    case "OPEN":
      return encodeOpen(id);
    case "CLOSE":
      return encodeClose(id);
    case "WINDOW":
      return encodeWindow(id, v.increment!);
    case "RESET":
      return encodeReset(id, v.code!, v.message ?? "");
    case "DATA": {
      const payload = v.payload_hex !== undefined ? Buffer.from(v.payload_hex, "hex") : Buffer.alloc(v.payload_length ?? 0);
      return encodeData(id, payload);
    }
    default:
      throw new Error(`unknown frame type ${v.type}`);
  }
}

describe("sequence fixtures", () => {
  if (!specResult.available) {
    it.skip(`spec fixtures unavailable: ${specResult.reason}`, () => {});
    return;
  }
  const all = loadAll();

  it("loaded every sequence fixture in the corpus", () => {
    expect(all.length).toBeGreaterThanOrEqual(20);
  });

  for (const { file, fixture } of all) {
    if (fixture.role === "server") {
      it.skip(`${file} (role: server -- written from the Go server's perspective; this SDK is client-only and does not replay server-role transcripts)`, () => {});
      continue;
    }

    if (file === "window_exhaustion_then_resume_client.json") {
      // This fixture's two "send DATA" steps require driving a real
      // app-level MixerStream.write() call (ws-mixer only emits DATA in
      // response to application code writing to a stream, never on its
      // own) -- go/wsmixer/sequence_test.go and the conformance runner both
      // have that hook (Stream.WriteContext / the adapter's `write`
      // command), but this loop only replays a fixture's `send` steps by
      // asserting against ws.sent, with no equivalent call into the actual
      // MixerStream object. See the fixture's own description.
      it.skip(`${file} (needs an app-level Stream.write() hook this wire-replay-only harness doesn't have)`, () => {});
      continue;
    }

    it(file, async () => {
      const ws = new FakeWS();
      // A client-role fixture's own scripted "send hello" step declares this
      // side's receive window (e.g. credit_violation_toward_client.json's
      // 16384, mirroring credit_violation.json's server-side twin); apply it
      // so the SDK under test actually advertises it, instead of silently
      // falling back to MixerConn's 262144 default.
      const helloStep = fixture.steps.find((s) => s.send?.t === "hello");
      const helloWindow = typeof helloStep?.send?.window === "number" ? (helloStep.send.window as number) : undefined;
      const conn = new MixerConn(ws, { token: "t", agent: { sdk: "ws-mixer-js-test", sdk_version: "0.0.0" }, window: helloWindow });
      conn.on("error", () => {});
      const streams = new Map<number, MixerStream>();
      conn.on("stream", (s) => {
        streams.set(s.id, s);
        s.on("error", () => {});
      });
      let lastClose: { wsCode: number; errorCode?: number; message: string } | undefined;
      conn.on("close", (c) => (lastClose = c));
      let lastTouchedStreamId: number | undefined;
      const streamResetCodes = new Map<number, number>();

      void conn.handshake().catch(() => {});

      // Cursor into ws.sent for "send" steps that specify a frame: each
      // match advances the cursor so a later step can't re-match the same
      // already-consumed frame.
      let sentCursor = 0;

      const scanSentResets = (): void => {
        for (const frame of ws.sent) {
          let f;
          try {
            f = decodeFrame(frame);
          } catch {
            continue;
          }
          if (f.type === FrameType.RESET && f.streamId > 0) {
            streamResetCodes.set(f.streamId, f.payload[3]!); // low byte of the big-endian uint32 code
          }
        }
      };

      // duplicate_pong_from_server.json and client_dead_peer_timeout.json
      // (the only two client-role fixtures with a wait_ms step) now write
      // their welcome's ping_interval/ping_timeout as plain, unscaled
      // protocol time (30000/90000, matching what a real server sends) and
      // their wait_ms as plain protocol time too, instead of pre-multiplying
      // wait_ms for a fixed WAIT_SCALE divisor. This SDK is exercised
      // directly via MixerConn (no _timing override reaches a real ping
      // loop's actual interval -- only conn.ts's floor *check*), and
      // control.ts's wire-level parseWelcome unconditionally rejects a
      // welcome.ping_interval below 5000ms regardless of any test-only
      // option, so this suite cannot shrink the SDK's real ping timer below
      // that floor. WAIT_SCALE is therefore the largest downscale that still
      // keeps a scaled welcome legal (5000 = 30000 * 1/6, the floor exactly;
      // ping_timeout scales to 15000, still >= 2x), applied identically to
      // wait_ms so the two stay in the same proportion the fixture encodes
      // (wait_ms > ping_interval, wait_ms > ping_timeout).
      const WATCHDOG_FILES = new Set(["duplicate_pong_from_server.json", "client_dead_peer_timeout.json"]);
      const WAIT_SCALE = WATCHDOG_FILES.has(file) ? 1 / 6 : 1;

      for (const step of fixture.steps) {
        if (step.wait_ms !== undefined) {
          await new Promise((r) => setTimeout(r, Math.max(1, Math.round(step.wait_ms! * WAIT_SCALE))));
          continue;
        }
        if (step.send) {
          // "send" from the client's perspective (role: client): the fixture
          // scripts what our MixerConn transmits.
          await new Promise((r) => setImmediate(r));
          if (isFrameLike(step.send)) {
            const expected = step.send;
            const idx = ws.sent.slice(sentCursor).findIndex((raw) => {
              let f;
              try {
                f = decodeFrame(raw);
              } catch {
                return false;
              }
              if (frameTypeName(f.type) !== expected.type) return false;
              if (f.streamId !== expected.stream_id) return false;
              if (expected.type === "WINDOW" && expected.increment !== undefined && windowIncrement(f) !== expected.increment) return false;
              if (expected.type === "RESET" && expected.code !== undefined && resetCode(f) !== expected.code) return false;
              return true;
            });
            expect(idx, `expected the client to have transmitted a ${expected.type} frame for stream ${expected.stream_id}`).toBeGreaterThanOrEqual(0);
            sentCursor += idx + 1;
          }
          continue;
        }
        if (step.recv) {
          const payload = step.recv;
          await new Promise((r) => setImmediate(r));
          if (isFrameLike(payload)) {
            if ("stream_id" in payload) lastTouchedStreamId = payload.stream_id as number;
            ws.receive(encodeStepFrame(payload as never));
          } else if (WATCHDOG_FILES.has(file) && payload.t === "welcome") {
            // Scale this welcome's ping_interval/ping_timeout by the same
            // WAIT_SCALE as wait_ms above, so the SDK's real (unaccelerated)
            // keepalive timers actually run within the scaled wait.
            const scaled = { ...payload };
            if (typeof scaled.ping_interval === "number") scaled.ping_interval = Math.round(scaled.ping_interval * WAIT_SCALE);
            if (typeof scaled.ping_timeout === "number") scaled.ping_timeout = Math.round(scaled.ping_timeout * WAIT_SCALE);
            ws.receive(encodeData(0, encodeControl(scaled as never)));
          } else {
            ws.receive(encodeData(0, encodeControl(payload as never)));
          }
          await new Promise((r) => setImmediate(r));
          scanSentResets();
          continue;
        }
        if (step.expect) {
          await new Promise((r) => setImmediate(r));
          if (step.expect.error_code !== undefined) {
            if (step.expect.error_code === null) {
              expect(lastClose).toBeUndefined();
            } else {
              expect(lastClose?.errorCode !== undefined ? codeName(lastClose.errorCode) : undefined).toBe(step.expect.error_code);
            }
          }
          if (step.expect.close_code !== undefined && step.expect.close_code !== null) {
            expect(lastClose?.wsCode).toBe(step.expect.close_code);
          }
          if (step.expect.stream_reset_code !== undefined) {
            // Two sources, mirroring go/wsmixer/sequence_test.go's checkExpect:
            // streamResetCodes (scanned from ws.sent) covers a RESET the
            // client itself autonomously transmitted; a RESET the client
            // instead *received* (e.g. reset_on_closed_stream_toward_client.json)
            // never appears there, so fall back to the live MixerStream's
            // own recorded resetCode.
            const target = lastTouchedStreamId !== undefined ? streams.get(lastTouchedStreamId) : undefined;
            const code =
              (lastTouchedStreamId !== undefined ? streamResetCodes.get(lastTouchedStreamId) : undefined) ?? target?.resetCode;
            expect(code !== undefined ? codeName(code) : undefined).toBe(step.expect.stream_reset_code);
          }
          if (step.expect.stream_state !== undefined && step.expect.stream_state !== null) {
            const target = lastTouchedStreamId !== undefined ? streams.get(lastTouchedStreamId) : undefined;
            if (target) {
              expect(target.getState()).toBe(step.expect.stream_state);
            } else if (lastTouchedStreamId !== undefined && streamResetCodes.has(lastTouchedStreamId)) {
              // Refused before a MixerStream was ever created (e.g.
              // STREAM_LIMIT): there's no live stream to call getState() on,
              // so the only observable evidence is the recorded RESET itself
              // -- derive "closed" from that evidence and compare it against
              // the fixture's expectation, rather than asserting the fixture
              // literal against a hardcoded string.
              const observedState = streamResetCodes.has(lastTouchedStreamId) ? "closed" : undefined;
              expect(observedState).toBe(step.expect.stream_state);
            } else {
              // Neither a live stream nor a recorded RESET for this id: the
              // fixture expected an observable stream_state but nothing
              // actually happened to it, which must fail loudly rather than
              // pass vacuously.
              throw new Error(
                `expected stream_state=${step.expect.stream_state} for stream ${lastTouchedStreamId}, but the stream was never created and no RESET was recorded`,
              );
            }
          }
        }
      }
    });
  }
});
