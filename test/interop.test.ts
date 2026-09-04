/**
 * Interop test against the real Go server, driven via cmd/testserver in the
 * (now separate) ws-mixer-go repo -- see that program's main.go for the
 * stdin-command / stdout-event protocol. Exercises a full request/response
 * round trip on N server-opened streams, an `app` round trip, and immediate
 * reconnect after `Drain`.
 *
 * Post-split, ws-mixer-go is a sibling repo, not a subtree of this one.
 * Resolution order for the checkout to build cmd/testserver from:
 *
 *  1. `WSMIXER_GO_DIR` env var, when set (must exist).
 *  2. `.goserver/` at the repo root, as populated by `npm run fetch-goserver`
 *     (shallow clone at the tag in `goserver.pin`). This is what CI uses.
 *  3. `../ws-mixer-go`, a sibling checkout -- convenient for local dev
 *     against a repo extracted from the same former monorepo tree.
 *
 * Skips cleanly (with a reason, not a silent pass) if `go` is unavailable,
 * no ws-mixer-go checkout can be found, or the build itself fails (e.g.
 * ws-mixer-go's own module isn't wired up yet).
 */
import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { connect, type MixerClient } from "../src/client.js";
import type { MixerStream } from "../src/stream.js";

// Toolchain resolution: prefer $GO (an explicit path to a `go` binary),
// falling back to whatever `go` is on PATH. The build below additionally
// sets GOTOOLCHAIN=auto so that a resolved `go` older than cmd/testserver's
// `go` directive transparently downloads and uses a matching toolchain
// instead of failing the build outright -- this makes the suite robust to a
// PATH `go` that doesn't match what the module needs, without requiring
// $GO to be set at all. GOTOOLCHAIN=auto is also Go's own default since
// 1.21, but is set explicitly here in case the ambient environment (e.g. a
// CI image pinning GOTOOLCHAIN=local) overrides that default.
const GO_BIN = process.env.GO?.trim() || "go";
const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "..");

function resolveGoDir(): { dir: string; reason?: string } {
  const envDir = process.env.WSMIXER_GO_DIR?.trim();
  if (envDir) {
    return existsSync(envDir) ? { dir: envDir } : { dir: envDir, reason: `WSMIXER_GO_DIR="${envDir}" does not exist` };
  }
  const fetched = path.resolve(repoRoot, ".goserver");
  if (existsSync(fetched)) return { dir: fetched };
  const sibling = path.resolve(repoRoot, "../ws-mixer-go");
  if (existsSync(sibling)) return { dir: sibling };
  let pin = "?";
  try {
    pin = readFileSync(path.join(repoRoot, "goserver.pin"), "utf8").trim();
  } catch {
    // goserver.pin missing is unusual but not this function's problem.
  }
  return {
    dir: fetched,
    reason: `ws-mixer-go checkout not found -- run 'npm run fetch-goserver' (clones ws-mixer-go@${pin} into .goserver/), set WSMIXER_GO_DIR to a local checkout, or check out ../ws-mixer-go as a sibling`,
  };
}

/**
 * Resolves the toolchain AND attempts the real build, both synchronously at
 * module load (before `describe.skipIf` below is evaluated): a `go version`
 * check alone isn't enough, because a `go` that runs fine can still fail to
 * build cmd/testserver (module fetch blocked, a toolchain download that
 * needs network access and none is available, an incompatible `go`
 * directive, the ws-mixer-go checkout itself not being buildable yet, ...).
 * Doing the real build here -- once, with its actual output kept for the
 * skip reason -- means a build failure also skips cleanly instead of
 * failing beforeAll(), per the task's explicit "skip with the build error
 * in the reason, not fail" requirement. GOTOOLCHAIN=auto is tried first
 * (also Go's own default since 1.21, set explicitly in case the ambient
 * environment overrides it) so a `go` older than the module's `go`
 * directive transparently downloads a matching toolchain instead of
 * failing outright -- and a PATH `go` newer than that directive (e.g. 1.27
 * against a `go 1.24` module) already just builds, no toolchain switch
 * needed at all.
 */
function resolveGo(): { available: boolean; reason?: string; binPath?: string } {
  const versionCheck = spawnSync(GO_BIN, ["version"], { stdio: "pipe" });
  if (versionCheck.error) {
    return {
      available: false,
      reason: `'${GO_BIN}' not found (set $GO to a go binary path, or put a working 'go' on PATH): ${versionCheck.error.message}`,
    };
  }
  if (versionCheck.status !== 0) {
    return {
      available: false,
      reason: `'${GO_BIN} version' exited ${versionCheck.status}: ${versionCheck.stderr?.toString().trim() || versionCheck.stdout?.toString().trim()}`,
    };
  }

  const goDirResult = resolveGoDir();
  if (goDirResult.reason) {
    return { available: false, reason: goDirResult.reason };
  }

  const tmpDir = mkdtempSync(path.join(os.tmpdir(), "ws-mixer-goserver-"));
  const binPath = path.join(tmpDir, process.platform === "win32" ? "goserver.exe" : "goserver");
  const build = spawnSync(GO_BIN, ["build", "-o", binPath, "./cmd/testserver"], {
    cwd: goDirResult.dir,
    stdio: "pipe",
    env: { ...process.env, GOTOOLCHAIN: "auto" },
  });
  if (build.status !== 0) {
    return {
      available: false,
      reason: `'${GO_BIN} build' of ${goDirResult.dir}/cmd/testserver failed: ${build.stderr?.toString().trim() || build.error?.message || "unknown error"}`,
    };
  }
  return { available: true, binPath };
}

const goResolution = resolveGo();
const skip = !goResolution.available;
const skipReason = goResolution.reason;

// The skip reason is already in the describe block's own title, but a
// default reporter's summary line often doesn't show suite titles for
// skipped suites -- print it once here too so `env -u GO npm test` (and any
// other run without a working `go`) still surfaces *why* on stdout/stderr,
// not just a silent skip.
if (skip) {
  console.warn(`[interop.test.ts] skipping: ${skipReason}`);
}

describe.skipIf(skip)(skip ? `interop: real Go server (skipped: ${skipReason})` : "interop: real Go server", () => {
  let child: ChildProcessWithoutNullStreams;
  const pendingEvents: Record<string, unknown>[] = [];
  let wakers: Array<() => void> = [];
  let addr = "";
  let client: MixerClient;

  function send(cmd: Record<string, unknown>): void {
    child.stdin.write(JSON.stringify(cmd) + "\n");
  }

  function wake(): void {
    const w = wakers;
    wakers = [];
    for (const fn of w) fn();
  }

  /** Scans (and removes) the first pending event matching predicate, waiting for new lines as needed. */
  async function nextEvent(predicate: (e: Record<string, unknown>) => boolean, timeoutMs = 10000): Promise<Record<string, unknown>> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const idx = pendingEvents.findIndex(predicate);
      if (idx !== -1) return pendingEvents.splice(idx, 1)[0]!;
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error(`timed out waiting for event matching ${predicate.toString()}`);
      await Promise.race([
        new Promise<void>((resolve) => wakers.push(resolve)),
        new Promise<void>((resolve) => setTimeout(resolve, remaining)),
      ]);
    }
  }

  beforeAll(async () => {
    if (skip) return;
    // The binary was already built by resolveGo() above (at module load,
    // before describe.skipIf ran), specifically so a build failure skips
    // the suite instead of failing beforeAll(). Spawning the built binary
    // directly (not `go run .`) means `child` IS the server, so killing it
    // actually kills it (no orphaned goserver processes left behind between
    // test runs -- `go run` would leave the compiled-binary grandchild it
    // spawns running).
    const binPath = goResolution.binPath!;

    child = spawn(binPath, [], {
      stdio: ["pipe", "pipe", "pipe"],
      // Its own process group, so afterAll can kill the whole group instead
      // of just this one process (defense in depth alongside building a
      // plain binary above: nothing here spawns further children today, but
      // this keeps it true if that ever changes).
      detached: process.platform !== "win32",
    });
    child.stderr.on("data", (d) => process.stderr.write(`[goserver] ${d}`));
    const rl = createInterface({ input: child.stdout });
    rl.on("line", (line) => {
      let e: Record<string, unknown>;
      try {
        e = JSON.parse(line) as Record<string, unknown>;
      } catch (err) {
        // A goserver bug (e.g. two goroutines racing on stdout) corrupting a
        // line used to be silently dropped here, which just looked like a
        // mysterious timeout on whatever event never arrived. Fail loudly
        // instead.
        throw new Error(`goserver emitted an unparseable line: ${JSON.stringify(line)} (${(err as Error).message})`);
      }
      pendingEvents.push(e);
      wake();
    });

    const listening = await nextEvent((e) => e.event === "listening", 30000);
    addr = listening.addr as string;
  }, 40000);

  afterAll(async () => {
    if (skip) return;
    try {
      await client?.close();
    } catch {
      // best effort
    }
    try {
      send({ cmd: "quit" });
    } catch {
      // process may already be gone
    }
    if (child?.pid) {
      try {
        // Negative pid: kill the whole process group (see the detached: true above).
        process.kill(process.platform === "win32" ? child.pid : -child.pid, "SIGKILL");
      } catch {
        child.kill("SIGKILL");
      }
    }
  }, 15000);

  it(
    "N streams round-trip, app round-trip, and immediate reconnect after drain",
    async () => {
      const url = `ws://${addr}/v1/tunnel`;
      const streamsSeen: MixerStream[] = [];
      client = await connect(url, {
        token: "test-token",
        agent: { sdk: "ws-mixer-js-interop-test", sdk_version: "0.0.0" },
        onStream: (stream) => {
          streamsSeen.push(stream);
          const chunks: Buffer[] = [];
          stream.on("data", (c: Buffer) => chunks.push(c));
          stream.on("end", () => {
            const req = Buffer.concat(chunks).toString();
            stream.end(Buffer.from(`resp:${req}`));
          });
        },
        onApp: (body) => {
          client.sendApp({ echo: body });
        },
      });
      send({ cmd: "wait_conn" });
      await nextEvent((e) => e.event === "connected");

      const N = 3;
      send({ cmd: "open_streams", count: N, prefix: "req" });
      const results: Record<string, unknown>[] = [];
      for (let i = 0; i < N; i++) {
        results.push(await nextEvent((e) => e.event === "stream_result"));
      }
      expect(results).toHaveLength(N);
      for (const r of results) {
        expect(r.ok).toBe(true);
        expect(r.response).toMatch(/^resp:req-\d$/);
      }
      expect(streamsSeen.length).toBe(N);

      // app round trip: server sends app -> client's onApp echoes it back -> server reports app_received.
      send({ cmd: "send_app", body: { hello: "world" } });
      send({ cmd: "wait_app" });
      const appEvent = await nextEvent((e) => e.event === "app_received", 15000);
      expect(appEvent.body).toMatchObject({ echo: { hello: "world" } });

      // drain: the client must reconnect immediately, and the server must see a fresh connection.
      // Run this twice in a row (blocker 3): a stale drainReconnectScheduled
      // flag left over from the first cycle would silently swallow whatever
      // the second cycle's own close/reconnect needed to do.
      let previousSession = "test-token"; // placeholder, overwritten before first use
      for (let cycle = 0; cycle < 2; cycle++) {
        const reconnected = new Promise<void>((resolve) => client.once("welcome", () => resolve()));
        send({ cmd: "drain" });
        await nextEvent((e) => e.event === "drain_sent", 15000);
        await reconnected;

        send({ cmd: "wait_conn" });
        const conn = await nextEvent((e) => e.event === "connected", 15000);
        expect(conn.session).toBeDefined();
        expect(conn.session).not.toBe(previousSession);
        previousSession = conn.session as string;
      }
    },
    60000,
  );
});
