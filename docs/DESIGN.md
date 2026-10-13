# JS SDK design

Status: normative for this repo. The wire protocol this SDK speaks is specified in
[`ws-mixer-spec`'s `docs/WIRE.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md);
the behavioural requirements every client SDK (this one included) must meet are in
[`ws-mixer-spec`'s `docs/CLIENT-SDK.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/CLIENT-SDK.md).
The mcpwarp layer on top (`./mcpwarp`) is described in [`MCPWARP.md`](./MCPWARP.md).

**Package** `@mcpwarp/ws-mixer`, ESM only, zero runtime dependencies. Node 22+ and any runtime with a
WHATWG `WebSocket`. Entry points: `.` (transport), `./mcpwarp` (`expose()`), `./testing`
(`FakeEdge`) are platform-neutral -- nothing reachable from them imports `node:*` or a package, enforced
by `test/boundary.test.ts`, `tsconfig.neutral.json` and `scripts/check-neutral.mjs`; `./node` holds the
Node-only adapters.

**WebSocket**: the transport talks to a `WebSocketLike` -- the subset of the WHATWG `WebSocket` it uses
(`readyState`, `protocol`, `bufferedAmount`, `binaryType = "arraybuffer"`, `send`, `close`,
`addEventListener`) -- created per dial by an injectable `WebSocketFactory` (`ConnectOptions.webSocket`).
The default factory uses the runtime's global `WebSocket`, passing `{protocols, headers}` only to a
constructor known to accept it (Deno's, Bun's, and Node's undici class; not a host-native constructor
under a Node-like `process`, such as an Electron renderer's Chromium `WebSocket`), else the protocols
alone. `webSocketFactory()` is overloaded so the DOM `WebSocket` type is accepted with `{ headers: false }`. A WHATWG socket has no per-send completion,
so write-side backpressure (`WIRE.md` §2.6 rule 2) is: hand one frame to `send()`, then wait while
`bufferedAmount` is above 1 MiB (polling 5 ms doubling to 50 ms) before the next. Message size is
enforced by the frame decoder (65 544 B, `FRAME_SIZE_ERROR`). A standard socket never exposes a rejected
upgrade's HTTP status; a factory may, through `WebSocketLike.upgradeFailure`.

**Header-less runtimes** (browsers, Zotero) work with `webSocketFactory(ctor, { headers: false })`:
they cannot send `Authorization`, so the token is only in `hello.token`, which ws-mixer spec v0.5.0
(`D-2026-10-13-01`) makes sufficient; the mcpwarp tunnel accepts it from 0.5.0. Never a token in the
query string (WIRE.md §2.1).

**Streams** are a small promise-based `MixerStream` (no Node or WHATWG stream dependency):
`read()` (`null` at clean EOF; credit returned as the app reads), async iteration, `write()`
(serialized, credit-gated), `closeWrite()` (flushes queued writes, then CLOSE), `reset()`, `close()`
(Go semantics: CLOSE if the peer finished sending, else RESET CANCEL), `refuse()`, and a `closed`
promise (`finished` / `reset{code, byPeer}` / `connection_lost`) that never rejects -- the shapes of the
Go and Rust SDKs' streams. `toWebStreams()` gives WHATWG streams; `toDuplex()` (`./node`) a Node `Duplex`.

```ts
import { connect, type MixerStream } from "@mcpwarp/ws-mixer";

const client = await connect("wss://connect.mcpwarp.io", {
  token: process.env.MCPWARP_TOKEN!,   // string | (() => Promise<string>) — called fresh on every dial
  meta: { client: "sdk-js", version: "0.8.0", os: "linux/x64", integration: "<uuid>" }, // -> hello.meta
  window: 256 * 1024,
  maxStreams: 64,

  // A stream arrived. Return quickly: handlers are delivered one at a time, in wire order.
  onStream: (stream) => void serve(stream),

  // Opaque application control messages (mcpwarp's register/unregister live here).
  onApp: (body) => mcpwarp.handleApp(body),

  // Server asked us to go away. The SDK already reconnects; this is for logging/UX.
  onDrain: ({ reason, deadlineMs, message }) => log.info({ reason, deadlineMs }, message ?? "server draining"),

  // Every disconnect, recoverable or fatal; see README.md's "Disconnect reason shape".
  onDisconnect: (reason) => log.warn(reason, `disconnected during ${reason.phase}`),

  reconnect: { base: 1000, cap: 60_000, connectTimeout: 10_000, maxAttempts: Infinity, stableAfter: 10_000 },
});

async function serve(stream: MixerStream) {
  const req = await parseRequest(stream);        // stream.read() until null (peer sent CLOSE)
  try {
    for await (const chunk of callLocalMcpServer(req)) await stream.write(chunk); // may stream for hours (SSE)
    await stream.closeWrite();                   // CLOSE
  } catch (e) {
    stream.reset("INTERNAL_ERROR", String(e));
  }
}

client.on("welcome", ({ session }) => log.info({ session }, "connected"));
client.on("fatal",   (e) => { log.error(e); process.exit(1); });  // see the fatal set below
await client.sendApp(encodeUnregister("anki"));  // from @mcpwarp/ws-mixer/mcpwarp
await client.close();                            // drain{client_requested}, 5 s grace, close 1000
// or: await client.close({ message: "operator requested shutdown" }); // APPLICATION_CLOSE, WS close 4014
```

| Event | Fires when |
|---|---|
| `welcome` | handshake complete; carries `session`, negotiated `window`/`max_streams`/`ping_interval`, `meta` |
| `stream` | a stream was opened (the `onStream` option is sugar for this) |
| `app` | an `app` message arrived |
| `drain` | `drain` received; the SDK has already started reconnecting in parallel |
| `reconnecting` | `{ attempt, delayMs, cause }` |
| `close` | every disconnect, recoverable or not — see README.md's disconnect-reason field table |
| `error` | non-fatal error; loud for 4001/4003/4004 (SDK bug) |
| `fatal` | the fatal set below — **no reconnect will be attempted** |
| `pong` | `{ id, rttMs }` — for the consumer's own metrics |

The fatal set (never retried; `connect()`/`client.start()` rejects if hit before any `welcome`):
close `4010` (`UNSUPPORTED`) in any phase; close `4011` (`UNAUTHORIZED`) *after* `welcome`; a rejected
token *before* `welcome` (`error{code:11}`, a bare `4011` close, or an HTTP `401` a factory reports via
`upgradeFailure`) — fatal on the **second** rejection when `token` is a provider function (the first
gets one immediate refresh-retry), on the **first** with a static string; HTTP `403`/`404` via
`upgradeFailure`; a missing/mismatched subprotocol echo; a factory that throws (e.g. no global
`WebSocket`); a token provider that throws/rejects an UNMARKED error (a `TokenUnavailableError`,
directly thrown or reachable via `cause`, is instead treated like a failed dial); `reconnect.maxAttempts`
exhausted. See README.md's reconnect-trigger table for the complete list.

Reconnect options: `{ base, cap, connectTimeout, maxAttempts, stableAfter }`, full jitter by default.
The attempt counter — and every once-only retry budget gated alongside it, namely `4013`
KEEPALIVE_TIMEOUT's one-shot immediate retry and the pre-`welcome` token refresh-retry above — resets
only once a connection has stayed up `stableAfter` ms after **its own** `welcome`, tracked per
connection, never on `welcome` itself. Default `10000`; `0` means no stability window at all — note
this differs from `ws-mixer-go`, where `StableAfter: 0` means "unset, use the 10s default". `drain` →
immediate parallel reconnect with 0–2 s jitter regardless of `base`.

**Drain hand-over** (`WIRE.md` §2.9: "in-flight streams finish normally"). When the replacement for a
drained connection welcomes, it becomes the active connection (new streams, `sendApp()`, `stats()`), and
the drained one is *retired*, not closed: it keeps answering pings and serving its in-flight streams.
It closes itself with `1000` once its stream table is empty (after flushing control frames already
queued, such as the last stream's CLOSE), or at the drain's `deadline_ms` + 1 s (30 s without one),
failing whatever is left with `StreamError(CANCEL, "connection drained")`; the server's own deadline
close `4012` usually comes first. However it ends, that end is reported once, non-fatal, and never
schedules a reconnect or touches the replacement's stability timer. `close()` ends retired connections
along with the active one.

The JS SDK's "ignore and count" counters (unknown frame types, stale frames, duplicate pongs, refused
opens, protocol violations, bytes in/out) are exposed via `client.stats()`, but this SDK does not escalate
a sustained run of any of them to a connection error the way a Go server's `STREAM_LIMIT`/refused-OPEN
flood guard does — a client seeing repeated `STREAM_LIMIT` resets just keeps counting. That escalation is
left for a later revision.
