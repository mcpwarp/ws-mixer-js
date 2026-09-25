# JS SDK design

Status: normative for this repo. The wire protocol this SDK speaks is specified in
[`ws-mixer-spec`'s `docs/WIRE.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md);
the behavioural requirements every client SDK (this one included) must meet are in
[`ws-mixer-spec`'s `docs/CLIENT-SDK.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/CLIENT-SDK.md).

**Package** `@mcpwarp/ws-mixer`. Node 20+ first.
WebSocket library: **[`ws`](https://github.com/websockets/ws)** on Node — the only serious option, and the
one whose `bufferedAmount` + `send(data, cb)` pair gives us the write-side backpressure of `WIRE.md` §2.6
rule 2.

**Browser support is not a goal for v1, and nothing in the design precludes it**: the in-band keepalive is
already browser-compatible, and the only blocker is that a browser cannot set the `Authorization` header —
solvable later as a negotiated relaxation (`hello.token` only), not a redesign. Keep the transport behind a
small interface so a `globalThis.WebSocket` backend can be dropped in.

Streams are Node **`Duplex`** streams (`stream.Duplex`), so `pipeline()`, `pipe()` and async iteration all
work. `duplex.end()` is `CloseWrite()`. A `toWeb()` helper returns `{ readable, writable }` for web-streams
consumers.

```ts
import { connect } from "@mcpwarp/ws-mixer";

const conn = await connect("wss://edge.mcpwarp.io/v1/tunnel", {
  token: process.env.MCPWARP_TOKEN!,   // string | (() => Promise<string>) — called fresh on every dial
  meta:  { mcpwarp: { v: 1, services: [{ id: "anki", name: "Anki MCP" }] } }, // -> hello.meta
  window: 256 * 1024,
  maxStreams: 64,

  // A stream arrived. `stream` is a Node Duplex.
  onStream: async (stream) => {
    const req = await parseRequest(stream);      // reads until EOF (peer sent CLOSE)
    try {
      const res = await callLocalMcpServer(req);
      await pipeline(res.body, stream);          // may stream for hours (SSE)
      stream.end();                              // CLOSE
    } catch (e) {
      stream.reset("INTERNAL_ERROR", String(e));
    }
  },

  // Opaque application control messages (mcpwarp's register/unregister live here).
  onApp: (body) => mcpwarp.handleApp(conn, body),

  // Server asked us to go away. The SDK already reconnects; this is for logging/UX.
  onDrain: ({ reason, deadlineMs, message }) =>
    log.info({ reason, deadlineMs }, message ?? "server draining"),

  // Every disconnect, recoverable or fatal. See CLIENT-SDK.md for the shape, and
  // README.md's "Disconnect reason shape" table for the full field-by-field detail
  // (errorName, closeReason — the peer's own close-frame reason, verbatim, distinct
  // from `message` — httpStatus, cause).
  onDisconnect: (reason: { phase: "dial" | "handshake" | "connected"; wsCode?: number;
                           errorCode?: number; closeReason?: string; httpStatus?: number;
                           fatal: boolean; message: string }) =>
    log.warn(reason, `disconnected during ${reason.phase}`),

  reconnect: { base: 1000, cap: 60_000, connectTimeout: 10_000, maxAttempts: Infinity, stableAfter: 10_000 },
});

conn.on("welcome", ({ session, publicUrl }) => log.info({ session }, publicUrl));
conn.on("error",   (e) => log.error(e));         // { code, name, message, streamId? }
conn.on("fatal",   (e) => { log.error(e); process.exit(1); });  // see the fatal set below
await conn.sendApp({ mcpwarp: { v: 1, op: "unregister", id: "anki" } });
await conn.close();                              // drain{client_requested}, 5 s grace, close 1000
// or, for an application-level reason instead of the default graceful drain (no drain, no grace
// period; one non-fatal report when a connection had actually been established this cycle):
// await conn.close({ code: ErrorCode.APPLICATION_CLOSE, message: "operator requested shutdown" });
```

| Event | Fires when |
|---|---|
| `welcome` | handshake complete; carries `session`, negotiated `window`/`maxStreams`/`pingInterval`, `meta` |
| `stream` | a stream was opened (the `onStream` option is sugar for this) |
| `app` | an `app` message arrived |
| `drain` | `drain` received; the SDK has already started reconnecting in parallel |
| `reconnecting` | `{ attempt, delayMs, cause }` |
| `close` | every close, recoverable or not — see README.md's disconnect-reason field table |
| `error` | non-fatal error; loud for 4001/4003/4004 (SDK bug) |
| `fatal` | the fatal set below — **no reconnect will be attempted** |
| `pong` | `{ id, rttMs }` — for the consumer's own metrics |

The fatal set (never retried; `connect()`/`client.start()` rejects if hit before any `welcome`):
close `4010` (`UNSUPPORTED`) in any phase; close `4011` (`UNAUTHORIZED`) *after* `welcome`; a rejected
token *before* `welcome` (HTTP `401`, `error{code:11}`, or a bare `4011` close) — fatal on the
**second** rejection when `token` is a provider function (the first gets one immediate refresh-retry,
see below), fatal on the **first** rejection when `token` is a static string; HTTP `403`/`404`; a
missing/mismatched subprotocol echo; a token provider that throws/rejects an UNMARKED error (a
`TokenUnavailableError`, directly thrown or reachable via `cause`, is instead treated like a failed
dial — non-fatal, normal backoff, see README.md's "Authentication" section); `reconnect.maxAttempts`
exhausted.
See README.md's reconnect-trigger table for the complete, authoritative list (it also covers every
non-fatal trigger: `drain`, `4012`/`1001`, `4013`'s one-shot retry, `4009`/connected-phase-`4014`'s
"start backoff at the cap" treatment, and HTTP `429`'s `Retry-After`).

Reconnect options: `{ base, cap, connectTimeout, maxAttempts, stableAfter }`, full jitter by default.
The attempt counter — and every once-only retry budget gated alongside it, namely `4013`
KEEPALIVE_TIMEOUT's one-shot immediate retry and the pre-`welcome` token refresh-retry above — resets
only once a connection has stayed up `stableAfter` ms after **its own** `welcome`, tracked per
connection (a `drain` hand-over's retired connection ending never resets or clears its replacement's
own timer), never on `welcome` itself. Default `10000`; `0` means no stability window at all (resets
on the very next tick after `welcome`, matching the pre-`stableAfter` behaviour) — note this differs
from `ws-mixer-go`, where `StableAfter: 0` instead means "unset, use the 10s default": a `0` config
value is not portable between the two SDKs. `drain` → immediate parallel reconnect with 0–2 s jitter
regardless of `base`. The full reconnect/backoff state machine is `CLIENT-SDK.md`'s normative
requirement for every client SDK, not restated here.

The JS SDK's "ignore and count" counters (unknown frame types, stale frames, duplicate pongs, refused
opens, protocol violations, bytes in/out) are exposed via `client.stats()`, but this SDK does not escalate
a sustained run of any of them to a connection error the way a Go server's `STREAM_LIMIT`/refused-OPEN
flood guard does — a client seeing repeated `STREAM_LIMIT` resets just keeps counting. That escalation is
left for a later revision.
