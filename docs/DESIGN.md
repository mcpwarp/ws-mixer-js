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

  // Every disconnect, recoverable or fatal. See CLIENT-SDK.md for the shape.
  onDisconnect: (reason: { phase: "dial" | "handshake" | "connected"; wsCode?: number;
                           errorCode?: number; httpStatus?: number; fatal: boolean; message: string }) =>
    log.warn(reason, `disconnected during ${reason.phase}`),

  reconnect: { base: 1000, cap: 60_000, connectTimeout: 10_000, maxAttempts: Infinity },
});

conn.on("welcome", ({ session, publicUrl }) => log.info({ session }, publicUrl));
conn.on("error",   (e) => log.error(e));         // { code, name, message, streamId? }
conn.on("fatal",   (e) => { log.error(e); process.exit(1); });  // 4010 / 4011
await conn.sendApp({ mcpwarp: { v: 1, op: "unregister", id: "anki" } });
await conn.close();                              // drain{client_requested}, 5 s grace, close 1000
```

| Event | Fires when |
|---|---|
| `welcome` | handshake complete; carries `session`, negotiated `window`/`maxStreams`/`pingInterval`, `meta` |
| `stream` | a stream was opened (the `onStream` option is sugar for this) |
| `app` | an `app` message arrived |
| `drain` | `drain` received; the SDK has already started reconnecting in parallel |
| `reconnecting` | `{ attempt, delayMs, cause }` |
| `close` | `{ wsCode, errorCode, message }` — every close, recoverable or not |
| `error` | non-fatal error; loud for 4001/4003/4004 (SDK bug) |
| `fatal` | 4010 / 4011 / HTTP 401-403-404 / no subprotocol echo — **no reconnect will be attempted** |
| `pong` | `{ id, rttMs }` — for the consumer's own metrics |

Reconnect options: `{ base, cap, connectTimeout, maxAttempts, jitter }`, full jitter by default, counter
reset on `welcome`. `drain` → immediate parallel reconnect with 0–2 s jitter regardless of `base`. The full
reconnect/backoff state machine is `CLIENT-SDK.md`'s normative requirement for every client SDK, not
restated here.

The JS SDK's "ignore and count" counters (unknown frame types, stale frames, duplicate pongs, refused
opens, protocol violations, bytes in/out) are exposed via `client.stats()`, but this SDK does not escalate
a sustained run of any of them to a connection error the way a Go server's `STREAM_LIMIT`/refused-OPEN
flood guard does — a client seeing repeated `STREAM_LIMIT` resets just keeps counting. That escalation is
left for a later revision.
