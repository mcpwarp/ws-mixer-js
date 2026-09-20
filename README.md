# @mcpwarp/ws-mixer

JS/TypeScript client SDK for `ws-mixer.v1`: N independent byte streams multiplexed over one
WebSocket connection. See [`ws-mixer-spec`'s `docs/WIRE.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md)
for the normative wire spec; this package is always the **answering peer** (client) -- it never
opens streams, only receives `OPEN` from the server and reads/writes/closes/resets what arrives.

See [`docs/DESIGN.md`](./docs/DESIGN.md) for this SDK's original design (package/library choice, Node
`Duplex` stream exposure, the event surface) — this README documents the SDK as actually built, which has
grown a fuller reconnect/disconnect-reason surface than the original design sketch; where they differ,
this README and the code win.

## Install

```bash
npm install @mcpwarp/ws-mixer
```

Requires Node >= 20. Zero runtime dependencies besides [`ws`](https://github.com/websockets/ws).

## Usage

```ts
import { connect } from "@mcpwarp/ws-mixer";
import { pipeline } from "node:stream/promises";

const client = await connect("wss://edge.mcpwarp.io/v1/tunnel", {
  token: process.env.MCPWARP_TOKEN!, // string | (() => Promise<string> | string) -- see "Authentication" below
  meta: { mcpwarp: { v: 1, services: [{ id: "anki", name: "Anki MCP" }] } }, // -> hello.meta

  // A stream arrived. `stream` is a Node Duplex.
  onStream: async (stream) => {
    const chunks: Buffer[] = [];
    for await (const chunk of stream) chunks.push(chunk as Buffer); // reads until EOF (peer's CLOSE)
    try {
      const res = await callLocalMcpServer(Buffer.concat(chunks));
      stream.end(res); // CLOSE
    } catch (e) {
      stream.reset(2 /* INTERNAL_ERROR */, String(e));
    }
  },

  onApp: (body) => mcpwarp.handleApp(client, body),
  onDrain: ({ reason, deadlineMs, message }) => log.info({ reason, deadlineMs }, message ?? "server draining"),

  reconnect: { base: 1000, cap: 60_000, connectTimeout: 10_000 },
});

client.on("welcome", (w) => log.info({ session: w.session }, "connected"));
client.on("fatal", (e) => { log.error(e); process.exit(1); }); // 4010 UNSUPPORTED / 4011 UNAUTHORIZED

await client.sendApp({ mcpwarp: { v: 1, op: "unregister", id: "anki" } }); // resolves once written (see caveat below)
await client.close(); // drain{client_requested}, 5s grace, close 1000
```

## Authentication

`token` is either a static string, or a provider callback (`() => string | Promise<string>`)
called **fresh on every dial** -- initial connect and every reconnect, never cached. This is the
shape to reach for whenever the token is short-lived (a Keycloak/OIDC access token, for example):

```ts
import { connect } from "@mcpwarp/ws-mixer";

let cached: { token: string; expiresAt: number } | null = null;

async function getToken(): Promise<string> {
  if (cached && cached.expiresAt > Date.now() + 5000) return cached.token;
  const res = await fetch(`${process.env.KEYCLOAK_URL}/realms/mcpwarp/protocol/openid-connect/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: process.env.KEYCLOAK_CLIENT_ID!,
      client_secret: process.env.KEYCLOAK_CLIENT_SECRET!,
    }),
  });
  if (!res.ok) throw new Error(`token refresh failed: ${res.status}`);
  const body = (await res.json()) as { access_token: string; expires_in: number };
  cached = { token: body.access_token, expiresAt: Date.now() + body.expires_in * 1000 };
  return cached.token;
}

const client = await connect("wss://edge.mcpwarp.io/v1/tunnel", { token: getToken, /* ... */ });
```

A provider that throws or rejects is **fatal**: no retry, and the thrown/rejected error is
surfaced verbatim as `DisconnectReason.cause` (`message` is copied from `error.message`).

**HTTP 401 on the upgrade gets exactly one refresh-retry** -- but only when `token` is a provider:
the SDK calls it again and redials immediately (no backoff, though the retry still counts toward
`reconnect.maxAttempts`). A second 401 is fatal. A static string `token` skips the retry entirely
and is fatal on the first 401, since there is nothing to refresh. HTTP 403/404 are always fatal,
with no retry, provider or not. This refresh-retry is specifically about the dial's own HTTP 401
response: a handshake-phase close carrying `UNAUTHORIZED` (`4011`), with or without a preceding
`error{}`, has no equivalent retry today and is simply fatal, the same as any other `4011`.

## Reconnect semantics

`connect()` resolves once the first `welcome` completes (and **rejects** if that never happens --
a fatal close/HTTP status, or `maxAttempts` exhausted before any connection ever succeeded). After
that, `MixerClient` owns a persistent reconnect loop, a single state machine
(`idle -> dialing -> connected -> backoff -> ...`, terminating in `closed`) driven entirely by
`src/client.ts` and OVERVIEW.md section 2.9:

| Trigger | Policy |
|---|---|
| Normal disconnect (1006, 5xx, timeouts, SDK-side protocol errors 4001/4003/4004, ...) | AWS "full jitter": `delay = random(0, min(cap, base * 2^attempt))`, base 1000ms, cap 60000ms. Attempt counter resets **only on `welcome`**, never on TCP connect or the 101. |
| `drain` received | New connection **immediately and in parallel** (jitter `random(0, 2000)ms` only), before the old socket closes. The old connection is torn down as soon as the replacement's `welcome` lands. If `reconnect.maxAttempts` is `0` (reconnecting disabled), no parallel connection is started: the conn is left alone, in-flight streams finish normally, and once the server's own deadline closes it with `4012` that close is reported once as a fatal disconnect (`"drained; reconnect disabled"`). |
| Close `4012` (`GOING_AWAY`) or `1001`, with no preceding `drain` | Same immediate-reconnect treatment as `drain` (unless it *is* the eventual close of a `drain` received with `reconnect.maxAttempts: 0`, per the row above). |
| Close `4013` (`KEEPALIVE_TIMEOUT`) | **One** immediate retry (no delay); if that retry itself fails to connect, falls back to normal backoff. |
| Close `4009` (`ENHANCE_YOUR_CALM`) | Backoff starts **at the cap**, not at `base`. |
| HTTP `429` during the dial, with a `Retry-After` header | Honoured verbatim (seconds or an HTTP-date) instead of the usual jitter. |
| HTTP `401` during the dial, with a token provider present, on this dial's first 401 | One immediate refresh-retry: the provider is called again and the dial redialed right away (no backoff, but counts toward `maxAttempts`). See "Authentication" above. |
| Close `4010` (`UNSUPPORTED`), `4011` (`UNAUTHORIZED`) -- in any phase, with or without a preceding `error{}`; HTTP 401/403/404 (after the one refresh-retry above, if it applied); missing/mismatched subprotocol echo; a token provider that throws/rejects | **Fatal.** Surfaced on the `fatal` event and via `onDisconnect({ ..., fatal: true })`; `close()` is issued; **never retried**. `connect()` rejects if this happens before any `welcome`. |
| Close `4014` (`APPLICATION_CLOSE`), connected phase | Same "start at the cap" treatment as `4009` above (WIRE.md section 2.9): it is by nature sent *after* `welcome` (the app accepted, then refused -- e.g. a per-account connection cap), and `welcome` already reset the attempt counter, so plain full-jitter backoff would redial roughly once a second forever instead of climbing. Applies to both the `error{14}+close` and bare-`4014` shapes. Never emitted by ws-mixer itself -- reserved for the application above to close a connection for its own reason (the reason text is in `error.message`/the WS close reason). See `close({ code, message })` below. A **handshake-phase** `4014` (before `welcome`) is not special-cased: it follows the ordinary handshake-failure path, where the attempt counter climbs normally. |
| `reconnect.maxAttempts` exhausted | Stops reconnecting. `connect()` rejects if it never connected once. |
| `close()` called while a dial is in flight | The in-flight socket is closed as soon as the dial resolves; nothing reconnects afterward. |

Also true regardless of trigger:

- On every replacement, the connection being retired (a `drain`-superseded connection, or one torn
  down by `close()` mid-handshake) is fully closed, and its timers and listeners go with it. Its
  retirement is not itself reported as a disconnect (the client stays connected throughout, via the
  replacement); every in-flight stream on it gets a stream-scoped `StreamError(CANCEL, "connection
  drained")`, not a connection-level error.
- **In-flight streams are lost on reconnect.** There is no resumption: stream ids, buffers and
  credit all restart from zero on the new connection. A handler must be able to tell "the
  response ended" (`end`/EOF) from "the tunnel died" (the stream's `error` event) -- this SDK
  does not paper over the difference; retry is the application's job.

### Disconnect reason shape

Every disconnect, recoverable or fatal, is reported the same way -- to `onDisconnect(reason)` and
the `'close'` event -- as **exactly one** `DisconnectReason` object. In particular, when a failure is
also the one that exhausts `reconnect.maxAttempts`, that's still a single report: `fatal: true`, the
exhaustion message, but carrying the underlying failure's `wsCode`/`errorCode`/`httpStatus`/`cause` --
never a first report for the failure followed by a second one for giving up.

| Field | Meaning |
|---|---|
| `phase` | `"dial"` (opening the socket / the auth handshake), `"handshake"` (`hello`/`welcome` after the socket opened), or `"connected"` (after `welcome`). Always present. |
| `wsCode` | The WebSocket close code, when a WS close occurred (including the SDK's own locally-generated code for a close it initiated itself, e.g. `4001` for a hello/welcome timeout). When derived from a ws-mixer error (this side's own error, or a peer's `error{code}`), this is always the semantic `4000+error_code` -- for a `code > 999` that differs from the `4002` actually sent on the wire (illegal WS close codes are clamped; see `errorCode`, which always keeps the real, unclamped value). When instead observed directly from a bare close frame with no preceding `error{}`, `wsCode` is exactly what was on the wire. |
| `errorCode` | The ws-mixer error code (OVERVIEW.md section 2.8), when one is known -- either because a ws-mixer `error{}` preceded the close, or because the close code itself is in the ws-mixer wire range (`4000`-`4999`). Absent for a token-provider throw/reject: that's an application error, not a wire error. |
| `errorName` | That error code's wire name (e.g. `"KEEPALIVE_TIMEOUT"`). |
| `httpStatus` | The HTTP status of the upgrade response, when the dial failed at the HTTP layer (401/403/404/429). |
| `fatal` | Whether the SDK will never reconnect after this (includes `reconnect.maxAttempts` exhaustion). |
| `message` | Human-readable description. |
| `closeReason` | The reason field of the close frame **received from the peer**, verbatim -- never this side's own outgoing reason. Absent or empty whenever no reason was received from the peer: an abnormal closure (no close frame at all), this side having initiated the close itself (a peer's echo carries no information and RFC 6455 doesn't require it to copy the reason), or the SDK closing on a peer's `error{}` without reading whatever close frame follows it (OVERVIEW.md section 2.7 allows "logs, surfaces and closes"). The human-readable text is in `message` for all of those cases instead -- consumers SHOULD prefer `closeReason` and fall back to `message`. |
| `cause` | The token provider's thrown/rejected error, when that's why the dial failed. |

`sendApp()` returns a `Promise<void>` that resolves once the frame is actually written to the
socket (OVERVIEW.md section 4), or rejects with the connection's terminal error if the connection
fails before it gets there -- `conn.ts`'s control queue carries a resolver per queued frame the same
way `sendData()`'s per-stream outbox already does for stream bytes.

### Application-initiated close

`close()` normally performs the default graceful shutdown shown above (`drain{client_requested}`,
a grace period, then close `1000`). Pass `{ code, message }` instead to close the connection for an
application-level reason (e.g. `ErrorCode.APPLICATION_CLOSE`, `0x0e`/WS close `4014` -- reserved for
this and never emitted by ws-mixer itself): `error{code, message}` on stream 0, then WS close
`4000+code` with `message` truncated to 123 UTF-8 bytes on a character boundary, then the socket --
no `drain`, no grace period, and no reconnect is scheduled (matches the default `close()`'s
one-report-then-done shape). `code` must be an integer in `[0, 999]` so `4000+code` is a legal WS
close code; anything else throws a `RangeError` synchronously, before either connection is touched.

```ts
await client.close({ code: ErrorCode.APPLICATION_CLOSE, message: "operator requested shutdown" });
```

### Errors

`WsMixerError` (and its `ConnError`/`StreamError` subclasses) is the single error type this SDK
throws or emits (`'error'`/`'fatal'`, and every `Promise` rejection). Its `wsCode`/`closeReason`
fields exist for the same reason as the identically-named `DisconnectReason` fields above -- set
only when this particular error was built from an actually-observed close frame (as opposed to a
locally-raised protocol violation), and under the same "never this side's own outgoing reason" rule
as `closeReason`.

## Tests

`npm test` (vitest) runs:

- `test/frame.test.ts`, `test/control.test.ts` -- every fixture under `{frames,control}` in a
  `ws-mixer-spec` checkout (see "Spec fixtures" below) is decoded/validated by this SDK's own codec
  and hand-written validator and checked against the fixture's expected outcome (`valid`/`wire_valid`),
  per `ws-mixer-spec`'s `spec/README.md`'s "SDK runtime validators assert `wire_valid`" rule. Skips
  with a reason if no spec checkout can be found.
- `test/sequence.test.ts` -- every `sequences/*.json` transcript with `role: "client"` is replayed
  against a real `MixerConn` over a deterministic fake transport (`test/helpers/fake-ws.ts`); the
  `role: "server"` transcripts are scripted from the Go server's perspective and are `it.skip`'d with
  that reason, since this SDK is client-only. Same spec-checkout resolution and skip behavior as above.
- `test/stream.test.ts`, `test/conn.test.ts`, `test/keepalive.test.ts`, `test/reconnect.test.ts` --
  unit coverage for the stream state machine, credit/WINDOW accounting, half-close, the
  control-priority + round-robin write scheduler (exact interleaving asserted against the fake
  transport), keepalive/dead-peer detection, and the reconnect backoff math.
- `test/interop.test.ts` -- `go build`s `cmd/testserver` from a `ws-mixer-go` checkout and drives it
  as a plain child process (not `go run .`, so killing the child actually kills the server, not a
  `go run` wrapper around it), connects this SDK as a real client over a real socket, and exercises a
  multi-stream request/response round trip, an `app` round trip, and an immediate reconnect after
  `Drain`.

  **Spec fixtures and the Go interop server now live in their own repos** (`ws-mixer-spec`,
  `ws-mixer-go`), not under this one. Both are resolved the same way: an env var override, else a
  fetched checkout at the pinned tag, else a sibling directory next to this repo.

  | | env var | fetched checkout | sibling fallback | pin file |
  |---|---|---|---|---|
  | spec fixtures | `WSMIXER_SPEC_DIR` | `.spec/spec` (`npm run fetch-spec`) | `../ws-mixer-spec/spec` | `spec.pin` |
  | Go interop server | `WSMIXER_GO_DIR` | `.goserver` (`npm run fetch-goserver`) | `../ws-mixer-go` | `goserver.pin` |

  `WSMIXER_SPEC_DIR` accepts either directory shape: this repo's own convention (the spec subdir
  itself, i.e. `$WSMIXER_SPEC_DIR/fixtures` exists directly) or `ws-mixer-go`'s convention
  (repo-root, i.e. `$WSMIXER_SPEC_DIR/spec/fixtures`) -- `test/helpers/spec-dir.ts` tries the former
  first, then falls back to `$WSMIXER_SPEC_DIR/spec`.

  Every one of `test/frame.test.ts`, `test/control.test.ts`, `test/sequence.test.ts` and
  `test/interop.test.ts` skips its suite with the resolution failure as the reason -- never fails or
  silently passes -- when none of the three resolve.

  Toolchain resolution for the Go build: `$GO` (a path to a `go` binary) is preferred if set, else
  whatever `go` is on `PATH`. Either way, the build runs with `GOTOOLCHAIN=auto`, so a resolved `go`
  older than `cmd/testserver`'s `go` directive transparently downloads a matching toolchain instead of
  failing -- `$GO` does not need to point at an exact-version match.

  Run just this one (needs a working `go` on `PATH`, or set `GO=/path/to/go`, and a resolvable
  `ws-mixer-go` checkout):

  ```bash
  npx vitest run test/interop.test.ts
  ```

## Counters

`client.stats()` (and `conn.stats()`) return "ignore and count" counters -- unknown frame types,
stale frames, duplicate pongs, refused opens -- for the sites that already tolerate and discard
those per the wire spec. Unlike the Go server, this SDK does not escalate a sustained run of any of
them (e.g. repeated `STREAM_LIMIT` refusals) into a connection error; that's left for later.

`bytesIn`/`bytesOut` count raw WS message bytes, header included, for every message sent or
received -- not just DATA payload bytes. This is not the same measurement as Go's
`BytesTransferred` metric, which counts payload only; don't compare the two directly.

## Build

```bash
npm run build       # tsup (dist/{index.js,index.cjs}) + build:types (dist/*.d.ts)
npm run build:types # plain `tsc --emitDeclarationOnly`, stripping @internal members
npm run typecheck   # tsc --noEmit, strict
npm test            # vitest run
```

Declarations are generated by a plain `tsc --emitDeclarationOnly` pass (per-module `dist/*.d.ts`,
not a single bundled file), not by tsup's own `dts` option: tsup 8.x's bundled-declaration step does
not honour `stripInternal`, so it was leaking every `@internal`-tagged member straight into the
public API surface. `tsconfig.json`'s `stripInternal: true` is the setting that actually matters;
`build:types` just repeats its flags on the CLI since `tsc -p` cannot mix a project file with
explicit entry points, and `src/index.ts` (not `test/**`) is the only entry point declarations are
needed for.

## Before this is published

While `@mcpwarp/ws-mixer` is unpublished (publishing is guarded by `scripts/check-registry.mjs`'s
`prepublishOnly` check, which refuses to publish anywhere but the public npm registry, not a
`"private"` field), a consumer (e.g. the mcpwarp
tunnel client) links against a built copy directly:

```bash
cd ws-mixer-js && npm install && npm run build   # produces dist/{index.js,index.cjs,index.d.ts,...}
```

```json
{
  "dependencies": {
    "@mcpwarp/ws-mixer": "file:../ws-mixer-js"
  }
}
```

then `npm install` in the consumer. `file:` dependencies are copied (not symlinked) by npm on
install, so **re-run `npm run build` here and `npm install` there** after every change -- there is
no live-reload across the `file:` link. Once this package is ready to publish for real, pick a
version and `npm publish` as usual; nothing else in this setup changes.
