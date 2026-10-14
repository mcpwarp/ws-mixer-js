# The mcpwarp control plane, as this SDK implements it

Status: descriptive of `src/mcpwarp/*` and `src/http1/*` (0.10.1). The tunnel's source of truth is
`mcpwarp-saas/apps/tunnel` (`internal/agentconn`, `internal/reqpath`); the Go CLI
(`mcpwarp-tui-cli`, `internal/appproto`, `internal/relay`) is the reference agent. This SDK speaks
ws-mixer.v1 underneath ([WIRE.md](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md)).

## 1. Endpoint and routing

`wss://connect.<domain>`, default `wss://connect.mcpwarp.io`. The tunnel routes by `Host`; the path
is ignored. Subprotocol `ws-mixer.v1`.

## 2. Authentication

The token always goes in `hello.token`, and also in the upgrade's `Authorization: Bearer <token>`
header when the runtime can set one:

- a personal access token `mcpwarp_pat_<id>_<secret>` (no audience check), or
- a JWT whose audience is `https://connect.<domain>`.

A bad token: `error{code: 11 UNAUTHORIZED}` + close `4011` (fatal; with a token provider the SDK
retries once with a fresh token first; `expose()` rejects, or the handle closes, with
`McpwarpError` code `UNAUTHORIZED`, the transport's `WsMixerError` as `cause`). Backend unavailable: close `4002` (retryable). The SDK never
refreshes tokens and never reads the environment; pass a provider function if the token rotates.

The header is optional (ws-mixer spec v0.5.0, `D-2026-10-13-01`; mcpwarp tunnel 0.5.0):
`hello.token` is the authoritative credential. An upgrade without `Authorization` is accepted and
authenticated at `hello`; a header that is present must be a well-formed `Bearer <token>` (else HTTP
401) equal to `hello.token` (else `4011`). When the WebSocket factory reports that 401 via
`upgradeFailure`, `expose()` rejects with `UNAUTHORIZED` as well, the cause's `httpStatus` 401 (a
fatal 403/404 stays `DISCONNECTED_FATAL`, with its `httpStatus`). So runtimes whose WebSocket
cannot set headers (browsers, Electron renderers, Firefox chrome code such as Zotero 7 plugins)
connect with `webSocketFactory(ctor, { headers: false })`. Those runtimes also send an `Origin`
the app cannot control (`Origin: null` from Gecko chrome windows); the tunnel accepts any
`Origin`, since `hello.token` is the credential. The SDK never moves the token anywhere else (no
query parameter: WIRE.md section 2.1 forbids it).

Before any `101`, the tunnel may refuse the upgrade with HTTP `503` (`Retry-After: 5`; its per-pod
cap on sockets still waiting for `hello`) or `429` (`Retry-After`; per-client-IP rate limit). Both
are recoverable dial failures. A standard WebSocket exposes neither the status nor any response
header, so the SDK sees a bare `error`/close `1006` and retries with its own full-jitter backoff
(`reconnect.base`/`cap`), not the `Retry-After` value; `onRetry` is called for each retry. Only a
factory that sets `WebSocketLike.upgradeFailure` lets the SDK honour a `429`'s `Retry-After`.

## 3. Identity

Sent on every dial:

| Where | Value |
|---|---|
| `hello.meta` | `{ "client": "sdk-js", "version": "<version>", "os": "<os>", "integration": "<uuid, lowercased>" }`; `os` is detected (`<platform>/<arch>`, or `unknown`) unless `expose({ agent: { os } })` sets it |
| upgrade header `X-Mcpwarp-Integration` | the same lowercased UUID (secondary channel) |
| `hello.agent` | `{ sdk: "ws-mixer-js", sdk_version, runtime, os }`; `runtime` is detected (`node/22.x` \| `deno/2.x` \| `bun/1.x` \| `electron/x` \| `browser` \| `unknown`) unless `expose({ agent: { runtime } })` sets it, `os` as in `hello.meta` |

The tunnel reads `hello.meta.{client,version,os}` as informational (truncated to 100 bytes in
presence). It takes the integration id from `hello.meta.integration` first and the
`X-Mcpwarp-Integration` header second, using the first that is a UUID after trimming and
lowercasing; an invalid one is dropped silently (a debug log on the tunnel, the connection still
succeeds). Header-less runtimes send only the `hello` parts, which is why `hello.meta` comes first.
The SDK no longer sends `X-Mcpwarp-Client` (the tunnel never read it); `hello.meta.client`/`version`
carry the same thing.

`integration` is **required**: `expose()` throws a `TypeError` synchronously unless it is a UUID in
RFC 4122 text form, exactly the set zod's `z.uuid()` and the tunnel accept: `8-4-4-4-12` hex digits,
version nibble `1`-`8`, variant nibble `8`/`9`/`a`/`b`, any case, or the nil
(`00000000-0000-0000-0000-000000000000`) and max (`ffffffff-ffff-ffff-ffff-ffffffffffff`) UUIDs.
Braces, URNs, missing dashes and surrounding whitespace are rejected (the tunnel trims, the SDK does
not). It is sent lowercased. The reason for failing loudly: the tunnel would drop a bad id without
telling anyone and usage would go unattributed. It is the registration UUID of the integration (the
app or plugin embedding the SDK, not a particular install) and gates nothing on the tunnel.

## 4. Envelope reference

Control messages are ws-mixer `app` bodies on stream 0, `{"mcpwarp": {...}}`, `v` always `1`.

Agent -> tunnel:

```json
{"mcpwarp":{"v":1,"op":"register","services":[{"name":"notes","kind":"http"}]}}
{"mcpwarp":{"v":1,"op":"unregister","services":[{"name":"notes"}]}}
```

Tunnel -> agent:

```json
{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"notes","id":"srv_..","url":"https://notes-user.mcpwarp.io/mcp","created":true}],"errors":[{"name":"x","code":"CONFLICT","message":"..","id":".."}]}}
{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"notes","id":"srv_.."}]}}
{"mcpwarp":{"v":1,"op":"disable","id":"srv_..","reason":"disabled"}}
{"mcpwarp":{"v":1,"op":"enable","id":"srv_..","name":"notes"}}
{"mcpwarp":{"v":1,"op":"conflict","id":"srv_..","name":"notes","reason":"taken_over"}}
{"mcpwarp":{"v":1,"op":"error","code":"OVERLOADED","message":".."}}
```

Decoding (`decodeEnvelope`): a body without an object `mcpwarp` key is not ours (ignored); `v != 1`
or an unknown op decode as `unknown` (ignored); a known op with wrong field types is
`MALFORMED_ENVELOPE` (a non-fatal `error` event). Defaults: `services`/`errors` `[]`, `url` `""`,
`created` `false`, error `message` `""`, `disable.reason` `"disabled"`, `conflict.reason`
`"taken_over"` (`conflict.id` and `conflict.name` are required strings).

Service names: `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`, at most 30 characters (checked synchronously by
`expose()`). Public URL: `https://<name>-<username>.<domain>/mcp`, taken from `registered.services[].url`.

`registered.errors[].code`:

| Code | SDK reaction |
|---|---|
| `SERVER_DISABLED` (carries `id`) | state `disabled`, `disabled` event; requests get 503 |
| `INTERNAL`, `NO_RESPONDERS`, `TIMEOUT`, `ABORTED`, `CONNECTION_LOST`, `MALFORMED_REPLY`, `UNAVAILABLE`, `SERVICE_ERROR` | transient: non-fatal `error`, re-register on the same connection after 1 s, 2 s, 4 s, ... capped at 30 s |
| `INVALID_NAME`, `USERNAME_REQUIRED`, `QUOTA_EXCEEDED`, `CONFLICT`, `BAD_REQUEST`, `UNAUTHENTICATED`, `FORBIDDEN`, `NOT_IMPLEMENTED`, others | before the first successful registration: **fatal** (`expose()` rejects, the handle closes). After it: non-fatal `error`, retried at the next welcome (`createTunnel()` treats the refusals among them, all but `BAD_REQUEST`/`NOT_IMPLEMENTED`/others, as fatal) |

`op: "error"` codes:

| Code | SDK reaction |
|---|---|
| `UNSUPPORTED_VERSION` | fatal |
| `OVERLOADED` (more than 256 app messages in flight) | re-send the outstanding register/unregister once after 1 s (the CLI's overload pause) |
| `CONNECTION_LIMIT` (more than 10 connections per owner and pod) | non-fatal `error`; the tunnel follows with close `4014`, which backs off from the cap |
| `TOO_MANY_SERVICES` (more than 100), `UNKNOWN_OP`, `BAD_REQUEST` | non-fatal `error` |

## 5. Session state machine (`src/mcpwarp/session.ts`)

The tunnel keeps **no registration state across connections**; identity is (token subject, name),
and the same name registered from two connections means the last register wins; since tunnel 0.5.0
the loser is told with `conflict` (section 9).

| State | Event | Action / next state |
|---|---|---|
| any but closing/closed/conflict | `welcome` (first connect, reconnect, drain replacement) | send `register` -> `registering` |
| any but closing/closed/conflict | `conflict` with our name or our id | `conflict` (sticky), `conflict` event `{serverId, name, reason}`, timers and the outstanding op dropped; before `expose()` has settled (the `conflict` overtook the first `registered`): **fatal** instead, `expose()` rejects with `McpwarpError` `TAKEN_OVER` and the client closes |
| `conflict` | `welcome`, disconnect, `enable`, `disable`, `registered` | ignored: no register, state stays `conflict`; inbound streams are RESET `CANCEL` |
| `registering` | `registered` with our name | `registered`, `registered` event `{url, serverId, created}`, also kept as `handle.registration` |
| `registering` | error `SERVER_DISABLED` | `disabled` |
| `registering` | transient error | stay, retry with backoff |
| `registering` | other error | fatal before the first registration, else non-fatal |
| `registered`, `registering` | `disable` (our id, or no id yet) | `disabled`, `disabled` event |
| any but conflict | `enable` with our name | `enabled` event, send `register` -> `registering` |
| any but closing/closed/conflict | disconnect leaving no active connection | `connecting`, retry timers cancelled (the end of a drain-superseded connection, or of the drain's parallel dial, does not count: the registration lives on the active connection) |
| any | `close()` | `closing`: if a register went out on the live connection (never after `conflict`), send `unregister` and wait for `unregistered` (2 s budget; any `unregistered` ends the wait, since the tunnel answers `services: []` for a name this connection does not hold); then the transport closes (`drain{client_requested}`, up to 5 s grace, close 1000) -> `closed` |

Before the first registration, `expose()` retries non-fatal failures (dial errors, refused upgrades,
transient register errors) without limit unless `reconnect.maxAttempts` is set. `onRetry({attempt,
delayMs, reason})` is called before each scheduled retry, and aborting `signal` rejects `expose()`
with an `AbortError` and closes the client (after the first registration it closes the handle).

For long-lived host integrations whose settings change at runtime (a plugin toggling the tunnel on
and off, a rotated token, a renamed service), use `createTunnel()` instead of calling `expose()`
directly: it restarts the exposure only when the config changed, serializes the restarts and
reports one `status` (`off`/`connecting`/`online`/`disabled`/`conflict`/`error`).

## 6. Data plane

- The edge opens **one stream per inbound HTTP request** and writes it as raw HTTP/1.1 in Go
  `Request.Write` shape: `Host` is the public host, hop-by-hop headers and `Authorization` are
  stripped, `Expect: 100-continue` is **not** stripped, the body is Content-Length or chunked. It
  then half-closes (CLOSE).
- **The app never sees the caller's `Authorization` header.** The tunnel verifies the public
  caller's token itself and deletes `Authorization` before forwarding (`stripHopByHopHeaders` in
  `apps/tunnel/internal/reqpath/handler.go`), so an app's own bearer-token auth cannot be enforced
  behind the tunnel: a check that requires the header rejects every request, and one that treats a
  missing header as anonymous lets everything through. The SDK adds
  `x-forwarded-host` (the public host) and `x-forwarded-proto: https` for `target` and `server`.
- The agent reads the head (64 KiB cap, 10 s deadline, else RESET CANCEL), then the body. It strips
  `Expect` and never sends a 1xx (the edge treats any response head as final).
- Request body cap: `maxRequestBodyBytes` on `expose()`, default 4 MiB, applied by every adapter
  (`handler`, `target`, `server`) before the app sees the request; above it the SDK answers `413`
  itself. The app's own limits only matter below the SDK's: with the default, an
  `express.json({ limit: "8mb" })` never sees a body over 4 MiB, so raise `maxRequestBodyBytes` to
  match when the app accepts more. `handler` and `target` read the whole body into memory before
  calling the app, so the cap also bounds per-request memory.
- The response is written raw HTTP/1.1: Content-Length when the local response declares one, else
  chunked, one chunk per body read (SSE flushes event by event), then CLOSE. A body shorter than its
  Content-Length, or one that errors mid-stream, is RESET (`INTERNAL_ERROR`; the error goes to the
  handle's `error` event). The `./node` adapter removes Node's own `Connection`/`Keep-Alive`
  response headers, answers a head over 64 KiB with `431` and a body over the cap with `413`
  (`Content-Length` is checked up front; a chunked body is cut off once the raw request passes the
  cap plus 64 KiB: `413` if the app has not started its response, else RESET `CANCEL`).
- Error replies (JSON `{"error": "..."}`): `400` malformed request, `413` body too large, `431` head
  too large, `500` the integration's handler failed, `502` local target unreachable / no request
  handler, `503` disabled or closing, `504` local target sent no headers within `targetTimeoutMs`
  (default 30 s; also without an `AbortController` global, where the fetch itself is left to
  settle), `404` (`{"error":"not_found"}`) any path other than `/mcp` (below).
- Only `/mcp` is forwarded. The public URL is `handle.url` and nothing else is reachable
  (`handle.path` is `"/mcp"`, for logging). Every adapter (`handler`, `target`, `server`) compares
  the request path (the raw request-target without query and fragment; for absolute-form, the part
  after the authority; never normalised, so dot-segments, `\` and percent-encoding do not match;
  under `server` Node's parser answers a `\` with `400`) exactly and case-sensitively against
  `/mcp` (`/mcp?x=1` matches, `/mcp/` and `/MCP` do not). `OPTIONS *` and authority-form targets
  are `400`. Any other request is answered `404` with only `Content-Type: application/json` and
  `Content-Length` (to `HEAD`: the same length, no body), right after the head is read: the handler, target or
  `http.Server` never sees it, and the rest of the body is read and dropped. The `404` takes
  precedence over the `503` (an unknown path is never served, whatever the state); `/mcp` while
  disabled or closing still gets `503`. Not configurable.
- The edge waits for the response head for its request timeout; the body streams until EOF. When
  the public client disconnects the edge RESETs the stream with CANCEL; the SDK then aborts the
  handler's `Request.signal` / the target `fetch`. On `disable` the tunnel RESETs open streams with
  CANCEL before sending `disable`.
- `Host` seen by the local server: `handler` gets the public URL; `target` gets the target's host
  plus `x-forwarded-host`/`x-forwarded-proto: https`; `./node` `server` gets `localhost` plus the same
  `x-forwarded-*`.

## 7. Drain

On `drain` the SDK dials a replacement in parallel. When the replacement welcomes, it re-registers
there (new requests are routed to it), while in-flight requests finish on the old connection, which
then closes itself (empty) or is cut at `deadline_ms` + 1 s; the tunnel's own deadline sweep RESETs
survivors and closes `4012`. Either end is reported once as a non-fatal disconnect.

A connection the tunnel is draining hands its services over silently: it never gets `conflict` for
the replacement's re-register.

## 8. Known limitations

- With a standard WebSocket the HTTP status and headers of a rejected upgrade are invisible:
  401/403/404/429/503 are retried as ordinary dial failures, without `httpStatus`, and `Retry-After`
  is not honoured (section 2). The tunnel's own token check at `hello` (`4011`) is still fatal.
- `conflict` is final for the handle; the SDK does not try to win the name back (section 9).
- Bun reports `bufferedAmount` as 0, so the SDK's send-side backpressure gate never engages there.

## 9. Tunnel 0.5.0 alignment (SDK 0.8.0)

What changed on the tunnel side, and how this SDK follows it:

- **`Authorization` header optional** (ws-mixer spec v0.5.0, `D-2026-10-13-01`). `hello.token` is
  authoritative; the header is sent whenever the runtime can set it. Header-less runtimes (browsers,
  Electron renderers, Zotero) can now connect (section 2).
- **Pre-hello gate.** The tunnel can answer the upgrade `503` (`Retry-After: 5`) or `429`
  (`Retry-After`) before any `101`. Both are recoverable dial failures, retried with the SDK's own
  backoff and reported through `onRetry`; a standard WebSocket cannot read `Retry-After` (section 2).
- **Integration id rule.** The tunnel reads `hello.meta.integration`, then `X-Mcpwarp-Integration`,
  and keeps the first that is a `z.uuid()`-form UUID after trimming and lowercasing; anything else is
  dropped silently. `expose()` therefore validates `integration` up front (`TypeError`) and sends it
  lowercased (section 3).
- **`X-Mcpwarp-Client` removed.** The tunnel never read it; `hello.meta.client`/`version` remain.
- **`conflict` op** (tunnel -> agent, unsolicited, stream 0):
  `{"mcpwarp":{"v":1,"op":"conflict","id":"<serverId>","name":"<name>","reason":"taken_over"}}`.
  Sent to the **old** holder when another connection of the same account registers the same name.
  The tunnel RESETs the old holder's streams for that server first, then sends the frame; on the
  same pod at register time, from another pod at that pod's next presence heartbeat (up to ~30 s
  later). A connection being drained never receives it. On receipt the session (matching our name,
  or our server id) goes to state `conflict` and emits `conflict { serverId, name, reason }`. The
  state is sticky for the handle: no register on later `welcome`s (reconnect, drain replacement) or
  on `enable`, and any stream that still arrives is RESET with `CANCEL` instead of being served (a
  `503` would claim the service is ours and merely unavailable; the tunnel has already moved it, so
  the request is cancelled as one the edge should not have routed here). `close()` still works and
  sends no `unregister`. Restarting the losing instance is left to a human or the app: an automatic
  re-register would just take the name back and start a ping-pong between two live instances.
  The tunnel records the holder before sending our `registered`, so a second install's register in
  that window can make `conflict` overtake our reply (the late `registered` is then ignored). If
  `expose()` has not settled yet, it rejects with a fatal `McpwarpError` code `TAKEN_OVER` (not
  `CONFLICT`, which is a register error code) and closes the client: a handle that could only ever
  `close()` is not worth returning, and switching resolve to reject later would be breaking.
