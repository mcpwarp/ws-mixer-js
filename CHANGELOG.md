# Changelog

All notable changes to `@mcpwarp/ws-mixer` (the JS/TypeScript client SDK) are documented here.

## 0.3.1 - 2026-09-20

- `DisconnectReason` gains `closeReason`: the reason field of the close frame *received from the
  peer*, verbatim -- never this side's own outgoing reason. Absent when this side initiated the
  close (a peer's echo carries no information), when no close frame was observed at all (an
  abnormal closure), or when the SDK closed on a peer's `error{}` without reading whatever close
  frame follows it. `message` is unchanged and still carries the human-readable text in every case.
- Fixed: a **handshake-phase** close now classifies (and, for `4010`/`4011`, goes fatal) from the
  ws-mixer wire code the same way a connected-phase one already did, covering two previously-wrong
  cases. (1) A bare close carrying a ws-mixer wire code before `welcome` previously reported
  `errorName: "INTERNAL_ERROR"` and `fatal: false` -- silently retrying forever against a server
  that will never accept the retry. (2) The normative pre-`welcome` auth-rejection shape
  (`error{code:11 UNAUTHORIZED}` + close `4011`, before `welcome` -- OVERVIEW.md section 3.4's
  Authenticate hook, `spec/fixtures/sequences/auth_failure.json`) was previously treated as a
  *local* protocol violation: the client replied with its own `error{PROTOCOL_ERROR}` (forbidden by
  WIRE.md section 2.7 -- a peer that receives `error` must never reply), closed with its own `4001`
  instead of `4011`, discarded the server's real reason, and retried forever. A revoked token is now
  what it should always have been: one fatal, non-retried disconnect. The client's own hello/welcome
  timeout now also reports its locally-generated `wsCode: 4001` (previously omitted).
- New public API: `MixerClient.close({ code, message })` (CLIENT-SDK.md's "Application close" row)
  performs `error{code, message}` + WS close `4000+code` (message truncated to 123 UTF-8 bytes on a
  character boundary) + close the socket, instead of the default graceful drain -- e.g. to close
  with the new `APPLICATION_CLOSE` code below. `code` must be an integer in `[0, 999]`, or `close()`
  throws a `RangeError` synchronously. `close()` with no arguments is unchanged. Fixed: `code`/
  `message` now also apply when `close()` is called while a `drain`-triggered replacement dial or an
  in-flight (pre-`welcome`) handshake is still outstanding -- both previously always sent
  `error{NO_ERROR, "client closing"}` regardless of what was passed.
- New named error code: `APPLICATION_CLOSE` (`0x0e`, WS close `4014`). Connection-level only, never
  emitted by ws-mixer itself -- reserved for the application above to close a connection for its
  own reason (see `close({ code, message })` above). A **connected-phase** `4014` gets `4009`'s own
  "start at the cap" backoff treatment (WIRE.md section 2.9), not plain full-jitter backoff: it is
  by nature sent *after* `welcome` already reset the attempt counter, so plain backoff would redial
  roughly once a second forever instead of climbing. A handshake-phase `4014` is unaffected (its
  attempt counter still climbs normally).
- Fixed: closing with an error code whose `4000 + code` isn't a legal WS close code (any
  `code > 999`, reachable off the wire whenever a peer sends `error{code >= 0x1000_0000}`) now sends
  WS close `4002` (`INTERNAL_ERROR`'s close code) on the wire instead of throwing inside `ws` and
  falling back to `terminate()` -- a bare abnormal closure (`1006`) that told the peer nothing at
  all. `errorCode` and the reported `wsCode` stay the real, semantic `4000+code` either way; only
  the bytes actually put on the wire are clamped (spec `WIRE.md` section 2.8 / decision
  `D-2026-09-20-03`).
- Fixed: `SDK_VERSION` -- sent on the wire in `hello.agent.sdk_version` and the dial's `User-Agent`
  header -- was hard-coded to `"0.2.0"`, two releases stale. Now kept in sync with `package.json`'s
  version by hand, enforced by `test/version.test.ts` so a future release bump that forgets it fails
  CI instead of silently drifting again.
- `conformance/adapter/adapter.mjs`: `disconnected` now always includes `phase`, and includes
  `close_reason` when the SDK reports one; `close` now forwards a non-zero numeric `code`/`message`
  to the new application-close API (`code: 0`/absent stays the existing graceful drain-then-close,
  so `graceful_close.json`'s WIRE.md section 2.10 step 14 drain sequence keeps being exercised); the
  RESET code-name table gained `14: "APPLICATION_CLOSE"`; `connect`'s `reconnect.baseMs`/
  `reconnect.capMs` (when positive) now override the SDK's default backoff `base`/`cap`, so a
  scenario needing a fast connected-phase `4014` at-cap reconnect isn't stuck behind the SDK's 60s
  default cap.

## 0.3.0 - 2026-09-04

Repo split from the `mcpwarp/ws-mixer` monorepo into its own repo,
`mcpwarp/ws-mixer-js`, per MIGRATION.md section 4.1.

- Licensed Apache-2.0 (previously `UNLICENSED` in the monorepo).
- The package now publishes to the public npm registry (`registry.npmjs.org`) instead of a
  private registry.
- Spec fixtures are no longer vendored from the monorepo's `spec/` directory;
  they are now fetched from `ws-mixer-spec` at the tag pinned in `spec.pin`
  (`npm run fetch-spec`, into the gitignored `.spec/`).

## 0.2.0

Per OVERVIEW.md section 4.0 ("Client SDK requirements") and the decision log entry dated
2026-08-27.

- `token` may now be a provider callback (`() => string | Promise<string>`), not just a static
  string. It is called fresh on every dial -- initial connect and every reconnect -- never cached.
- HTTP `401` on the upgrade now gets exactly one immediate refresh-retry when `token` is a
  provider: the SDK calls it again and redials right away, no backoff, though the retry still
  counts toward `reconnect.maxAttempts`. A second `401` is fatal. A static string `token` is
  unchanged: fatal on the first `401`.
- A token provider that throws or rejects is fatal; the error is surfaced verbatim via the new
  `DisconnectReason.cause` field.
- The disconnect reason reported to `onDisconnect` and the `'close'` event is now a `DisconnectReason`:
  adds `phase` (`"dial"` | `"handshake"` | `"connected"`), `errorName`, `httpStatus`, and `cause`
  alongside the existing `wsCode`/`errorCode`/`fatal`/`message`. Every disconnect -- not just fatal
  or maxAttempts-exhaustion ones -- is now reported, including a recoverable dial failure and a
  non-fatal handshake failure, which previously went unreported.
- Fixed: a disconnect that also exhausted `reconnect.maxAttempts` (or hit the dial-side 401
  refresh-retry ceiling) previously fired `onDisconnect` **twice** -- once for the failure, once
  for giving up. It's now exactly one report per disconnect, `fatal: true` with the exhaustion
  message, carrying the underlying failure's `wsCode`/`errorCode`/`httpStatus`/`cause`.
- Fixed: the dial's per-attempt failure context (phase, HTTP status, provider `cause`) is now
  threaded through each dial attempt instead of stored on shared mutable client fields, so a
  concurrent old-connection close can no longer clobber a new dial's own disconnect report.
- Fixed: an `unexpected-response` during the dial (HTTP 401/403/404/429) now drains and destroys
  the response, destroys the request, and terminates the socket -- `ws` skips its own cleanup once
  a listener is attached, and this SDK's listener wasn't doing that cleanup itself.
- A `drain` while `reconnect.maxAttempts` is `0` no longer starts a parallel reconnect the client
  is configured never to complete, and no longer treats the `drain` itself as an immediate fatal
  event either: per OVERVIEW.md section 2.9, the connection is left alone, in-flight streams finish
  normally, and only once the server's own deadline closes the socket with `4012` is that reported
  -- once -- as the terminal `fatal: true` disconnect (`message: "drained; reconnect disabled"`).
- Fixed: a superseded (drained) connection's own teardown, once its replacement's `welcome` lands,
  no longer fires a spurious `onDisconnect`/`'close'` -- the client is still connected throughout,
  via the replacement. Its in-flight streams now get a stream-scoped `StreamError(CANCEL,
  "connection drained")` instead of the connection-level error, via `MixerConn.fail()`'s new
  optional per-stream error override.
- `MixerStream.reset()` now actually sets `resetCode`, as its docs already promised. Added
  internal `abort()`, used for SDK-detected stream violations (mirrors a peer-sent RESET): unlike
  the app-facing `reset()`, `abort()` also emits `'reset'`, since in both cases the stream's owner
  didn't choose the teardown.
- Per OVERVIEW.md section 2.9's Drain and the 2026-08-27 decision log: an `OPEN` received above a
  drain's `last_stream_id` is now connection-fatal `PROTOCOL_ERROR` (WS close `4001`), not a
  stream-scoped `REFUSED_STREAM` -- the server already promised not to send one, so this is it
  breaking that promise, not a benign race. `stats().drainRefusedOpens` is renamed
  `stats().drainViolations` to match.
- New public exports: `TokenProvider`, `DisconnectReason`, `DisconnectPhase`, `DisconnectPayload`.

## 0.1.0

Initial release: `connect()`, `MixerClient`'s reconnect/backoff state machine (OVERVIEW.md section
2.9 -- full-jitter backoff, `drain`/`4012` handling, `4013`/`4009` special-cased retries, fatal-close
handling), `MixerConn` (handshake, keepalive, flow control, the control-priority + round-robin
write scheduler), `MixerStream` (`stream.Duplex`), and the frame/control wire codecs.
