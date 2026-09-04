# Changelog

All notable changes to `@mcpwarp/ws-mixer` (the JS/TypeScript client SDK) are documented here.

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
