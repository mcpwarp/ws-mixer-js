# Changelog

All notable changes to `@mcpwarp/ws-mixer` (the JS/TypeScript client SDK) are documented here.

## 0.7.0 - 2026-09-26

- A write issued in the same tick as the app's own `destroy()`/`reset()` (or `closeWrite()`) no
  longer puts its DATA on the wire after the stream's `RESET`/`CLOSE` and reports success: the chunk
  is never sent, and the write callback gets the stream's error exactly once --
  `StreamError(CANCEL)` for `destroy()`/`reset()`, `StreamError(STREAM_CLOSED)` after
  `closeWrite()`. DATA still queued when a stream is retired is now rejected the same way instead of
  being dropped (its write callback used to hang forever).
- A stream CLOSE'd in both directions (the peer's `CLOSE` plus the app's own `closeWrite()`/`end()`,
  in either order) is now destroyed once its read side has emitted `'end'` (and, after `end()`, once
  `'finish'` has fired), so `'close'` fires; previously it never did. `'end'` still precedes
  `'close'` when data is buffered. `reset()` on such a stream, before it gets there, now destroys it
  (discarding what is still buffered) instead of doing nothing.
- `closeWrite()` now fails a write that was waiting on send credit with `StreamError(STREAM_CLOSED)`
  instead of leaving it waiting for a `WINDOW` -- forever, once the stream was CLOSE'd both ways, not
  even `destroy()` settled it. The callback is called right away unless the peer's `CLOSE` has
  arrived and its data is still unread; then it is held like the writes in the next item.
- A failed write on a stream whose peer `CLOSE` already arrived is now held whatever this side's own
  write state: after `closeWrite()` it used to fail immediately, erroring the read side and losing
  the peer's buffered data (and `'end'`). The same now applies to a write issued after
  `closeWrite()`. A held write callback is settled once `'end'` fires, when the app calls
  `destroy()`/`reset()`, or when the connection is torn down with nothing left unread on the stream;
  while data is still unread it waits for `'end'` (or `destroy()`/`reset()`), even past the
  connection's teardown.
- A stream CLOSE'd both ways but not yet destroyed (its reader hasn't reached `'end'`) is now still
  reached by the connection's teardown: with nothing left unread it is destroyed there, so `'close'`
  fires and a held write callback settles. It keeps its clean end: the teardown's error is not
  applied to it (no `resetCode`, a held write keeps its own `STREAM_CLOSED` or socket error).
  Previously such a stream was never reached, and a write held on it hung (0.6.0 failed it
  immediately instead, losing the peer's buffered data and `'end'`). The connection tracks such
  streams only weakly, except while one owes a write callback: that one is held until its callback
  settles, even if the app dropped it. One the app dropped with no write pending (say, `end()`'d but
  never read) can still be garbage-collected on a long-lived connection, in which case its `'close'`
  listeners never run -- as in 0.6.0, where such streams were unreachable from the teardown
  entirely.
- On a drain hand-over, a write whose DATA was still queued behind another stream's in-flight write,
  or whose in-flight socket send failed after the hand-over, now fails with its stream's own
  `StreamError(CANCEL, "connection drained")`, the same error as `stream.errored`. Previously it got
  the superseded connection's `NO_ERROR`, or the raw socket error.
- Connection-death errors seen by a stream are now always a `ConnError` (`stream.errored`, its
  `'error'`, write callbacks, and `sendApp()` rejections): the socket's own close (previously a plain
  `WsMixerError`), a graceful `close()`'s `NO_ERROR` (likewise), and a raw `ws` send failure such as
  `Error("WebSocket is not open")` (previously leaked as-is) are wrapped, keeping the original as
  `cause` and copying `code`/`wsCode`/`closeReason`. `ConnError`/`WsMixerError` accept `cause`. A
  drain hand-over still gives `StreamError(CANCEL, "connection drained")`; README's "Errors" section
  now spells out which class means what.
- `conn.once(...)` listeners for `'stream'`/`'app'`/`'drain'` (and `events.once(conn, ...)`) were
  never detached: the ordered delivery loop called the unwrapped listener, so it fired again on every
  later event and leaked, pinning whatever it closed over (e.g. its stream) for the connection's lifetime.
  Fixed; they now fire once and detach.
- An incoming `RESET` message longer than 256 UTF-8 bytes is now clamped to 256 bytes on a character
  boundary (in `'reset'` and the stream's `StreamError`). WIRE.md section 2.3's limit is a
  sender-side SHOULD, so it stays tolerated, not a protocol error.
- Tests: `test/stream-matrix.test.ts`, a generated 600-cell matrix over the stream teardown seam
  (state x trigger x `'error'` listener x read buffer x pending write, including DATA queued behind
  a second stream's in-flight write, plus a no-reader variant for peer-CLOSE'd streams) through a
  real `MixerConn`, checking write callbacks settle exactly once and only report success for bytes
  on the wire, no DATA after `CLOSE`/`RESET`, buffered data and `'end'` before `'close'`, `'close'`
  exactly once and last, and the class and code of `stream.errored` and of any write-callback error
  per trigger.
- ci: publish workflow now runs typecheck + tests (test job, on Node 20 and 24) before npm publish; a failing tag no longer ships.
- Code comments now point to ws-mixer-go (`ws-mixer-go/wsmixer/...`) and ws-mixer-spec (`ws-mixer-spec/docs/research/...`) instead of the old monorepo paths.

## 0.6.0 - 2026-09-26

- A stream write that fails outside stream termination now reports that error to its write callback,
  so a promisified/awaited write rejects. Previously, with no `'error'` listener attached, the
  callback was called with no error — the write reported success even though its bytes were never
  sent.
  - On a stream whose peer CLOSE already arrived, a write whose socket send fails before the
    connection's teardown reaches the stream is held, the same way as 0.4.0's held writes: the
    buffered data, `'end'` and `'close'` are still delivered, and the callback is settled once the
    read side has emitted `'end'` (or the stream is destroyed first) — with the error of whatever
    tore the stream down first (connection teardown, or the app's own `destroy()`/`reset()`),
    otherwise the send's own error. This no longer depends on teardown reaching the stream, so it
    also holds after the app's own `closeWrite()`. If `'end'` had already fired, the callback gets
    the send's error immediately.
    With an `'error'` listener attached, 0.5.0 instead failed the callback immediately, which
    errored the read side and blocked `'end'`; that is fixed too.
  - Otherwise the callback gets the error immediately. This includes a write rejected after the
    app's own `closeWrite()`: for a stream with no `'error'` listener it now also marks the stream
    errored (`stream.errored` carries the error), same as before with a listener. If nothing is
    listening, the SDK's internal no-op `'error'` listener is attached first, so the failure never
    crashes the process.
- CI: a new `pair-go-js` conformance job runs the go->js pair matrix (`--mode pair`) against
  ws-mixer-go at `goserver.pin`, with a `go->js: 10` floor in `conformance/COUNTS.json` (all 10 pair
  scenarios pass at spec v0.4.0 / ws-mixer-go v0.6.0). `spec.pin` is v0.4.1 in this release, whose
  runner enforces COUNTS on `--sdk`/`--mode`-filtered runs, so the floor is live.
- Docs: nearly all comments (84 of 88 references) now cite the split spec docs (`WIRE.md` section
  2.x, `CLIENT-SDK.md` rows, and this repo's `docs/DESIGN.md`) instead of the old monorepo
  `OVERVIEW.md` section numbers. The remaining four point at material that no longer exists in any
  spec doc and are left as-is (`conn.ts`, `control.test.ts` ×2, `reconnect.test.ts`).
  Comment-only; no behaviour change.

## 0.5.0 - 2026-09-25

- **Breaking:** `MixerClient.close()`'s options no longer take a `code` — `close({message})` always
  closes the connection with `APPLICATION_CLOSE` (`0x0e`/WS close `4014`), never a caller-chosen
  code. WIRE.md section 2.8 makes `APPLICATION_CLOSE` the only code an application may close a
  *connection* with, so a caller-supplied code could previously close a healthy connection with a
  protocol-fault code and produce an `errorCode`/`errorName` pair outside that table (D-2026-09-25-01).
  Which path runs is chosen by whether `message` is present, not by any `code`: `close({message})`
  is the application close, and `close()`/`close({})`/`message: undefined` is the default graceful
  drain-then-close. Passing a `code` key at all (e.g. a plain-JS caller still on
  `close({code: 14})`, with or without `message`) now throws a `TypeError` synchronously, before
  any close is attempted, instead of silently taking either path. The `[0, 999]` `RangeError`
  validation is gone along with `code`.
- Conformance adapter: the `close` command's `code` now accepts only `0`/absent (graceful close) or
  `14` (application close via the new `close({message})`) — any other value replies with a
  command-error, `"close: only 0 or 14"`.

## 0.4.0 - 2026-09-25

- New `TokenUnavailableError` (exported from the package root): a token provider that throws or
  rejects with an instance of it (directly, or wrapped via `cause`) now gets treated like a failed
  dial instead of the fatal-by-default verdict every other provider throw/reject still gets — a
  non-fatal `{phase: "dial", fatal: false}` report, normal full-jitter backoff (it counts as a
  failed attempt: `reconnect.maxAttempts` applies, the stability rule is unaffected), and the
  provider's error surfaced verbatim as `cause`, same as before. Detection is `instanceof
  TokenUnavailableError` only, deliberately not duck-typed on any property or method, so an
  unrelated library's error can never accidentally turn a genuinely fatal provider failure into an
  endless retry loop. On the one-time pre-`welcome` refresh-retry's own provider call, a marked
  failure leaves the retry budget spent and takes this same non-fatal path rather than going fatal
  or granting a second refresh. On the very first connect, a marked failure no longer rejects
  `start()`/`connect()` — same as a first-dial network error.
- A pre-`welcome` token rejection -- an HTTP 401 on the upgrade, or a handshake-phase
  `UNAUTHORIZED` (`4011`, with or without a preceding `error{}`) -- now gets the same one-time
  refresh-retry treatment in *either* place, as ONE shared budget: when `token` is a provider
  function, the SDK calls it again and retries the dial immediately (no backoff); a second
  rejection, in either shape (`401` then `4011`, or `4011` then `401`), is fatal. A retry dial that
  instead fails for an unrelated, non-auth reason (a network error, an HTTP 5xx, a transport death
  before `welcome`) is *not* treated as a second rejection -- it takes the ordinary recoverable path
  (non-fatal report, normal backoff) -- but the budget stays spent regardless, so a genuine
  rejection on some later cycle, before the client ever goes stable, is still fatal with no further
  refresh. Previously only the dial's own HTTP 401 got this retry -- a handshake-phase `4011` was
  always immediately fatal, even with a token provider present. A **static string** `token` is
  unaffected: still fatal on the very first rejection of any of the three forms, since there is
  nothing to refresh (this was already true for HTTP 401 with a static token; it now also
  explicitly applies to the two handshake-phase forms, which previously had no retry concept at all
  to skip). Also fixed: this refresh-retry is now unconditional on `reconnect.maxAttempts`/
  reconnect being disabled (it applies to the very first connect too -- one immediate redial that
  completes the initial connection is not itself "a reconnect"), matching `ws-mixer-go`'s
  `dialAndHandshake`; it likewise does not itself increment the attempt counter.
- The refresh-retry budget above is a client-level flag (not local to one `connectOnce()` call), and
  re-arms only once the connection reaches stability (see `stableAfter` below), never merely on the
  next redial -- otherwise a server that welcomes and then closes shortly after could make the
  client hit the token endpoint on every single reconnect cycle forever.
- New `reconnect.stableAfter` option (default `10000`ms, next to `base`/`cap`/`connectTimeout`/
  `maxAttempts`; must be a finite number `>= 0`, or the constructor throws a `RangeError`
  synchronously, same as `close()`'s `code` validation -- `0` is legal and reproduces the pre-0.4
  "reset on `welcome`" behaviour): how long a connection must stay up past `welcome` before it's
  considered stable (WIRE.md/OVERVIEW.md section 2.9's `stable`). Fixed: the backoff attempt
  counter, and every once-only retry budget re-armed alongside it (4013 KEEPALIVE_TIMEOUT's one-shot
  immediate retry; the pre-`welcome` token refresh-retry above), now reset only once a connection
  reaches stability -- **not** on `welcome` itself, as before. Fixed alongside it: 4013's one-shot
  budget no longer un-spends itself on the very next 4013 (a leftover pre-`stableAfter` line reset
  the flag back to "unused" immediately after falling back to normal backoff, so four 4013s in a row
  with no intervening stable connection cycled immediate/backoff/immediate/backoff... forever instead
  of only the first ever being immediate). A server that welcomes and then immediately closes no
  longer resets backoff every cycle (which turned it into a redial-roughly-once-a-second loop
  instead of actually backing off): `4009`/`4014`'s own "start backoff at the cap" treatment is
  unaffected (still an explicit "refused on purpose, back off hard" rule, not a workaround for the
  counter's reset timing). Stability is tracked per connection: a `drain` hand-over's retired
  connection ending, however long after its replacement's own `welcome`, never resets or clears the
  replacement's own stability timer. `reconnect.maxAttempts`'s doc is updated to match: it now
  counts consecutive reconnect attempts *without* a stable connection in between.
- Fixed: `giveUp` (the `reconnect.maxAttempts`-exhaustion path) now mirrors `goFatal` -- it clears
  the stability timer and detaches-then-fails every live/in-flight conn (`conn`, `dialingConn`,
  `retiringConn`, which a drain hand-over can leave all pointing at the very same live connection)
  before reporting. Previously, exhaustion during (for example) a drain hand-over's parallel dial
  failing could leave the still-live predecessor connection running -- socket, ping/watchdog timers,
  and the now-orphaned stability timer all still ticking 10s past a client that already reports
  itself `closed`.
- Fixed: a transport failure between the 101 upgrade and `welcome` no longer fabricates a
  semantically-meaningless ws-mixer close code (`wsCode: 4002`, "as if" this side had itself closed
  with `INTERNAL_ERROR`) depending on whether `ws`'s `'error'` or `'close'` event happened to be
  delivered first. `'close'` is now the sole reporter for this window (measured against real `ws`
  8.21: a TCP reset/half-close/peer close frame delivers a bare `'close'` with no `'error'` at all;
  a protocol-level failure `ws` itself detects delivers `'error'` immediately followed by `'close'`
  a macrotask or more later -- `'close'` never races `'error'`, it always follows it, if it comes at
  all); `'error'` only records its message for `'close'`'s report to fall back on when the close
  itself carries no reason. The report now always carries the close code `ws` actually delivers
  (`1006` for a genuine abnormal closure, or whatever real close code it sent), never a fabricated
  one. If `'error'` fires and `'close'` never follows at all, nothing here waits on it: the existing
  hello/welcome timeout (`wsCode` `4001`) is unaffected and is what eventually reports it, as before
  -- deliberately the only fallback for that case. The connected phase was checked for the same
  class of bug and does not have it: an `'error'` there never produces a report by itself (see the
  `message` bullet below for the one real behaviour change to the connected phase's report shape).
- Fixed: a second `drain` on the same connection (before its first parallel reconnect resolves) no
  longer starts a second, redundant parallel dial -- mirrors `ws-mixer-go`, which already ignores a
  repeat drain the same way.
- `conformance/adapter/adapter.mjs`: `disconnected.error_name` now prefers the SDK's own
  `DisconnectReason.errorName` (which already derives the wire name from a bare 4xxx close code, not
  only from an explicit `error{}`) over re-deriving it from `errorCode` through the adapter's own
  smaller name table.
- A disconnect whose close frame carried no reason (from the peer, or a `ws`-level abnormal closure)
  now reports `message: "socket closed with code N"` instead of an empty string, in both the
  connected and handshake phases. Previously that fallback text only ever reached the internal
  `WsMixerError` used to reject in-flight sends and fail the handshake promise; the *emitted*
  `'close'` event (and so `DisconnectPayload.message`) carried the bare, possibly-empty close
  reason directly. A non-empty `message` is strictly more useful (and matches `ws-mixer-go`, which
  reports `"peer closed with code N"` for the same case) -- consumers that want "nothing, if the
  peer sent nothing" specifically should keep preferring `closeReason` over `message`, as the
  `closeReason` field's own doc already recommends.
- Confirmed (no behavior change): `MixerClient.close()`/`close({code,message})` already stop
  reconnecting in every state, and clear the stability timer alongside the existing backoff timer.
  Precisely what's reported: one non-fatal disconnect when a connection had actually been
  established for this cycle; nothing at all when none had (dialing, mid-handshake, or backing off)
  -- exactly like plain `close()` in those same windows, since there is no connection to send
  `error{}`/a WS close over in the first place.
- **Behavior change** (D-2026-09-20-09, cross-SDK alignment with `ws-mixer-go`): `errorCode`/
  `errorName` are no longer synthesised for anything that isn't a ws-mixer wire close code. An HTTP
  `401`/`403` upgrade rejection previously carried `errorCode: UNAUTHORIZED` -- it no longer does;
  `httpStatus` alone still identifies it (`404`/`429`/`5xx` were never affected, they never carried
  one). Conversely, a **connected**-phase bare close (no preceding `error{}`) in ws-mixer's private-use
  range `4001`-`4999` now derives both `errorCode`/`errorName` from the wire code the same way the
  dial/handshake phases already did -- previously only `errorName` was derived there, `errorCode`
  stayed `undefined`. The derivation itself is unchanged: `errorCode = wsCode - 4000`, `errorName`
  from the WIRE.md §2.8 table (unknown -> `INTERNAL_ERROR`), restricted to `4001`-`4999` (not `4000`,
  which is never legitimately on the wire -- `NO_ERROR` closes as `1000`). Also fixed as part of the
  same alignment: the `'fatal'` event's `WsMixerError.code` now follows this same rule instead of its
  own, separately-stale `info.errorCode` read -- for an HTTP `401`/`403` it is now `INTERNAL_ERROR`
  (no ws-mixer code exists for an upgrade rejection) where it used to be `UNAUTHORIZED`, and for a
  bare `4010`/`4011` close it is now the derived `UNSUPPORTED`/`UNAUTHORIZED` (previously
  `INTERNAL_ERROR`, mismatching the disconnect reason's own `errorCode` for that same close).
  Consumers that need to distinguish *why* a fatal happened should branch on the disconnect reason's
  `httpStatus`/`wsCode`/`errorCode`, not on the fatal error's `code` alone.
- **Fixed (blocker):** a stream `OPEN`, `app`, or `drain` event already queued for delivery when the
  connection ended could be silently dropped instead of delivered, whenever an async
  `onStream`/`onApp`/`onDrain` handler was still in flight at that exact moment (`error{}` is always
  the *last* message on the wire, so anything queued ahead of it genuinely arrived before the
  connection ended and is owed delivery — CLIENT-SDK.md's "Handler delivery" row). The delivery loop
  now finishes flushing whatever was already queued even after the connection has torn down, instead
  of exiting as soon as `this.closed` flips true out from under an in-flight `await`. **Behaviour
  change:** `onStream`/`onApp`/`onDrain` (and the `'stream'`/`'app'`/`'drain'` events) may now fire
  shortly after `MixerClient.close()`/`MixerConn.close()`'s own promise has already resolved, for an
  event that arrived before that close. A `drain` delivered this way never starts a reconnect for a
  connection that is no longer the live one: on an already closing/closed client the existing
  `!this.closing && this.state !== "closed"` guard already covered it, but a `drain` queued behind a
  blocked handler on a conn that then dies for an *unrelated* reason (its own 'close' handler already
  ran synchronously and replaced/cleared `this.conn`, and already scheduled the real reconnect) needed
  a further `this.conn === conn` guard -- without it, the flushed `drain` would still see
  `!this.closing && this.state !== "closed" && !this.drainReconnectScheduled` all true and spawn a
  second, parallel reconnect against the already-dead conn, latching `drainReconnectScheduled` and
  wrongly suppressing the next legitimate close-driven reconnect. Nothing can be newly enqueued once
  the connection is closed (defensive backstop, in addition to the architectural invariant that the
  read path always stops before teardown), so the flush is always bounded and terminates.
- **Fixed:** an open stream whose connection died abnormally (a `1006` tunnel death, or any
  connection-level failure) with no `'error'` listener attached used to end *cleanly* — `'end'`
  then `'close'`, with `stream.errored` never set — indistinguishable from the peer's own response
  legitimately finishing (CLIENT-SDK.md's "Stream teardown on disconnect" row; WIRE.md section 2.9
  requires the opposite: `io.ErrUnexpectedEOF`-equivalent, unless that stream's own `CLOSE` had
  already arrived). **Behaviour change**, and it now depends on whether the peer's own `CLOSE` for
  *that stream* had already arrived when the connection died, per WIRE.md's "`CLOSE` preserves
  buffered data; `RESET` discards it" rule:
  - `CLOSE` already arrived: the response DID complete, independent of the connection dying. Node's
    `push(null)` doesn't discard what's still buffered — it marks EOF, and Node delivers the buffered
    bytes to the consumer and only then emits `'end'`. `stream.errored` stays `null` and no `'error'`
    event fires. Only the **write** side fails: a write already pending, or started afterward, fails
    promptly (its callback receives the connection's error) instead of hanging or silently
    succeeding.
  - Otherwise (a peer `RESET`, or the connection ending any other way, with this stream's `CLOSE`
    never having arrived): the stream always ends with an error, never a false clean end —
    `stream.errored` carries it, `'close'` fires, and (for a consumer that *did* attach `'error'`)
    that listener receives it, but `'end'` is never emitted for this case; a read or write already
    pending, or started afterward, fails promptly. A pending write's callback now also receives the
    real error instead of being reported as having succeeded, in both cases above. A `RESET` always
    discards buffered data and errors, even if this stream's `CLOSE` had *also* already arrived
    (`RESET` always wins). A stream already fully, cleanly closed before the connection died (both
    directions' `CLOSE` already exchanged) is unaffected — it already ended cleanly and is no longer
    tracked by the connection at all. `MixerClient`'s class doc comment and README already described
    the intended behaviour; the code now actually matches it, with no remaining gap against
    CLIENT-SDK.md's "data already buffered ... still delivered first" rule.
- **Fixed:** a stream whose peer `CLOSE` had already arrived when the connection died -- the
  cleanly-ending case in the bullet above -- never actually emitted `'close'` (never got
  destroyed): `push(null)` delivered the buffered data and `'end'` fired, but nothing then called
  `destroy()`. It now does: buffered data delivered, `'end'`, then `destroy()`/`'close'`, same as
  a stream that ends any other way.
- **Fixed:** in that same state, a write already pending (or started afterward) had its error
  routed through Node's ordinary Writable error path, which also marks the readable side
  `errored` and permanently blocks `'end'` -- a consumer draining `data`/`end` on the stream could
  hang forever waiting behind a write it never awaited. Affected write callbacks are now held and
  only settled with the terminal error once the read side has finished delivering buffered data
  and emitted `'end'`, or the stream is destroyed some other way (app `destroy()`, `reset()`). An
  app that awaits a write's callback without ever reading this stream will wait until it does one
  or the other.
- **Fixed:** a fatal close (`UNSUPPORTED`/`UNAUTHORIZED`) on the retiring connection during a
  `drain` hand-over's parallel dial didn't cancel that in-flight dial: the client emitted
  `'fatal'` and then flipped back to `"connected"` once the dial's own `welcome` landed, and with
  `UNAUTHORIZED` it could also redial again via the pre-`welcome` token-refresh-retry loop. The
  dial is now cancelled (`NO_ERROR`/"client closing") as part of going fatal, and the client stays
  closed.
- `conformance/adapter/adapter.mjs` now reports the SDK's real `SDK_VERSION` in `ready`/`hello`
  instead of a stale hardcoded `"0.1.0"`.

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
