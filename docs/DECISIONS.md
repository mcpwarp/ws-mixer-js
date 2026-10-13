# Decision log

Status: **local**. This file records only `ws-mixer-js` implementation decisions. Protocol decisions are
recorded in [`ws-mixer-spec`](https://github.com/mcpwarp/ws-mixer-spec) → `docs/DECISIONS.md`; the wire
protocol itself is normatively specified in
[`ws-mixer-spec`'s `docs/WIRE.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md). Each
local entry below links back to the spec entry it implements, where one exists.

## 2026-10-12

- **D-2026-10-12-01** — WHATWG `WebSocket` through an injectable factory; `ws` dropped. The transport
  talks to a `WebSocketLike` (the WHATWG subset it uses) created by `ConnectOptions.webSocket`, defaulting
  to the runtime's global `WebSocket` (headers passed where supported: Node >= 22, Deno, Bun). Backpressure
  is a `bufferedAmount` gate (1 MiB) after each send; an upgrade's HTTP status is only known when a factory
  sets `upgradeFailure`, so with the default backend a 401/403/404/429 upgrade is a retried dial failure
  (the mcpwarp tunnel's real token check happens at `hello`, `4011`, which stays fatal). Runtimes that
  cannot set upgrade headers send the token only in `hello.token` and identity only in `hello.meta`;
  nothing is moved into the URL (WIRE.md §2.1). Supersedes D-2026-08-26-05b. Engines: Node >= 22.
- **D-2026-10-12-02** — Streams are a platform-neutral promise API (`read`/`write`/`closeWrite`/`reset`/
  `close`/`refuse`/`closed`), modelled on the Go and Rust SDKs' streams (Rust's `closed()` and
  flush-then-CLOSE `close_write()`, Go's `Close()`); no events on streams. A Node `Duplex` exists only in
  `./node` (`toDuplex`), WHATWG streams via `toWebStreams`.
- **D-2026-10-12-03** — ESM only, with sub-path entries `.`, `./mcpwarp`, `./node`, `./testing` sharing
  esbuild chunks (one copy of each class, so `instanceof` holds across entries). Nothing reachable from
  `.`, `./mcpwarp` or `./testing` may import anything but relative paths; enforced on sources
  (`test/boundary.test.ts`, `tsconfig.neutral.json` without `@types/node`) and on dist
  (`scripts/check-neutral.mjs`); a Deno smoke run proves the neutral build runs on a second engine.
- **D-2026-10-12-04** — Drain hand-over keeps the drained connection until its in-flight streams finish
  or the drain deadline (+1 s) passes (WIRE.md §2.9), instead of cancelling them when the replacement
  welcomes; its end is reported once, non-fatal. Implements
  [`WIRE.md` §2.9](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md)'s "in-flight streams
  finish normally". ws-mixer-go still cancels at the replacement's welcome.
- **D-2026-10-12-05** — `./node`'s `server:` adapter hands each stream, as a `Duplex`, to a private
  `http.Server` (`emit("connection")`) that forwards `request` events to the user's server, so the app
  gets Node's own parser and real `IncomingMessage`/`ServerResponse`. The private server sets the
  undocumented `httpAllowHalfOpen = true` (else Node ends the socket when the edge's request CLOSE
  arrives, before the response) and `checkContinue`/`checkExpectation` listeners (else Node answers
  `Expect: 100-continue` with an interim response the edge would take as final). The user's server is
  never listened on or mutated.

## 2026-08-26

- **D-2026-08-26-05b** — Superseded by D-2026-10-12-01. WebSocket library: [`ws`](https://github.com/websockets/ws) on Node, not
  `globalThis.WebSocket`/browser-native. This is the JS half of
  [`D-2026-08-26-05`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/DECISIONS.md), which was
  originally one bullet ("Go server on `coder/websocket`; JS client on `ws`, streams exposed as Node
  `Duplex`") split into two independent implementation decisions — see
  [`D-2026-08-26-05a`](https://github.com/mcpwarp/ws-mixer-go/blob/main/docs/DECISIONS.md) for the Go half.

  `ws` is the only serious option on Node 20+, and its `bufferedAmount` + `send(data, cb)` pair gives us
  the write-side backpressure of
  [`WIRE.md` §2.6](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md) rule 2 for free.

  Browser support is not a goal for v1, and nothing in the design precludes it later: the in-band keepalive
  is already browser-compatible, and the only blocker is that a browser cannot set the `Authorization`
  header — solvable later as a negotiated relaxation (`hello.token` only), not a redesign. The transport is
  kept behind a small interface so a `globalThis.WebSocket` backend can be dropped in without touching the
  rest of the client.

  Streams are exposed as Node **`Duplex`** streams (`stream.Duplex`), so `pipeline()`, `pipe()`, and async
  iteration all work out of the box; `duplex.end()` is `CloseWrite()`. A `toWeb()` helper returns
  `{ readable, writable }` for web-streams consumers.
