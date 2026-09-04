# Decision log

Status: **local**. This file records only `ws-mixer-js` implementation decisions. Protocol decisions are
recorded in [`ws-mixer-spec`](https://github.com/mcpwarp/ws-mixer-spec) → `docs/DECISIONS.md`; the wire
protocol itself is normatively specified in
[`ws-mixer-spec`'s `docs/WIRE.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md). Each
local entry below links back to the spec entry it implements, where one exists.

## 2026-08-26

- **D-2026-08-26-05b** — WebSocket library: [`ws`](https://github.com/websockets/ws) on Node, not
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
