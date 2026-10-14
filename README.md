# @mcpwarp/ws-mixer

Expose a local MCP server at a public mcpwarp URL from inside your app: Node 22+, Deno, Bun, the
Electron main process, and Obsidian, Zotero and browsers through a header-less `webSocket` factory
(see [Runtimes](#runtimes)). Also a general `ws-mixer.v1` client: N independent byte
streams multiplexed over one WebSocket (see
[`ws-mixer-spec`'s `docs/WIRE.md`](https://github.com/mcpwarp/ws-mixer-spec/blob/main/docs/WIRE.md)).

| Entry point | What | Runtime |
|---|---|---|
| `@mcpwarp/ws-mixer/mcpwarp` | `expose()`: register one MCP server, serve its HTTP requests | any (platform-neutral) |
| `@mcpwarp/ws-mixer/node` | `expose({ server })` for an `http.Server`/Express app, `toDuplex()`; also re-exports all of `./mcpwarp` (`McpwarpError`, types, codes) plus `ErrorCode`, `WsMixerError`, `TokenUnavailableError`, `WebSocketFactory` | Node (not Bun) |
| `@mcpwarp/ws-mixer/testing` | `FakeEdge`: an in-process fake of the mcpwarp edge for your tests | any |
| `@mcpwarp/ws-mixer` | the ws-mixer transport: `connect()`, `MixerStream`, ... | any |

## Install

```bash
npm install @mcpwarp/ws-mixer
```

ESM only, zero runtime dependencies. Node >= 22 (global `WebSocket`); `require()` of the ESM build
works on Node >= 22.12.

## Quickstart

```ts
import { McpServer, createMcpHandler } from "@modelcontextprotocol/server";
import { expose } from "@mcpwarp/ws-mixer/mcpwarp";

const mcp = createMcpHandler(() => {
  const server = new McpServer({ name: "notes", version: "1.0.0" });
  // server.registerTool(...)
  return server;
});

const handle = await expose({
  token: process.env.MCPWARP_TOKEN!, // a personal access token, mcpwarp_pat_...
  name: "notes", // public URL slug
  integration: "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10", // required: your integration's registration UUID
  handler: mcp.fetch,
});
console.log(handle.url); // https://notes-<username>.mcpwarp.io/mcp
if (handle.registration?.created) console.log("first time this service was registered");

process.on("SIGTERM", () => void handle.close());
```

`expose()` resolves once the service is registered (or disabled from the dashboard: `state`
`"disabled"`, `url` undefined; the only case in which a resolved handle has no `url`) and rejects
with a fatal `McpwarpError` if the first connection or first registration fails fatally: a bad
token is `code: "UNAUTHORIZED"` (the transport's `WsMixerError` as `cause`), a quota or name problem
carries the register error code (`QUOTA_EXCEEDED`, `INVALID_NAME`, ...), and another instance
taking the name over before the first registration completes is `code: "TAKEN_OVER"`.

`handle.registration` is `{ url, serverId, created }` from the latest successful register, readable
right after `await expose()` (the first `registered` event fires before you hold the handle).
`created` is true only when that register created the service; re-registrations after a reconnect
or drain report `false`. It is undefined only when `expose()` resolved as disabled and the service
has not registered since.

Non-fatal failures before that (no network, a wrong `url`, an upgrade the tunnel refuses, a
transient register error) are retried forever by default, so `expose()` can stay pending. Bound or
observe it:

```ts
const handle = await expose({
  token, name: "notes", integration, handler: mcp.fetch,
  signal: AbortSignal.timeout(60_000), // or your plugin's unload signal
  onRetry: ({ attempt, delayMs, reason }) => console.warn(`mcpwarp retry #${attempt} in ${delayMs} ms: ${reason}`),
  // reconnect: { maxAttempts: 10 }, // also bounds the attempts before the first registration
});
```

Aborting `signal` before the first registration rejects `expose()` with an `AbortError` and stops
the client; aborting it later closes the handle like `handle.close()`. `onRetry` is called before
every scheduled reconnect (zero-delay redials included; a drain's replacement dial as `{ attempt: 0,
delayMs: <0-2 s jitter>, reason: "drain" }`; the one-shot token-refresh redial as `{ attempt: 1,
delayMs: 0, reason: "token-refresh" }`) and every re-sent register.

## Long-lived integrations

A plugin (Zotero, Obsidian, ...) whose tunnel settings live in a preferences pane should not juggle
handles itself: create one tunnel at startup and hand it the current settings whenever they change.

```ts
import { createTunnel } from "@mcpwarp/ws-mixer/mcpwarp";

const tunnel = createTunnel({
  integration: "0f6c2a8e-4c1b-4b7e-9a4e-2f1d9b7c3a10",
  handler: (req) => mcp.fetch(req),
  agent: { runtime: "zotero" },
  logger: (level, message) => console[level](`[mcpwarp] ${message}`),
});

// On startup and on prefs changes; unchanged settings are a no-op. Debounced, since a
// restart can take seconds and text fields change on every keystroke.
const applyPrefs = () =>
  tunnel.configure({ enabled: prefs.enabled, token: prefs.token, name: prefs.name || "zotero" });
await applyPrefs();
let debounce: ReturnType<typeof setTimeout> | undefined;
prefs.onChange(() => {
  clearTimeout(debounce);
  debounce = setTimeout(applyPrefs, 500);
});

// Status UI: called now and on every change.
const unsubscribe = tunnel.subscribe(({ state, url, code, lastError }) => renderStatus(state, url, code, lastError));

// On unload: unregister + drain, bounded.
clearTimeout(debounce);
unsubscribe();
await tunnel.dispose({ timeoutMs: 1500 });
```

`configure()` restarts only when `enabled`, `token`, `name`, `url`, `kind` or `webSocket` (by
reference, falling back to the one passed to `createTunnel()`) changed, serializes overlapping
calls (the last one wins) and resolves once the new connection is dialing; it does not wait for
`online`. A `webSocket` in the config overrides the one passed to `createTunnel()`, for hosts whose
socket source changes at runtime. A restart first waits for the old connection to unregister and
drain, which takes up to ~7 s while requests or SSE streams are open (MCP clients usually keep
one); the status is `connecting` meanwhile. Disabled or an empty token is `off` without dialing,
whatever the other fields hold. `configure()` never throws: a non-boolean `enabled`, or (while
enabled with a token) a field of the wrong type (an unset pref read as `undefined`, say), is
`error` with `INVALID_CONFIG`; an invalid name is `error` with `INVALID_NAME`. States: `off`, `connecting` (dialing, retrying, after a
disconnect), `online` (`url` set), `disabled` (from the dashboard), `conflict` (another instance
took the name over) and `error` (a fatal failure such as a rejected token). After `conflict` or
`error` the tunnel stays down until the next `configure()`, even with the same settings; `stop()`
does the same on demand. Call `configure()` on settings changes, not from a status listener:
re-applying the same settings after every `conflict` makes two instances take the name from each
other in turn. A ws-mixer `drain` with reason `replaced` is a take-over too (`conflict`, no
replacement dial); any other drain is a hand-over that keeps the status. `dispose()` is final; its
`timeoutMs` (a positive finite number, else a `TypeError`) can cut the drain short, leaving the old
connection to close in the background.

When something is wrong the status says why in two forms: `code`, one of `TunnelStatusCode` for
your logic, and `lastError`, the same in plain language to show users as is (`off` and a plain
`connecting` carry neither). Every `lastError` is a full sentence ending with a period, so you can
append it to your own text:

| `code` | state | `lastError` |
|---|---|---|
| `RETRYING` | `connecting` (a failed dial, or a register re-sent after a transient error or `OVERLOADED`, the very first one included) | "Can't reach mcpwarp; retrying." / "mcpwarp couldn't register the service; retrying." |
| `DISCONNECTED` | `connecting` (redialing after a lost connection, including a drained one that ended before its replacement got `welcome`) | "Connection lost; retrying." |
| `DISABLED` | `disabled` | "This service is disabled in the mcpwarp dashboard." |
| `TAKEN_OVER` | `conflict` | "Another connection took over this name." |
| `UNAUTHORIZED` | `error` (token rejected, or register refused as `FORBIDDEN`: the token may not publish this service) | "The token was rejected." / "The token is not allowed to publish this service." |
| `INVALID_CONFIG` | `error` (a field of the wrong type, an invalid `kind` or `reconnect` option) | "The service name is not set." / "The token is not set." / "The tunnel settings are invalid." |
| `INVALID_NAME` | `error` (the name fails the syntax or the 30-character limit, or the server refused it) | "The service name is invalid: use lowercase letters, digits and dashes (not at the start or end), at most 30 characters." |
| `USERNAME_REQUIRED` | `error` | "Choose a username in your mcpwarp account first." |
| `QUOTA_EXCEEDED` | `error` | "Your mcpwarp account has no services left in its quota." |
| `CONFLICT` | `error` | "The name is in use by another server." |
| `UNSUPPORTED_VERSION` | `error` | "This SDK version is no longer supported; please update." |
| `DISCONNECTED_FATAL` | `error` (a fatal close or transport failure, or `reconnect.maxAttempts` ran out) | "The connection to mcpwarp failed." |
| `INTERNAL` | `error` (incl. a register refused as `BAD_REQUEST`, `NOT_IMPLEMENTED` or a code this SDK does not know), or `connecting` for a non-fatal server error | "Something went wrong; the log has the details." |

A register the server refuses, with a listed code or one this SDK does not know, is fatal even on
a reconnect after an earlier success: nothing would retry it. An HTTP 401
at the upgrade is `UNAUTHORIZED` (a 403 or 404 `DISCONNECTED_FATAL`) only when the WebSocket
factory reports the status (`upgradeFailure`); a standard WebSocket hides it and the tunnel keeps
retrying. `RETRYING` already shows on the very first failed dial or register, before the tunnel
was ever online; after a lost connection the redials say `DISCONNECTED` instead. With
`reconnect.maxAttempts` set, running out of attempts (those before the first registration count
too) ends in `DISCONNECTED_FATAL`.

The technical message behind each goes to `logger`. At `error`: fatal failures as
`CODE: message` with the McpwarpError code (a failure without one has the status code, such as
`INVALID_NAME: <the rule>` for a name caught before dialing), and a refused config as
`INVALID_CONFIG: <which field is wrong>`, never echoing its value. At `warn`: retries as
`retry #N in MS ms: reason`, disconnects as `disconnected: reason`, and non-fatal errors as
`CODE: message` (a request handler's error has no code). Progress goes to `info`. Logger messages
carry no `mcpwarp: ` prefix and never the token; add your own tag.

`TunnelStatus` has no slot for reasons on the host's side (Zotero with no WebSocket-capable window
to take a factory from, say). Keep your own copy of the status for the UI: until the tunnel can be
configured, show `{ state: "off", lastError: <your text> }` from a host constant, then relay the
tunnel's status once you subscribe.

For plugin sandboxes: the controller itself needs no `AbortController` or `DOMException`. The SDK
needs `setTimeout`/`clearTimeout`, `Promise`, `URL`, `TextEncoder`/`TextDecoder` (`TextDecoder`
already when the module loads) and `Request`/`Response`/`Headers` (plus `fetch` for `target`);
`AbortController` is optional. The `webSocket` factory still has to come from somewhere (see
Runtimes).

### Status text

`describeStatus()` turns a status into short UI text in `en`, `de`, `es`, `fr`, `ja` or `zh-CN`
(`statusLocales`), so you need not translate states and codes yourself:

```ts
import { describeStatus } from "@mcpwarp/ws-mixer/mcpwarp";

tunnel.subscribe((status) => {
  const { label, detail } = describeStatus(status, { locale: globalThis.navigator?.language ?? "en" });
  renderStatus(label, detail); // detail is undefined when online or plainly off
});
// Override single strings, over the resolved locale:
describeStatus(status, { locale: "de", messages: { states: { error: "Problem" } } });
```

`label` is the state; `detail` the code's sentence, or `lastError` when there is no code (your own
host-side reasons included), and left out when empty or when it would repeat `label` (`disabled`,
`conflict`). So state `conflict` (`TAKEN_OVER`) has no detail, while state `error` with `CONFLICT`
does: "Name already in use". `INVALID_CONFIG` appends a colon (in French with a no-break space
before the colon and a space after it, `"："` in Japanese and Chinese, `": "` otherwise) and
`lastError`, which names the bad field and stays English in every locale ("Ungültige
Einstellungen: The service name is not set."); the generic "The tunnel settings are invalid." is
left out. A `lastError` equal to or starting with the localized sentence is shown alone. When the
two are joined, a `": "` inside `lastError` is rendered as `"; "`, so the detail has a single
colon; this applies only to `INVALID_CONFIG`, and a code-less `lastError` is returned unchanged.
`INVALID_NAME` shows its sentence alone, which states the name rule. The tables are ws-mixer-spec's
normative status-text data, string for string; the SDK's tests check them against it. A
locale resolves by exact tag (case-insensitive, `_` read as `-`), then by language (`de-AT` is
`de`), then to English; it never throws. Any `zh-*` (`zh-TW`, `zh-HK` and `zh-Hant` included) gets
the Simplified Chinese `zh-CN` table.

## Three adapters

Every public request becomes one ws-mixer stream carrying raw HTTP/1.1, served in the background
(a slow request never holds up another). Pick the adapter that matches your server:

### `handler`: a fetch-style function (any runtime)

`(request: Request) => Response | Promise<Response>`. The request URL is the public URL
(`https://notes-<user>.mcpwarp.io/mcp?...`). Fits MCP SDK v2's `createMcpHandler(...).fetch`, v1's
`WebStandardStreamableHTTPServerTransport.handleRequest` (a method, so wrap it:
`handler: (req) => transport.handleRequest(req)`), Hono, or a hand-written function. A
Zotero-style `handleMCPRequest(body) -> {status, headers, body}` wraps like this:

```ts
expose({
  token, name: "zotero", integration,
  handler: async (req) => {
    const r = await handleMCPRequest(await req.text());
    return new Response(r.body, { status: r.status, headers: r.headers });
  },
});
```

A handler that throws (or returns something that is not a `Response`) is answered with `500`; the
error is emitted on the handle's `error` event.

### `target`: an HTTP server you already run on localhost (any runtime with `fetch`)

```ts
expose({ token, name: "obsidian", integration, target: "http://127.0.0.1:27123/mcp" });
```

Requests to the public `/mcp` go to the target URL (target query parameters first, then the
incoming ones); no other path is forwarded (see "Only `/mcp` is forwarded"). `Host` becomes the
target's own (local servers, MCP SDK v1's DNS-rebinding protection included, validate it); the public host
and scheme arrive as `x-forwarded-host` / `x-forwarded-proto: https`. The SDK asks for
`accept-encoding: identity`, and a response that is compressed anyway is decoded by `fetch` and
re-served without `content-encoding`. No response headers within `targetTimeoutMs` (default
30000, at most 2147483647; only valid with `target`): `504`; target unreachable: `502`.

### `server`: an `http.Server` or `(req, res)` listener (Node, not Bun)

```ts
import express from "express";
import { expose } from "@mcpwarp/ws-mixer/node";

const app = express();
app.post("/mcp", (req, res) => { /* ... */ });
await expose({ token, name: "my-app", integration, server: app });
```

MCP SDK v1's Node transport:

```ts
import http from "node:http";
import { randomUUID } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { expose } from "@mcpwarp/ws-mixer/node";

const mcp = new McpServer({ name: "my-app", version: "1.0.0" });
const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: () => randomUUID() });
await mcp.connect(transport);
await expose({ token, name: "my-app", integration, server: http.createServer((req, res) => void transport.handleRequest(req, res)) });
```

The server is never `listen()`ed and never modified: each stream is handed as a socket to a
private `http.Server`, which passes real `IncomingMessage`/`ServerResponse` objects to yours via
its `request` event. `req.headers.host` is `localhost`, with the public host in `x-forwarded-host`.
Node's own `Connection`/`Keep-Alive` response headers are removed before the response reaches the
edge. Unlike `handler`/`target`, `server` streams the request body to your server as it arrives.
The body cap below is checked against `Content-Length` up front; a chunked request body is counted
as it is decoded, and the chunk that would take it past the cap never reaches your server: `413` if
your server has not written any response byte yet, else the stream is reset with `CANCEL` (or, if your response
was already complete, it simply ends). Either way, unless your response was already complete, your `req` is aborted (`'aborted'`, plus
`'error'` if you listen for it) and later `res` writes are dropped.

`server` needs Node's own `http` internals: the private server is fed each stream through
`emit("connection", socket)`. **It does not work on Bun**, whose `node:http` ignores sockets passed
that way, so requests would never be answered; `expose({ server })` from `./node` throws an `Error`
on Bun instead. On Bun, serve on localhost and use `target` (or a fetch-style `handler`).

### Only `/mcp` is forwarded

The public URL is `handle.url` and nothing else is reachable. Only requests whose path is exactly
`/mcp` reach your app (`handle.path`, for logging). The match is exact and case-sensitive on the
path alone: `/mcp?x=1` passes, `/mcp/`, `/MCP` and `/healthz` do not. Every other request is
answered `404 {"error":"not_found"}` by the SDK in all three adapters, before your handler, target
or server sees it. This is not configurable. The order is `404` (path) > `503` (disabled or
closing) > `413` (body over `maxRequestBodyBytes`) > your app: an unknown path is never served,
whatever the state, and the `404`/`503` go out right after the head without reading the body, so
an oversized `/mcp` request to a disabled service gets `503`, not `413`.

### What your server sees

- One HTTP/1.1 request per stream, only for `/mcp` (above): method, path + query, headers, body
  (Content-Length or chunked, up to `maxRequestBodyBytes`, default 4 MiB; `413` above it).
- The body cap is the SDK's and applies to all three adapters, so your own limit only matters below
  it: `express.json({ limit: "8mb" })` behind the default cap never sees more than 4 MiB of body.
  `handler`/`target` enforce it before your app is called (`413`). `server` streams: a chunked body
  reaches your server up to the cap and is then cut off (`413` if you have not answered yet, else
  RESET, see above). Raise `maxRequestBodyBytes` on `expose()` to match.
- No `Authorization` header: the edge verifies the caller's token and strips the header before
  forwarding, so **your app's own bearer-token auth cannot be enforced behind the tunnel**. The SDK
  adds `x-forwarded-host`/`x-forwarded-proto` (`target`, `server`). No `Expect` header and no
  hop-by-hop headers either.
- `Host` per adapter as described above.
- No interim (1xx) response is ever forwarded: the edge would treat it as the final one.
- Responses stream through: Content-Length when declared, else chunked with one chunk per body
  read, so SSE (`text/event-stream`) reaches the client event by event.
- Malformed requests are answered `400`, a request head over 64 KiB `431`, a head not received
  within 10 s is cancelled, any path other than `/mcp` `404`, and while disabled or closing every
  request to `/mcp` gets `503`.

## Authentication

`token` is a personal access token (`mcpwarp_pat_...`, e.g. from `process.env.MCPWARP_TOKEN`; the
SDK never reads the environment itself) or a JWT with audience `https://connect.<domain>`. It can
also be a provider, `() => string | Promise<string>`, called on every dial. The SDK does **not**
refresh tokens: a provider that needs refreshing does it itself. Throw `TokenUnavailableError` from
a provider for a temporary failure (it is retried with backoff instead of being fatal). A rejected
token ends the exposure (`error{UNAUTHORIZED}` + close `4011`, fatal); with a provider, the SDK
first redials once with a fresh token, which `onRetry` reports as `{ attempt: 1, delayMs: 0, reason:
"token-refresh" }`.

The token travels in `hello.token`, which is what the tunnel checks, and also in the upgrade's
`Authorization: Bearer` header when the runtime can set one (ws-mixer spec v0.5.0 makes the header
optional); never in the URL. See "Transport reference: authentication" below for the provider and refresh-retry rules.

## Identity

`integration` is required: your integration's registration UUID. It attributes usage to your
integration (not to a particular install). It must be a UUID in RFC 4122 text form, the same set
zod's `z.uuid()` accepts (`xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx`, version `M` 1-8, variant `N`
`8`/`9`/`a`/`b`, any case, or the nil/max UUID); anything else makes `expose()` throw a `TypeError`,
because the tunnel would drop it without telling anyone. It is sent lowercased in
`hello.meta = { client: "sdk-js", version, os, integration }` (what the tunnel reads first, and
the only copy header-less runtimes send) and in the `X-Mcpwarp-Integration` upgrade header.

## Lifecycle and events

`handle.state`: `connecting` -> `registering` -> `registered` (or `disabled`), back to `connecting`
on every disconnect, then `closing` -> `closed`. `conflict` is sticky: see below.

| Event | When |
|---|---|
| `registered` | `{ url, serverId, created }`, after every successful register (the service is re-registered after every reconnect: the edge keeps no state across connections); the latest one is also `handle.registration` |
| `disabled` | `{ serverId, reason }`: disabled from the dashboard; requests get `503` |
| `enabled` | `{ serverId }`: re-enabled; the SDK re-registers |
| `conflict` | `{ serverId, name, reason: "taken_over" }`: another connection of the same account registered this name and now owns it (below); `reason: "replaced"` for a ws-mixer drain that moved the name elsewhere (`serverId` is absent when none is known) |
| `error` | non-fatal: a failing handler or response body, a rejected re-registration, a transient register error (retried with backoff 1 s, 2 s, 4 s, ... up to 30 s), an exception thrown by one of your handle listeners |
| `disconnect` | every transport disconnect (`DisconnectPayload`, see below); the SDK reconnects itself |
| `close` | once, terminal: `{}` after `close()`, `{ error }` (a `McpwarpError`) after a fatal failure |

Two instances exposing the same `name` under one account: the newer register wins. The tunnel
RESETs the older instance's open requests and sends it `conflict` (right away on the same tunnel
pod, up to ~30 s later from another pod). The older handle goes to state `conflict` and stays
there: it stops serving (any request that still reaches it is RESET), and it never registers
again, not after a reconnect, a drain or an `enable`. The SDK does not fight for the name; restarting
the losing instance (or calling `close()` and `expose()` again) is a human's or the app's call, so
log or surface the event. `close()` still works and skips the unregister (the name is no longer
held). `url`/`serverId`/`registration` keep their last values, which now belong to the winner. A
`conflict` that overtakes the first `registered` reply makes `expose()` reject with a fatal
`McpwarpError` `TAKEN_OVER` instead (the client is closed; no handle, no `conflict` event).

A server-side drain is transparent: in-flight requests finish on the old connection while new ones
go to the replacement. `handle.close()` unregisters (2 s budget), then closes the connection
(`drain{client_requested}`, up to 5 s for in-flight requests, close 1000). `handle.client` is the
underlying `MixerClient` (stats, events).

## Runtimes

| Runtime | Status |
|---|---|
| Node >= 22, Deno, Bun | full: the global `WebSocket` sends the `Authorization` header |
| Electron (main) | as Node |
| VS Code desktop extension host | as Node: a plain Node process (no `window`), so use the default factory. Needs a VS Code whose Node has the global `WebSocket` (Node >= 22); on older builds the default factory fails fatally with a clear message |
| Obsidian / Electron renderer, VS Code for the web, browsers, Zotero 7 | need `webSocket: webSocketFactory(win.WebSocket, { headers: false })` (Zotero's plugin sandbox has no global `WebSocket`; use the main window's). These runtimes cannot set upgrade headers, so neither `Authorization` nor `X-Mcpwarp-Integration` is sent; the tunnel authenticates on `hello.token` and reads the integration from `hello.meta` (ws-mixer spec v0.5.0, mcpwarp tunnel 0.5.0). A refused upgrade (429, 503, ...) looks like a network failure and is retried, so pass `signal`/`onRetry` (above) |

The default factory passes `{ protocols, headers }` only to a `WebSocket` known to accept it
(Node's, Deno's, Bun's); an Electron renderer is detected as such even though it has `process`.
`webSocketFactory(ctor)` (headers) takes a constructor accepting that init object;
`webSocketFactory(ctor, { headers: false })` takes any standard one (`typeof WebSocket` from the DOM lib).
`webSocketFactory` is exported from the package root and also from `./mcpwarp` (and `./node`).

The detection guesses from the global `WebSocket`. The `ws` package's class (or a subclass of it)
is recognised as a non-native constructor with `ping`/`terminate` on its prototype and `ws`'s
statics (`createWebSocketStream`, `Server` or `WebSocketServer`), and is called as `new
WebSocket(url, protocols, { headers })`; this applies to a global replaced by `ws` and to
`webSocketFactory(ws)`. Bun's own `WebSocket` also has `ping`/`terminate` but is native and has none
of those statics, so it gets the `{ protocols, headers }` init object it reads headers from. A bound
or `Proxy`-wrapped global looks native, so under Node its headers are silently dropped: pass a
`webSocket` factory there.

`target`/`handler` need `Request`/`Response`/`Headers`/`TextEncoder` (all present in Zotero's
sandbox); `target` also needs `fetch`.

Browser-class runtimes always send an `Origin` upgrade header the app cannot control (Gecko chrome
windows, e.g. Zotero, send `Origin: null`). An edge that enforces same-origin on the ws-mixer endpoint
rejects that upgrade with 403, which the SDK sees as a generic connection failure and retries. The
mcpwarp tunnel accepts any `Origin` because the token in `hello` is the credential; if you run your
own ws-mixer edge, do the same (e.g. `OriginPatterns` in coder/websocket for a Go server).

Runtime/OS detection reads `process`, `Deno` or `navigator`; a sandbox with none of them reports
`unknown`. Pass `agent` to `expose()` to say what it is (sent in `hello.agent`, `hello.meta.os` and the
`User-Agent` where headers are sent), e.g. in Zotero `agent: { runtime: "zotero", os: Services.appinfo.OS }`.

## Testing your integration

```ts
import { FakeEdge } from "@mcpwarp/ws-mixer/testing";
import { expose } from "@mcpwarp/ws-mixer/mcpwarp";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const edge = new FakeEdge();
await edge.start();
const handle = await expose({ url: edge.url, webSocket: edge.webSocket, token: "test", name: "notes", integration, handler: mcp.fetch });

const client = new Client({ name: "test", version: "1.0.0" });
await client.connect(new StreamableHTTPClientTransport(new URL(edge.publicUrl("notes")), { fetch: edge.fetch }));
console.log(await client.listTools());

await client.close();
await handle.close();
await edge.stop();
```

`FakeEdge` runs in memory (no sockets): `edge.fetch` writes each request onto a ws-mixer stream as
raw HTTP/1.1 exactly like the real edge, and rejects any 1xx response. It records `upgrades`,
`hellos` and `appLog`, and can `disable(name)`/`enable(name)`, `failRegister(name, error)`,
`rejectNextUpgrade(status)`, `drain()`, `closeConnection()` and `appError()` to exercise the paths
your integration must survive. `fetch` needs global `ReadableStream`/`Response`.

## Transport reference

### `connect()`

```ts
import { connect, type MixerStream } from "@mcpwarp/ws-mixer";

const client = await connect("wss://connect.example.com", {
  token: process.env.TOKEN!, // string | (() => Promise<string> | string)
  meta: { app: "demo" }, // -> hello.meta
  onStream: (stream) => {
    void serve(stream); // return fast: see "Handler delivery"
  },
  onApp: (body) => console.log("app", body),
  onDrain: ({ reason }) => console.log("draining", reason),
  onDisconnect: (r) => console.warn(r.phase, r.message),
  reconnect: { base: 1000, cap: 60_000, connectTimeout: 10_000, stableAfter: 10_000 },
});

async function serve(stream: MixerStream): Promise<void> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of stream) chunks.push(chunk); // until the peer's CLOSE
  try {
    await stream.write(`got ${chunks.length} chunks`);
    await stream.closeWrite(); // CLOSE
  } catch {
    stream.reset("INTERNAL_ERROR", "failed");
  }
}

await client.sendApp({ hello: "world" }); // resolves once handed to the socket
await client.close(); // drain{client_requested}, 5 s grace, close 1000
```

`webSocket` (a `WebSocketFactory`) replaces the default `defaultWebSocketFactory` (the global
`WebSocket`, with headers where the runtime supports them). A factory can expose a rejected
upgrade's HTTP status as `socket.upgradeFailure`; a standard WebSocket never can.

`MixerClient`/`MixerConn` are small platform-neutral emitters with the same method names as Node's
`EventEmitter` (`on`/`once`/`off`/`emit`/`listenerCount`), and work with `events.once()`.

### `MixerStream`

| Member | |
|---|---|
| `read(): Promise<Uint8Array \| null>` | next chunk; `null` at clean EOF (peer CLOSE, buffer drained); rejects after a RESET or a lost connection. Credit is returned as you read. |
| `[Symbol.asyncIterator]` | `for await (const chunk of stream)` |
| `write(chunk: Uint8Array \| string): Promise<void>` | serialized, waits for send credit (16 KiB DATA frames); resolves once handed to the socket |
| `closeWrite(): Promise<void>` | half-close: refuses new writes, flushes queued ones, then sends CLOSE |
| `reset(code, message?)` | RESET both ways (`code` numeric or a wire name like `"CANCEL"`); discards buffers |
| `close()` | `closeWrite()` if the peer already CLOSE'd, else `reset("CANCEL")` |
| `refuse()` | `reset("REFUSED_STREAM")` |
| `closed: Promise<StreamCloseReason>` | never rejects: `{kind:"finished"}`, `{kind:"reset", code, name, message, byPeer}`, or `{kind:"connection_lost", error}`; buffered data stays readable |
| `getState()`, `getSendWindow()`, `getRecvWindow()`, `resetCode` | inspection |

`toWebStreams(stream)` returns `{ readable, writable }` WHATWG streams; `toDuplex(stream)` from
`@mcpwarp/ws-mixer/node` returns a Node `Duplex`.

### Handler delivery

`onStream`/`onApp`/`onDrain` are delivered in wire order from **one** delivery loop per connection,
one at a time; a returned promise is awaited before the next event. **Return quickly from
`onStream`** and serve the stream in the background: a handler that awaits the whole exchange
blocks every other stream.

An event already received when the connection ends is still owed to its handler (`error{}` is
always the last message on the wire), so a handler MAY fire shortly after `close()` resolved or
after `onDisconnect` reported the disconnect. A stream delivered that way has already ended: its
reads and writes fail promptly. A `drain` delivered that way never starts a reconnect for a
connection that is no longer the live one. A handler that throws or rejects is counted
(`stats().handlerErrors`), surfaced as `handlerError` on the conn, and never stops delivery.

### Transport reference: authentication

`token` is a static string or a provider called **fresh on every dial** (never cached):

```ts
async function getToken(): Promise<string> {
  const res = await fetch(`${process.env.AUTH_URL}/token`, { method: "POST" }).catch((e) => {
    throw new TokenUnavailableError("token endpoint unreachable", { cause: e }); // retried
  });
  if (!res.ok) throw new Error(`token refresh failed: ${res.status}`); // fatal
  return ((await res.json()) as { access_token: string }).access_token;
}
```

A provider that throws or rejects is **fatal** unless the error is (or has in its `cause` chain, up
to 10 links) a `TokenUnavailableError` -- detected by `instanceof` only, so import it from the same
installed copy of this package. A marked failure is treated like a failed dial (non-fatal report,
normal backoff).

A pre-`welcome` token rejection -- an HTTP `401` on the upgrade (only visible through a factory
that sets `upgradeFailure`) or a handshake-phase `4011` (`error{code:11}` + close, or a bare
close) -- gets **one** immediate refresh-retry when `token` is a provider; the second rejection, in
either shape, is fatal. A static string is fatal on the first. The budget re-arms only at stability
(below). HTTP `403`/`404` (again only via `upgradeFailure`) are always fatal.

### Reconnect semantics

`connect()` resolves on the first `welcome` and rejects if that never happens (fatal failure, or
`maxAttempts` exhausted first). After that `MixerClient` reconnects on its own. Phases, also in
`DisconnectReason.phase`: **`dial`** (token + WebSocket open), **`handshake`** (`hello` ->
`welcome`), **`connected`**.

| Trigger | Policy |
|---|---|
| Normal disconnect (1006, timeouts, 4001/4003/4004, ...) | full jitter: `random(0, min(cap, base * 2^attempt))`, base 1000 ms, cap 60000 ms. The attempt counter (and the once-only budgets below) resets only at stability, never on `welcome` itself. |
| `drain` received | new connection **immediately and in parallel** (jitter 0-2 s). Once it welcomes, new streams go there and the drained connection keeps serving its in-flight streams until they finish (then closes itself, `1000`) or `deadline_ms` + 1 s passes (its remaining streams end with `StreamError(CANCEL, "connection drained")`), whichever comes first; the server's own deadline close `4012` may come first. Its end is reported once, non-fatal, without another reconnect. With `maxAttempts: 0` no parallel connection is started; the server's `4012` is then reported once as fatal (`"drained; reconnect disabled"`). |
| Close `4012`/`1001` without `drain` | immediate reconnect (0-2 s jitter), counts toward `maxAttempts` |
| Close `4013` (`KEEPALIVE_TIMEOUT`) | one immediate retry, then normal backoff (once-only budget) |
| Close `4009` (`ENHANCE_YOUR_CALM`), connected-phase `4014` (`APPLICATION_CLOSE`) | backoff starts **at the cap** |
| `upgradeFailure` 429 with `Retry-After` | honoured verbatim |
| Pre-`welcome` token rejection, provider, budget unspent | one immediate refresh-retry (see above) |
| Close `4010` any phase; `4011` after `welcome`; a final token rejection; `upgradeFailure` 403/404; missing subprotocol echo; an unmarked provider throw; a factory that throws (e.g. no global `WebSocket`) | **fatal**: `fatal` event, `onDisconnect({fatal: true})`, never retried |
| `maxAttempts` exhausted | stop; one merged fatal report |

With a standard WebSocket a rejected upgrade (401, 429, 503, ...) is indistinguishable from a
network failure: it is a retried, non-fatal dial failure without `httpStatus`. The standard API
exposes no response headers either, so a `Retry-After` (the mcpwarp tunnel sends one with its
pre-upgrade `429` rate limit and `503` handshake cap) cannot be read: the SDK's own backoff
applies instead, and `onRetry` sees each attempt. The mcpwarp tunnel's real token check happens at
`hello` (`4011`), which is fatal everywhere.

**In-flight streams are lost on reconnect** (no resumption). A stream still open when its
connection ends delivers what was already buffered, then: a clean `null` if the peer's CLOSE had
arrived, otherwise a rejected read (`ConnError`); its `closed` reports `connection_lost`. A peer
RESET always discards buffered data.

### Stability and `reconnect.stableAfter`

`stableAfter` (default 10000 ms) is how long a connection must stay up past **its own** `welcome`
before the attempt counter, the `4013` one-shot retry and the token refresh-retry budget reset. A
server that welcomes and then disconnects can therefore not turn backoff into a tight loop. Must be
a finite number `>= 0` (`RangeError` from `connect()` otherwise; `expose()` throws a `TypeError`
and `createTunnel()` reports `error`/`INVALID_CONFIG`); `0` resets on the next tick. Differs from
`ws-mixer-go`, where `StableAfter: 0` means "use the default".

### Disconnect reason shape

Every disconnect is reported exactly once to `onDisconnect(reason)` and the client's `close` event.

| Field | Meaning |
|---|---|
| `phase` | `"dial"`, `"handshake"` or `"connected"` |
| `wsCode` | the WebSocket close code, when a close occurred; the semantic `4000+error_code` when derived from a ws-mixer error (an illegal code is clamped to `4002` on the wire only) |
| `errorCode` / `errorName` | the ws-mixer error code and its name, when one applies: a preceding `error{}`, a bare `4001`-`4999` close, or an SDK-raised error. Never for an HTTP rejection, `1006` or other non-ws-mixer codes, or a provider failure |
| `httpStatus` | the upgrade's HTTP status -- only when the `webSocket` factory exposes it (`upgradeFailure`); the default backend never sets it |
| `fatal` | the SDK will not reconnect |
| `message` | never empty |
| `closeReason` | the reason of the close frame **received from the peer**, verbatim; never this side's own |
| `cause` | the token provider's (or factory's) thrown error |
| `protocolError`, `code`, `name` | set for `4001`/`4003`/`4004` (an SDK bug) |

### Application close

`close({ message })` closes with `APPLICATION_CLOSE` (`error{code:14, message}`, WS close `4014`,
message truncated to 123 UTF-8 bytes) instead of the graceful drain; no reconnect.

### Errors

`WsMixerError` and its subclasses are what the transport throws, emits and rejects with. On a
stream: `ConnError` -- the connection under it ended (the connection's own terminal error is kept as
`cause`; `code`/`wsCode`/`closeReason` copied); `StreamError` -- this stream alone was reset (peer
RESET, the app's own `reset()`/`close()`, a write after `closeWrite()` (`STREAM_CLOSED`), or a
drain deadline (`CANCEL`, `"connection drained"`)). The `fatal` event's `code` is `INTERNAL_ERROR`
whenever no ws-mixer code applies; branch on the disconnect reason's fields instead. `./mcpwarp`
reports with `McpwarpError` (`code`: a register/app error code, `MALFORMED_ENVELOPE`, `CLOSED`,
`UNAUTHORIZED` for a rejected token, `TAKEN_OVER` for a `conflict` before the first registration,
`DISCONNECTED_FATAL` for any other fatal transport failure or when `reconnect.maxAttempts` runs
out; `fatal`; the transport's `WsMixerError`, or on exhaustion
the `DisconnectPayload`, is kept as `cause`).
`expose()` never rejects with a bare `WsMixerError`; the only other rejection is the `AbortError`
from `signal`.

### Counters

`client.stats()` returns "ignore and count" counters (unknown frame types, stale frames, duplicate
pongs, refused opens, drain violations, unknown drain reasons, handler errors, protocol violations)
and `bytesIn`/`bytesOut` (whole WS messages, header included -- not comparable with Go's
payload-only metric). Nothing escalates a sustained run into a connection error.

## Development

```bash
npm run typecheck     # tsc (all) + tsc -p tsconfig.neutral.json (neutral code without Node types)
npm test              # vitest
npm run build         # tsup (ESM, shared chunks) + tsc declarations (@internal stripped)
npm run check:neutral # after build: nothing reachable from ., ./mcpwarp, ./testing imports node:* or a package
npm run smoke:deno    # after build: the neutral build end to end on Deno
```

Tests: spec fixtures (`frame`, `control`, `sequence`) from a `ws-mixer-spec` checkout and the Go
interop test from a `ws-mixer-go` checkout, both resolved from an env var, else a fetched checkout,
else a sibling directory (suites skip with the reason when none resolves):

| | env var | fetched checkout | sibling fallback | pin file |
|---|---|---|---|---|
| spec fixtures | `WSMIXER_SPEC_DIR` | `.spec/spec` (`npm run fetch-spec`) | `../ws-mixer-spec/spec` | `spec.pin` |
| Go interop server | `WSMIXER_GO_DIR` | `.goserver` (`npm run fetch-goserver`) | `../ws-mixer-go` | `goserver.pin` |

The interop test builds `cmd/testserver` with `$GO` or the `go` on `PATH` (`GOTOOLCHAIN=auto`) and is
the one real-socket test of the default (global `WebSocket`) backend. `test/e2e-mcp.test.ts` runs
the real MCP SDKs (devDependencies only) through `FakeEdge`; `test/boundary.test.ts` enforces the
import boundary on the sources. The conformance adapter lives in `conformance/adapter/`.

Declarations come from a plain `tsc -p tsconfig.build.json` pass, not tsup's `dts` (which ignores
`stripInternal`).
