# Module API design

Status: implemented on branch `module-api`: the brain side and the first module,
`gobnc-retecloud` (conformance below). Spec: `specs/2026-10-23-gobnc-module-api-lite.md`.

A **module** is a separate process that connects to the brain over a unix socket, negotiates
features, subscribes to per-network events with an atomic snapshot, reads history, activity and
read markers, acts as the user through virtual clients, and keeps per-device state and key/value
data in the brain. The first module, `gobnc-retecloud`, serves the ReteCloud WebSocket **lite**
protocol to Rete on iOS — one WebSocket for every network.

## Why a module, and why a separate process

GoBNC is already two processes: the keeper holds uplink sockets and never exits, and the brain can
restart freely and reattach (`docs/keeper-design.md`). A module is a third kind of process, one
layer further out:

```
keeper (uplinks) ⇄ brain (Session, history, read markers, devices) ⇄ module socket ⇄ gobnc-retecloud ⇄ WSS ⇄ Rete iOS
```

- **Clients survive a brain reload.** While the brain execs, the module keeps its WebSocket clients
  connected, then reconnects to the new brain and catches up from history. A phone sees a short
  pause, not a disconnect.
- **One WebSocket for every network.** A module sees all networks of the bouncer, so one WebSocket
  carries them all as sessions.
- **The brain stays IRC-shaped.** Protocol translation, JSON, WebSocket and compression live in the
  module; the brain exposes IRC state, history and shared per-device state, nothing
  ReteCloud-specific.
- **Extendable.** The API is generic — bridges, loggers, bots — with features granted per module.
- **`internal/keeper` is not touched.**

## Decisions (owner, 2026-10-23)

- **iOS lite mode only.** `gobnc-retecloud` serves `sync_mode: "lite"`; desktops use IRC.
- **Wire-identical with retecloud-api's `lite-mode` branch** after `authenticate`. Pinned spec
  revision: retecloud-api **`d016cb5`** (`documentation/WEBSOCKET_PROTOCOL_LITE.md`, "Sessions and
  the per-device toggle" and "Backend-specific authentication"). A needed protocol change goes to
  retecloud-api `lite-mode` first (doc + harness test), then here; `lite-mode` merges into
  retecloud-api only once GoBNC and Rete are both drawn up.
- **Authentication** is GoBNC's own, over the WebSocket: the bouncer password
  (`{"password", "client_id"}`) or a TLS client certificate whose SHA-256 is in `cert_fingerprints`
  (`{"client_id"}`). No REST, JWT or device tokens.
- **One login, all networks.** `authenticate_success` lists every network with this device's
  toggle; `sync_session` (lite) turns one on, `unsync_session` off; the toggle lives per device in
  the brain. retecloud-api has the same flow (track S of the spec).
- **Generic features**, negotiated in `hello` and granted per module in `gobnc.json`. `devices` is a
  core feature: per-device open sets and toggles, and the rule "a new conversation line in a query
  opens it on every device", run in the brain at history-store time.
- **Networks are GoBNC's** (SQLite `networks`, `gobnc network add/mod`): the module refuses
  `create_session` and session deletion with `"Not supported"`.
- **The brain owns all shared state.** A module may cache but never holds the only copy, so it can
  be restarted or replaced without losing anything, and IRC clients and modules agree (a read
  marker set from iOS reaches IRC clients as MARKREAD, and the other way round).
- **The brain starts modules detached**, only if not already running; they outlive a brain reload.
- **Module reload** (`gobnc module reload <name>`, below) replaces a module without a brain
  reload. **Next milestone:** push (below).

## Architecture (approach A: bus + virtual clients)

### The bus: atomic snapshot and stream, per network

Each `Session` has a bus mutex, `busMu` (`internal/session/bus.go`). Every path that changes state a
snapshot reports holds it across the change and the module emit: `HandleMessage` from storing
history through `applyState` to the envelope, plus the CTCP-request path, labeled self-echoes, the
local self-echo, MARKREAD sets, CAP lines, registration (001, completion), `SeedFromBlob` and
`HandleDisconnect`. `Subscribe` builds the snapshot and registers the sink under `busMu`, and the
subscribe response is queued under it too, so a module's events start exactly where its snapshot
ends (`TestSubscribeAtomicWithLineStream` breaks this on purpose). Lock order is `busMu` before
`s.mu`. Fan-out to downlinks, and anything that may re-enter one of these paths, runs after `busMu`
is released. Emitting is a non-blocking enqueue; the brain never waits on a module.

Known limits of the bus, from review (not needed by gobnc-retecloud, which takes fresh snapshots):

- A solicited reply routed to one client emits no line, so state it changes (a client's own NAMES
  rebuilding a roster that was already complete, WHO updating hosts, MODE nick updating umodes)
  reaches modules only through their next snapshot. A roster that becomes complete still emits
  `state`/`members`.
- A CTCP request in `edge` or `disable` mode emits nothing; a host it teaches (`applyState`'s
  prefix tracking) likewise waits for the next snapshot.
- An uplink disconnect resets Session state under `busMu` but emits no line; modules learn it from
  the `network` event (state `disconnected`) and should resubscribe once it is `registered` again.
- `busMu` is held across the history insert and the keeper blob push of the line being applied,
  so a Subscribe waits for those (never for a downlink or a module).

### Envelopes

For each line the brain computes what modules would otherwise re-implement
(`internal/session/envelope.go`): the kind, the buffers it belongs to (display name, CASEMAPPING
key, and the history id where that buffer stored it), source, self, msgid, time, and the label of a
module's own echo. QUIT/NICK/AWAY/ACCOUNT/CHGHOST/SETNAME list the channels shared before
`applyState` plus the user's query when history has one. A solicited reply routed to one client by
the `RequestTracker` emits no line.

### Virtual clients

A module acts as the user through a `VirtualClient` (`internal/session/virtual.go`): a `Downlink`
with no socket, ID `mod:<module>:<client>`, one per WebSocket per followed network. Its `Send` is a
`client_line` event, so the `RequestTracker` routes a WHOIS reply to the phone that asked. Broadcast
paths whose lines the module already has as envelopes skip virtual clients (the main broadcast
loop, self-echo fan-outs, CTCP relay, MARKREAD broadcasts, own-umode broadcast). Virtual clients
survive an uplink disconnect and are not counted as attached clients.

Known limits of virtual clients: a line event's `label` is not tied to the client that sent it, so
a module should use labels unique across its clients; the "uplink connection failed" notice
before registration goes to IRC clients only; a virtual client's caps are fixed (CAP DEL arrives as
a client_line but does not change them).

### Devices in the brain

Tables `devices`, `device_networks` (the toggle) and `open_buffers` (open/closed per device, with a
closed-at floor that only rises; `internal/store/devices.go`). When a line is stored under a query,
`maybeStoreHistory` reopens it on every device where it is not open (a closed row reopens with its
floor raised to id−1) and emits `buffer_open` reason `message` before the line's own event. As in
ReteCloud, only a PRIVMSG opens a query (ours included, CTCP ACTION included; notices, other CTCP,
server lines and `$` masks never). `devices.seed` gives a new device the queries with unread lines.

The rule applies to devices that follow the network (`device_networks.subscribed`), and never
reopens a buffer closed after the line (a keeper replay of a stored line). Seeding runs once per
device and network (`seeded`). Known limits: the reopen and seeding queries run under `busMu`
(a transaction per stored query PRIVMSG; seeding reads every query target once), and a
buffer_open names the query as that line spelled the nick.

### Readiness and reload

The module socket starts before the networks are registered (`Server.Run` → `startModules`), so a
module connecting during boot is told `ready: false`; `ready` follows right after `SendLiveReady`
(`bootNetworks` → `markModulesReady`), and `send.line` / network control answer `not_ready` until
then — nothing writes to a resumed uplink before LiveReady. At a brain reload the old brain's module
connections close; the module reconnects to the new brain on the same socket path (the replacement
replaces the socket file; the old brain removes the path on exit only if it is still its own),
re-subscribes and fetches `history.messages after_id` per buffer (`TestModuleSurvivesBrainReload`).

Resume is gap-only (`docs/keeper-design.md`): a resumed brain does not know channel rosters until
its NAMES refresh lands, so such channels are `members_complete: false` in the snapshot until a
`state` event of kind `members` replaces the list.

### Code layout

- `pkg/moduleapi/wire` — frames, constants, params/result/event types (importable by modules).
- `pkg/moduleapi` — Go client library: dial, call, events, reconnect, the module lock, hot-reload
  handoff.
- `internal/module` — the brain-side server: socket, peer check, hello, per-connection queue
  (8192 frames; overflow closes that connection), dispatch, one file per feature.
- `internal/session` — bus, snapshot, envelopes, virtual clients, device rules.
- `internal/store` — id-paged history, activity, devices, module key/value.
- `internal/server/modules.go` — the `module.Backend`, readiness, network events, module start.
- `cmd/gobnc-modlog` — example logger module; `cmd/gobnc-retecloud`, `modules/retecloud` — the
  lite module.

## Wire protocol

Transport: unix socket at `modules.socket` (default: `control_socket` path with its final element
replaced by `gobnc-modules.sock`), created mode `0600`. Peer uid must equal the brain's uid
(`SO_PEERCRED` on Linux, `getpeereid`/`LOCAL_PEERCRED` on BSD/macOS); otherwise close without a
reply. Framing: one JSON object per line, `\n`-terminated, UTF-8, max 1 MiB per line (longer → close
with error code `bad_frame`).

Frames:

- Request (module → brain): `{"id": <uint64>, "method": "<name>", "params": {...}}`
- Response (brain → module): `{"id": <same>, "result": {...}}` or
  `{"id": <same>, "error": {"code": "<code>", "message": "<text>"}}`
- Event (brain → module): `{"event": "<name>", "data": {...}}`

Error codes (exact strings): `hello_required`, `unsupported_api`, `not_granted`, `unknown_method`,
`bad_params`, `not_found`, `not_ready`, `overflow`, `bad_frame`, `internal`.

The first request must be `hello`; anything else gets `hello_required` and the connection closes.

`hello` params `{"api": 1, "module": "<name>", "features": ["..."], "pid": <int>?, "handoff": <bool>?}`
(`pid`: the module's process id, used to stop or restart it; `handoff`: it replaces itself on a
`reload` event) → result
`{"api": 1, "brain_version": "<version.String()>", "granted": [...], "denied": [...], "ready": <bool>}`.
`api` other than `1` → `unsupported_api` and close. `module` must match a configured module name
(`modules.list[].name`), else `not_granted` and close. `granted` = requested ∩ configured
`features`. `ready` is false until the brain has sent `SendLiveReady`; the `ready` event follows.

Features and their methods (a method of a feature not granted → `not_granted`):

| Feature | Methods |
|---|---|
| `networks` | `networks.list`, `networks.connect`, `networks.disconnect`, `networks.reconnect` |
| `events` | `events.subscribe`, `events.unsubscribe` |
| `history` | `history.messages`, `history.activity` |
| `read_markers` | `read_markers.get`, `read_markers.set` |
| `clients` | `clients.attach`, `clients.detach` |
| `send` | `send.line` (requires `clients` too) |
| `devices` | `devices.touch`, `devices.networks`, `devices.set_network`, `devices.buffers`, `devices.open`, `devices.close`, `devices.seed` |
| `auth` | `auth.check` |
| `storage` | `storage.get`, `storage.set`, `storage.delete`, `storage.list` |
| `commands` | `commands.register`, `commands.reply` |

Method shapes (`network` is the GoBNC network id, int64; `buffer` is a display name; `key` is its
CASEMAPPING fold under the network's ISUPPORT `CASEMAPPING`):

- `networks.list {}` → `{"networks": [{"id", "name", "state", "nick"}]}`; `state` ∈ `"stopped"`,
  `"connecting"`, `"registered"`, `"disconnected"`.
- `networks.connect|disconnect|reconnect {"network"}` → `{}`; before ready → `not_ready`.
- `events.subscribe {"network", "kinds"?: [...]}` → `{"snapshot": Snapshot}`; then `line` /
  `state` events for that network. Re-subscribing replaces the previous subscription and returns a
  fresh snapshot. `events.unsubscribe {"network"}` → `{}`.
- Snapshot: `{"network", "state", "last_id", "seq", "self": {"nick","user","host","account","umodes","away"},
  "isupport": {"KEY": "value", ...}, "caps": [...], "channels": [{"name","key","topic":
  {"text","setter","time"},"modes","chankey","members_complete",
  "members": [{"nick","prefixes","user","host","account","away"}]}]}`. `last_id` = newest
  `messages.id` of the network at snapshot time (0 if none). `seq` = seq of the last `line` event
  emitted before the snapshot, so a module that re-subscribes for a fresh snapshot can place it in
  the stream it is consuming. `members_complete` is false for a
  channel seeded from the keeper blob after a brain restart until its NAMES refresh lands, at which
  point a `state` event `{"network","kind":"members","buffer","key","members":[...],"complete":true}`
  replaces the list.
- `history.messages {"network","buffer","before_id"?|"after_id"?|"around_id"?|"anchor"?,"limit"?,"device"?}`:
  exactly one of the four selectors (none = latest). `anchor` ∈ `"first_unread"`, `"latest"`;
  `first_unread` needs `device` (to apply its closed-at floor) and returns the page starting at the
  first line after the read marker. `limit` 1..500, default 50. → `{"messages": [Message],
  "has_before": bool, "has_after": bool}`, ascending by id. Message:
  `{"id","time","msgid","command","source","buffer","raw","text"}`; `time` RFC 3339 nano UTC.
- `history.activity {"network","device"}` → `{"buffers": [{"buffer","key","last_id","last_time",
  "unread","highlights"}]}` for every buffer the device shows (joined channels + open queries +
  `Server`). `unread` counts PRIVMSG/NOTICE (CTCP ACTION included, other CTCP excluded, our own
  lines excluded) with `time` after the read marker and `id` above the device's `closed_at_id`.
  `highlights`: among those, lines in a query, or whose text contains our current nick as a whole
  word under CASEMAPPING (word characters: `A-Za-z0-9_-[]\`^{}|`).
- `read_markers.get {"network","buffer"}` → `{"id","time"}` (`id` = newest message id at or before
  the stored time; 0/"" if none). `read_markers.set {"network","buffer","id"}` → `{"stored_id",
  "updated": bool}`; only moves forward; on update the brain sends MARKREAD to IRC clients with
  `draft/read-marker` (existing `broadcastMarkRead`) and emits `read_marker`.
- `clients.attach {"network","client"}` (`client` ≤ 64 bytes, `[A-Za-z0-9._-]`) →
  `{"client_id": "mod:<module>:<client>"}`; registers a virtual client in that `Session`.
  `clients.detach {"network","client"}` → `{}`. All of a connection's virtual clients are detached
  when the module connection closes.
- `send.line {"network","client","line","label"?}` → `{}` once handed to
  `Session.HandleClientMessage` as that virtual client (with `label` tag set when given). Before
  ready or while the network is not registered → `not_ready`.
- `devices.touch {"client_id","name"?}` → `{"created": bool}`.
- `devices.networks {"client_id"}` → `{"networks": [{"network","subscribed"}]}` (every network;
  missing rows = `false`). `devices.set_network {"client_id","network","subscribed"}` → `{}`.
- `devices.buffers {"client_id","network"}` → `{"buffers": [{"buffer","key","open","closed_at_id"}]}`.
- `devices.open {"client_id","network","buffer"}` → `{}`; emits `buffer_open` reason `"user"`.
- `devices.close {"client_id","network","buffer"}` → `{"closed_at_id"}` (newest id in that buffer);
  emits `buffer_close`.
- `devices.seed {"client_id","network"}` → `{"seeded": [buffers]}`: only when the device has no
  `open_buffers` rows for that network: opens every query with a conversation line after the
  shared read marker (not CTCP requests, not server/status traffic); emits `buffer_open` reason
  `"seed"` per buffer.
- `auth.check {"password"} | {"cert_sha256"}` plus `"remote_ip"` → `{"ok": bool}`. Password via
  `auth.VerifyPassword` against the `auth` row; only when `allow_password_auth`. Cert via
  `cert_fingerprints` (normalized with `auth.NormalizeFingerprint`); only when `allow_cert_auth`.
  `remote_ip` not in `allowed_ips` (when set) → `ok:false`.
- `storage.*` — key ≤ 256 bytes, value string ≤ 64 KiB, namespaced by module name:
  `get {"key"}` → `{"value","found"}`, `set {"key","value"}` → `{}`, `delete {"key"}` → `{}`,
  `list {"prefix"}` → `{"keys": [...]}`.
- `commands.register {"name","help"}` → `{}`; `name` = `[a-z][a-z0-9-]{0,31}`, must not collide
  with an existing `BNC` command. `BNC <name> <args...>` from an IRC client and
  `gobnc <name> <args...>` from the CLI emit `command {"invocation_id","args":[...],"origin":"bnc"|"cli"}`
  to the registering module; the module answers `commands.reply {"invocation_id","lines":[...]}`;
  no reply within 10 s → the caller sees `"module <name> did not answer"`.

Events (brain → module):

- `ready {}` once after `SendLiveReady` (also reflected in `hello`'s `ready`).
- `network {"network","name","state","nick","change"}` with `change` ∈ `"added"`, `"removed"`,
  `"config"`, `"state"`.
- `line {"network","seq","kind","raw","msgid","time","source","nick","self","label"?,
  "buffers": [{"name","key","id"?}]}` — `seq`: per-network uint64 counter, +1 per emitted event,
  reset on brain start. `kind` ∈ `message` (PRIVMSG, incl. CTCP ACTION), `notice`, `ctcp` (PRIVMSG
  CTCP other than ACTION), `ctcp_reply` (NOTICE CTCP), `tagmsg`, `join`, `part`, `quit`, `kick`,
  `nick`, `mode`, `topic`, `invite`, `away`, `account`, `chghost`, `setname`, `numeric`, `error`,
  `other`. `buffers`: for messages the channel or (for a query) the peer's nick, `Server` for
  server-sourced / `$mask` lines; QUIT/NICK/AWAY/ACCOUNT/CHGHOST/SETNAME list every channel the user
  shared with us before the change plus their query buffer if one exists in history; `id` present
  when that buffer stored the line. `self` true when the source is us. `label` present on the echo
  of a module's own `send.line`. Solicited replies routed by `RequestTracker` to a specific client
  emit **no** `line` event (they go to the asker only, as `client_line` for a virtual client).
- `client_line {"network","client","raw"}` — anything `Session` sent to that virtual client.
- `state` — see Snapshot.
- `read_marker {"network","buffer","key","id","time"}`.
- `buffer_open {"network","client_id","buffer","key","reason"}` (`reason` ∈ `message`, `user`,
  `seed`); `buffer_close {"network","client_id","buffer","key","closed_at_id"}`.
- `device_network {"client_id","network","subscribed"}`.
- `command` — see `commands.register`.
- `overflow {}` written best-effort before the brain closes a module whose queue is full.
- `reload {"exec","args"?,"config"?}` — sent by `gobnc module reload` to a module that said
  `handoff`: its current gobnc.json entry (see Module reload).

Queue: 8192 frames per module connection, written by one writer goroutine; a full queue closes that
connection (the brain never blocks). Order of all frames on one connection = order of enqueue.

Implementation notes on the protocol above:

- No path removes a network from a running brain yet, so `network` events with `change: "removed"`
  are not sent.
- `BNC <name> …`, the control socket's `MODULE <name> …` and `gobnc <name> …` (any non-flag first
  argument that is not a built-in verb) reach a module's registered command.

## ReteCloud lite: what both backends add

Added to retecloud-api `lite-mode` in track S and implemented identically by `gobnc-retecloud`:

- `authenticate` data gains `"client_id"` (string, required in lite use; the device id later sent in
  `sync_session`). `authenticate_success` data gains `"sessions": [{"session_id","name","state",
  "subscribed"}]`; `state` ∈ `"connected"`, `"connecting"`, `"disconnected"`; `subscribed` = this
  device's stored toggle (default `false` for a never-seen device/session pair).
- `sync_session` with `sync_mode: "lite"` stores `subscribed=true` for (device, session).
- New `unsync_session` (`session_id` at top level, `data: {"client_id"}`) → `unsync_session_success`;
  stores `subscribed=false`; the connection stops receiving anything for that session (tier 0,
  activity, subscriptions); a later `sync_session` re-enables it.
- New broadcast `session_created {"type":"session_created","session_id","data":{"name","state"}}` to
  every authenticated connection of the account when a session is created; `session_deleted`
  (existing) unchanged.
- GoBNC only: `session_id` = `"net-<network id>"`; `name` = the GoBNC network name;
  `create_session` → `create_session_error` `"Not supported"`.

## Conformance

retecloud-harness runs its lite tests against `gobnc-retecloud` with `RETE_BACKEND=gobnc`
(`docker-compose.gobnc.yml`; see the harness README). Against the pinned retecloud-api revision
**`d016cb5`**, harness commit `890f1b2`: `test_lite_mode`, `test_lite_pipelining`,
`test_casemapping`, `test_compression`, `test_reply_routing`, `test_history_paging`,
`test_lite_bootstrap_cache`, `test_session_toggle` and `test_buffer_close` pass on ircu2 and solanum
(22 passed; 4 skipped because they need full/state mode or REST).

Known differences from retecloud-api, none visible to the lite tests: `get_messages` needs a
`buffer` (no network-wide paging), at most 500 lines per page; `creation_id` is 0 in
`buffer_state`; identity `realname`, `secure`, `bot` and `oper` are not known to the brain.

## Configuration

```json
"modules": {
  "socket": "",
  "list": [
    {"name": "retecloud", "exec": "/usr/local/bin/gobnc-retecloud",
     "features": ["networks", "events", "history", "read_markers", "clients", "send", "devices", "auth", "storage"],
     "config": {"listen_addr": "0.0.0.0:8443", "tls_cert": "certs/server.crt", "tls_key": "certs/server.key"}}
  ]
}
```

`socket` defaults to `gobnc-modules.sock` next to the control socket. A started module gets
`GOBNC_MODULE_SOCKET`, `GOBNC_MODULE_NAME` and `GOBNC_MODULE_CONFIG` (the entry's `config`) and holds
`<socket dir>/gobnc-module-<name>.lock` while it runs. REHASH applies a changed `list` to module
connections made afterwards; it does not start or stop modules (`gobnc module start|stop`).

## Module reload

`gobnc module <verb> [name]`, `BNC MODULE <verb> [name]` and the control verb
`MODULECTL <verb> [name]` (reply `OK <json lines>` / `ERR <text>`) manage module processes from the
running brain (`internal/server/modules_control_unix.go`); one operation per module at a time.

- `list` — each configured module: `running, pid <pid>[, handoff], features <...>` or
  `not running` (`(lock held by another process)` when its lock is taken without a connection).
- `start` — starts a configured module with an `exec` that is neither connected nor locked.
- `stop` — SIGTERM to the pid each connection reported in hello, then waits until no connection of
  that name remains and its lock is free; SIGKILL after 10 s, then 2 s more. A module that sent no
  pid is refused.
- `restart` — stop, then start from the current gobnc.json entry (after a REHASH, its new `config`).
- `reload` — needs a ready brain and exactly one connection. A module without `handoff` is
  restarted instead (`no handoff support; restarted ...`). Otherwise the brain sends it a `reload`
  event with its current entry and waits up to 30 s for a newer connection of that name **and** the
  old one closing: `reloaded <name>: pid <old> → <new>`. Otherwise the error names the old pid,
  which is still serving.

The handoff itself is the module's (`pkg/moduleapi/handoff.go`), so a bug in it is fixed by
reloading the module, not the brain:

1. `Client.Handoff` starts the replacement detached (own session, stdio on `/dev/null`) from the
   event's `exec`/`args`, with `GOBNC_MODULE_CONFIG` = the event's `config` and inherited
   descriptors: fd 3 the module lock, fd 4 a confirmation pipe, then the module's files (for
   `gobnc-retecloud`, `wss`: the listening socket). `GOBNC_MODULE_HANDOFF=lock=3,confirm=4,wss=5`
   names them. Inherited descriptors are marked close-on-exec on adoption.
2. The replacement's `Dial` adopts the inherited lock (a flock belongs to the open file, which both
   processes share), `InheritedListener("wss")` the socket (a changed `listen_addr` binds anew),
   and once it is connected to a ready brain, `ConfirmHandoff` writes `ok`; only then does it start
   accepting. Until then both processes hold the socket and the old one keeps accepting.
3. The old process, once confirmed (15 s at most), stops accepting, closes each WebSocket with 1012
   "service restart" after the frames already queued (2 s drain), and calls `Client.Release`, which
   closes the lock file **without** unlocking it — unlocking would release the replacement's lock.
   A replacement that gives up before confirming does not unlock it either.
4. If the replacement exits or does not confirm, the old process kills it and keeps serving.

Live WebSocket connections are not carried across (TLS state cannot move between processes, the
reason the keeper exists): clients reconnect and resume with `after_id`, the same path as a brain
reload. During the overlap both processes are connected to the brain; virtual client names carry
the pid (`p<pid>-ws<n>`), so they never collide. A WebSocket closed because the module shuts down
(SIGTERM, `stop`) gets 1001 "going away". The replacement loads the TLS certificate afresh, so a
renewed certificate needs only a reload.

## Later milestone: push notifications (iOS)

The module decides when to push and encrypts the notification; the ReteCloud API only relays the
ciphertext to Apple, so neither the API nor Apple can read message content.

- **Registration.** Over its authenticated WebSocket an iOS device sends its APNs token, APNs
  environment and a per-device notification key it generated; the brain stores them with the
  device. The key never leaves the bouncer and the device.
- **When to push.** On a highlight or a DM, for each device with push registered that has no live
  connection (or reports itself backgrounded), unless muted; highlight rules are the brain's;
  coalesced per buffer with a short delay.
- **Payload.** The module seals `{network, buffer, sender, text, id}` with the device key (AES-GCM;
  construction pinned with Rete's Notification Service Extension) and POSTs `{device_token,
  environment, ciphertext, collapse_id}` to a relay endpoint on the ReteCloud API, authenticated by
  a relay key of the bouncer owner's ReteCloud account.
- **ReteCloud API.** The relay sends an alert push with a generic title/body and `mutable-content`,
  the ciphertext as a custom field; it stores nothing and rate-limits per relay key.
- **Rete (iOS).** A Notification Service Extension decrypts and replaces title and body; on failure
  the generic text stays.

Open: how the relay key is issued on the ReteCloud side; mute rules (per device per buffer, or
follow the device's open set); whether plain-text content is an acceptable alternative.
