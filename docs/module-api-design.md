# Module API design

Status: design, not yet implemented (2026-10-23). The first module is `gobnc-retecloud`, which serves
the ReteCloud WebSocket protocol (`retecloud-api/documentation/WEBSOCKET_PROTOCOL.md` and
`WEBSOCKET_PROTOCOL_LITE.md`), both full and lite mode, so Rete (desktop and iOS) can use a GoBNC
directly.

## Why a module, and why a separate process

GoBNC is already two processes: the keeper holds uplink sockets and never exits, and the brain can
restart freely and reattach (`docs/keeper-design.md`). A module is a third kind of process, one layer
further out:

```
keeper (uplinks) ⇄ brain (Session, history, read markers, devices) ⇄ module socket ⇄ gobnc-retecloud ⇄ WSS ⇄ Rete
```

- **Clients survive a brain reload.** While the brain execs, the module keeps its WebSocket clients
  connected, then reconnects to the new brain and catches up. A phone sees a short pause, not a
  disconnect — the keeper's property, applied to downstream clients.
- **One WebSocket for every network.** A module sees all networks of the bouncer, so one WebSocket
  carries them all as sessions (lite mode's pipelined `sync_session` already allows it). This fixes
  "one WebSocket per network" on the server side.
- **The brain stays IRC-shaped.** Protocol translation, JSON, WebSocket and compression live in the
  module; the brain exposes IRC state, history and the shared per-device state, nothing
  ReteCloud-specific on the wire.
- **`internal/keeper` is not touched.** Nothing here needs it.

## Principles

1. **The brain owns every piece of shared state.** IRC state, history, read markers, devices and
   per-device open sets all live in the brain's SQLite and are exposed over the module API. A module
   may cache, but it never holds the only copy of anything, so a module can be reinstalled,
   restarted or replaced without losing state, and IRC clients and modules agree (a read marker set
   from iOS clears the badge in an IRC client via MARKREAD, and the other way round).
2. **Snapshot and subscribe are one atomic step, per network.** The events a module receives start
   exactly where its snapshot ends. This is the property that took the most sabotage tests to get
   right in retecloud-api; here it is built in from the start.
3. **The brain never blocks on a module.** Every module has a bounded queue; overflow disconnects
   that module only (as `downlink.Client.Send` does). A disconnected module re-subscribes and fills the
   gap from history.
4. **No event log in the brain.** Resume is a fresh snapshot plus the history gap by id — the same
   model the lite protocol gives its own clients.

## Transport

- A unix socket, path from `gobnc.json` (`modules.socket`, default next to the control socket),
  mode 0600, peer uid checked against the brain's own uid (ported from `internal/keeper`'s
  peercred, not shared, so the keeper stays untouched). A module can act as the user and read all
  history, so only the same user may connect.
- JSON lines, one object per line. Requests carry an `id`; responses echo it; events carry no `id`.
  Language-agnostic and debuggable with `socat`.
- First frame is `hello`: `{api: 1, module: "retecloud", events: [...]}`; the reply names the brain's
  version and the networks. An unknown major `api` is refused.

## What the brain exposes

### Networks and subscription

- `networks` → every network: id, name, state (`connecting`, `registered`, `disconnected`,
  `stopped`), and config fields the protocol shows.
- `subscribe {network}` → a snapshot, then events after it, in order:
  - self: nick, user@host, account, umodes, away;
  - ISUPPORT, including CASEMAPPING, CHANTYPES, STATUSMSG and PREFIX;
  - channels: name, topic (text, setter, time), modes, key, members (nick, prefixes, user@host,
    account, away);
  - enabled caps;
  - `last_id`: the newest history id at the moment of the snapshot.

  Implemented in `Session` exactly where `Attach` registers a downlink, under `s.mu`, so no line
  can fall between snapshot and stream.
- `unsubscribe {network}`.
- Network lifecycle events: added, removed, config changed, connecting, registered, disconnected.

### Events

One event per line the brain processed after routing, carrying:

- `network`, `kind` (`message`, `notice`, `ctcp`, `join`, `part`, `quit`, `kick`, `nick`,
  `mode`, `topic`, `numeric`, …), the raw line, `msgid`, `time`;
- `buffers`: every buffer the line belongs to, each with its display name and CASEMAPPING-folded
  key (a QUIT lists every shared channel, as the C++ node's `UserQuit.common_channels` does);
- `id` per buffer when the line was stored in history (`messages.id`; these ids are what the
  protocol's `after_id`, `last_message_id` and `closed_at_id` mean);
- `label` when the line is the echo of a module's own `send`.

The structured parts are computed by the brain (it already routes for downlinks), so a module never
re-implements IRC routing. A state-only event (a member's away change, a mode) carries the same
envelope without an `id`.

Also emitted: `read_marker` (network, buffer, id/time, who set it), `buffer_open` and
`buffer_close` (network, device, buffer, reason), `device` changes.

### History and activity

- `messages {network, buffer, before|after|around: id, limit}`, plus `anchor: first_unread`, built on
  `store.QueryMessages`. Returns rows with ids and whether more exist on each side.
- `messages_after {network, after_id, limit}` across all buffers of a network — full mode's gap
  replay from a device's watermark.
- `activity {network, device}` → per buffer shown on that device: newest id and time, unread count
  after the read marker and the device's closed-at floor, highlight count (highlight rules live in
  the brain so every module counts alike).

### Read markers

`read_marker {network, buffer, id}` sets GoBNC's own `read_markers` (only forward), which broadcasts
MARKREAD to IRC clients and `read_marker` to modules. The table stores timestamps; the brain
converts id ↔ time.

### Devices and per-device open sets

New brain tables (owned by `internal/store`, generic rather than ReteCloud-named):

- `devices`: `client_id`, name, created, last seen, per-device token hash (see Auth).
- `open_buffers`: `(network_id, client_id, folded target)`, display name, `is_open`, `closed_at_id`.
- `device_cursors`: `(network_id, client_id)`, last id synced (full mode's watermark).

The rules from retecloud-api move into the brain, applied at history-store time so they are ordered
with the line:

- an open or close on one device stays on that device;
- a new conversation line in a query opens it on every device where it is not open, including one
  that closed it; what arrived while it was closed stays dismissed (floor = id − 1). CTCP requests,
  NOTICEs and server traffic never open one. The `buffer_open` event is emitted before the line's
  own event;
- a device's first sync is seeded with the queries that have lines after the shared read marker.

Requests: `open_buffer`, `close_buffer`, `buffers {network, device}`, `device_cursor` get/set.

### Acting as the user

- `send {network, line, label?}` goes through `Session.HandleClientMessage` with a synthetic
  `Downlink` per module connection, so self-echo to IRC clients, history storage and flood pacing
  behave exactly as for a real client. The echo comes back as an ordinary event with the `label`.
- Network control (`connect`, `disconnect`, `reconnect`, config updates) goes through the same
  plumbing as `internal/control` / `BNC` commands.

### Auth

`auth_check {password | cert_fingerprint}` checks GoBNC's own downlink credentials
(`internal/auth`). On success the module asks the brain to issue a per-device token
(`device_token {client_id}`), stores nothing itself, and later clients authenticate with that
token; `device_revoke` invalidates it.

## Resume rules this must respect

- **Brain restart.** The module socket is served by the new brain only after boot, and `send` to a
  resumed network is refused with `not_ready` until after `SendLiveReady` (`Server.Run`'s boot
  order: nothing may write to a resumed uplink before LiveReady). The module retries; its clients
  just see a delay.
- **Gap-only resume gaps.** Whatever `Session` cannot reconstruct after a brain restart (member
  rosters until the NAMES refresh in `dialNetworkLocked`'s resumed branch, nick-recovery state) is
  marked as such in the snapshot (`members_complete: false`), and a later `members` event replaces
  the list once the refresh lands, so a module never presents a partial roster as complete.
- **Module restart or overflow.** Re-`subscribe` and fetch `messages_after {last id}`.

## Code layout

- `internal/module` — protocol types, listener, peer check, per-connection queue.
- `internal/session` — subscriber hook next to the downlink fan-out; snapshot builder; the
  synthetic downlink.
- `internal/store` — device, open-buffer and cursor tables and queries.
- `internal/server` — wiring, config, starting configured modules.
- `pkg/moduleapi` — the Go client library (not `internal/`, so modules can live outside the tree).
- `cmd/gobnc-retecloud`, `modules/retecloud/` — the ReteCloud module.

Modules are configured in `gobnc.json` and started by the brain **detached**, only if not already
running (the `keeperboot` pattern), so a module outlives a brain reload.

## Build order

One commit and one test per step (CLAUDE.md's revert-and-confirm discipline applies to each):

1. `internal/module` socket, hello, peer check, queue overflow.
2. Subscribe: snapshot + ordered events, with a test that breaks the atomicity on purpose.
3. History, activity and read-marker requests.
4. Device, open-buffer and cursor tables and their rules.
5. `send` via the synthetic downlink; network control.
6. Auth and device tokens.
7. Brain reload with a module attached: it stays connected and fills the gap (extends the
   `reload_handoff` / `resume_*` tests).
8. `pkg/moduleapi` plus a tiny event-logger module as a smoke test.
9. `gobnc-retecloud`: full and lite mode, deflate-stream compression. Conformance: retecloud-harness's
   protocol tests pointed at the module through a fixture adapter.

## Open questions

- Full mode's session create/delete over REST: map to GoBNC network add/remove, or leave networks
  to `gobnc.json` and refuse?
- Push notifications for iOS (later): a module-side APNs path fed by highlight events.
