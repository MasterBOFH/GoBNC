# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

GoBNC is a single-user IRC bouncer in Go. See `README.md` for user-facing setup, config (`gobnc.json`), and the `BNC`/CLI admin surface — not repeated here.

## Commands

```bash
go build -o bin/gobnc ./cmd/gobnc          # bouncer binary
go build -o bin/gobnc-keeper ./cmd/keeper  # keeper binary
make build                                 # both, via Makefile

go test ./...                              # full suite
go test ./internal/session/...             # one package
go test ./internal/session/... -run TestName -v   # one test
go test -race ./...                        # make test-race
go test -tags=integration -count=1 -timeout 120s ./...   # make test-integration
make test-ircd                             # real-ircd interop via Docker; requires Docker
```

No linter is configured (CI runs `go test`, `go test -race`, and a build — nothing else). `go vet ./...` is worth running but isn't wired into CI.

Tests that dial real fake TCP servers (most of `internal/session`, `internal/brain`, `internal/server`) are fast and safe to run repeatedly. `test-ircd` needs Docker and takes longer; don't run it reflexively.

## Architecture: the keeper/brain split

The bouncer is two processes, not one, and this shapes almost everything in `internal/`. Full rationale in `docs/keeper-design.md` — read it before touching `internal/keeper` or the wire protocol between the two processes; it documents *why*, not just *what*, and several non-obvious invariants live only there.

**Why two processes:** the bouncer needs to reload its own code without dropping uplink TCP/TLS connections to IRC servers. There's no way to hand off `crypto/tls` session state across an exec, so whatever holds the uplink sockets can never exit. `internal/keeper` (long-lived, rarely changes) holds the sockets; `internal/brain` + everything downstream of it (frequently changes) can restart freely and reattach.

- **`internal/keeper`** — owns the uplink TCP/TLS sockets. Line framing, autonomous `PING` reply, a seq'd ring buffer per network, and a small per-network key/value "blob" store (`internal/keeper/blob.go`) that survives a brain restart. This is the *only* package with IRC-shaped state that outlives a brain process — it deliberately does no other IRC interpretation. `internal/keeper/listener.go` serves the keeper↔brain unix socket protocol (`internal/keeper/protocol.go`); `internal/keeper/client.go` (`AttachClient`) is the brain-side half.
- **`internal/registration`** — a pure state machine (`Step(State, Input) (State, []Action)`) for CAP/SASL/nick-collision/welcome-numeric handling. No I/O. Its replay-identical-to-live property is regression-tested against a real captured-transcript corpus (`testdata/registration/`).
- **`internal/brain`** (`Driver`) — the only thing that turns `registration.Action`s into real writes over the keeper's unix socket. Owns per-network registration/reconnect/nick-recovery/flood-pacing bookkeeping. No transport of its own, no protocol logic of its own — it wires the other two together.
- **`internal/session`** (`Session`) — one per IRC network. Consumes the line stream `Driver` republishes, tracks channel/user/cap state, replays a welcome+channel burst to newly-attached clients, and pushes small derived facts (self-nick, cloak, enabled caps, channel keys, isupport) back to the keeper's blob store so a *resumed* brain can reconstruct enough to mark itself registered without the original registration burst ever replaying.
- **`internal/downlink`** — accepts TLS IRC clients, authenticates them (password or cert fingerprint, `internal/auth`), and attaches them to a `Session`.
- **`internal/server`** (`Server`) — wires `internal/store` (SQLite config/history), every network's `Session`, the one shared keeper attach/`Driver`, and the downlink listener together; owns the boot sequence and REHASH/admin plumbing.
- **`internal/control`** — the CLI/`BNC`-command admin unix socket (start/stop/reconnect network, status, rehash).

`internal/uplink` (the pre-split single-process design) no longer exists in this tree — the cutover to keeper/brain is complete; don't design around a coexistence period.

### Resume: the property most bugs in this area violate

A brain restart **reattaches** to a keeper that keeps holding already-connected uplinks. `Server.Run`'s boot order matters and is enforced, not incidental: every network is registered (`registerNetworkLocked`, incl. `Session.SeedFromBlob` for a network the keeper already held) **before** `SendLiveReady`, and only dialing a genuinely new network happens **after** — the keeper's `serveLive` reads nothing at all, not even a `WriteRequest`, until `LiveReady` is the first frame it sees. Anything that needs to write to an already-resumed network's uplink (a NAMES refresh, a USERHOST query, …) has to be deferred to the post-`SendLiveReady` point (`dialNetworkLocked`'s resumed branch), never fired from inside `SeedFromBlob`/`registerNetworkLocked` itself — this has been the exact shape of more than one real bug.

Resume is **gap-only**: a resumed attach never replays the original registration burst, only lines since the brain's own last acked seq. That means `Session` state that used to be learned by watching that burst (self-nick, cloak/host, enabled caps, channel keys, ISUPPORT) has to come from the keeper's blob store instead, and anything not explicitly pushed there is invisible to a resumed brain — a channel's member roster and nick-recovery state are known, accepted gaps for exactly this reason.

## Working discipline for this repository

- **Every feature or fix gets its own test**, and the project's own revert-and-confirm discipline applies: before trusting a test that's meant to prove a specific fix, temporarily undo the fix, confirm the test actually fails (with the expected symptom, not just any failure), then restore it. A test that never demonstrably fails without the fix hasn't proven anything.
- **One commit per feature or fix**, not batched. Don't accumulate unrelated changes into one commit; don't leave finished, tested work uncommitted.
- **At the end of every turn where files changed, explicitly state whether `internal/keeper` was touched** — yes/no, and if yes, what changed. `internal/keeper` is the one package designed to change rarely (see "why two processes" above); a change there is categorically higher-stakes than one anywhere else in the tree, since it's the one thing that isn't supposed to need a restart to pick up.
- **Repeat that same keeper-change confirmation in the commit message** for every commit, whether or not `internal/keeper` was touched by that commit.
