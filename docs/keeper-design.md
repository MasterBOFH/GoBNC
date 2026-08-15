# Keeper/brain split: design record

This document exists because most of this project's reasoning currently
lives only in conversation history, and conversation history compacts and
degrades. It is written while that reasoning is still complete. If something
here turns out to be wrong against the actual code, the code is the
authority — update this file, don't quietly work around what it says.

## The problem this exists to solve

The bouncer needs to upgrade its own code without dropping uplink
connections to IRC servers. Uplinks are TLS. `exec`-with-fd-inheritance
transfers the raw socket but not the `crypto/tls` session state, which lives
in the old process's heap with no export path. So the process holding
uplink sockets can never exit. Something small and stable has to own those
sockets permanently; the code that gets iterated on has to live somewhere
else, in a process that's free to restart.

That constraint is the entire reason this is two processes instead of one
with a reload path. There is no way to reload TLS connection state in
place; the socket has to stay in a process that never restarts.

## Rejected: Go plugins

A prior attempt used `plugin.Open` to hot-swap code in a single process.
Moved to a separate branch (`dev/plugin-reload`), not resurrected, for three
concrete reasons:

- **Plugins cannot be unloaded.** Every reload leaks the previous version's
  code and state for the life of the process. A design meant to run
  indefinitely cannot afford that.
- **The `pluginpath` identity workaround makes each loaded version's types
  formally distinct.** A `Foo` loaded by plugin version 1 is not the same
  type as `Foo` loaded by version 2, even with identical source — type
  assertions and interface satisfaction break across a reload in ways that
  are hard to test for in advance.
- **Toolchain-matching requirements are a deployment landmine.** The host
  binary and every plugin must be built with the exact same Go toolchain and
  dependency versions, or loading fails at runtime. That's a fragile
  constraint to impose on every future deploy.

That attempt also drew the process boundary in the wrong place: it carved
out ~3,600 lines of protocol logic (CAP, SASL, registration, history,
tracking) as the swappable side, forcing a wide, semantically rich interface
(`moduleapi.API`) between the host and the plugin. This design inverts that.
The boundary is the connection itself — closer to "an `io.ReadWriter` with
resume" than a protocol API. `internal/keeper`'s scoping work found that the
codebase already had the seam in roughly the right place: `internal/connio`
was already a thin line-framing wrapper with no IRC semantics, and all
protocol code in `internal/uplink` already reached the socket only through
it — not scattered across the package. That made this a contained
extraction, not the wide one the plugin attempt required.

## The boundary rule

**The keeper stores what the server said and what the brain told it. The
brain decides what any of it means.**

Every design choice below either follows from this or gets flagged as an
exception to it. The keeper does line framing and exactly one piece of IRC
interpretation (responding to server `PING`) because that's socket-liveness
mechanism, not meaning. Everything else — CAP negotiation, SASL, ISUPPORT,
registration, channel state — is the brain's, full stop.

## Keeper responsibilities

- Uplink TCP + TLS: dial, handshake, hold. Never closes on brain restart.
- Line framing: split the byte stream into lines. No parsing of contents.
- `PING`: answered autonomously, at all times, brain attached or not. The
  only IRC semantics the keeper is allowed to have.
- Sequence numbers: monotonic per network, assigned to every line *read*
  from the uplink (never to what the keeper itself writes — a line only
  gets a seq if the keeper received it, not if it produced it).
- A bounded ring buffer of recent lines per network, addressable by seq.
- A blob store (design below; not yet implemented).
- Brain lifecycle: spawn (validate and live modes), detect exit, respawn per
  the failure policy (not yet implemented — needs the blob store first).

## Multi-network shape

**One keeper process holds every network's uplink. Not one keeper process
per network.**

This was not in the original design and was added mid-build once it became
clear the bouncer runs multiple uplinks per instance. The two options were:

- One keeper process per network. Isolates a keeper crash to one network,
  but a brain restart then means attaching to *N* sockets, and a validate
  pass has to succeed across all of them transactionally before cutover —
  more moving parts for isolation the design doesn't obviously need.
- One keeper process, `Manager` holding one `Keeper` instance per network.
  One socket, one handshake, one validate pass covers everything, one thing
  to supervise.

The second was chosen because the isolation the first buys was already
free: each `Keeper` instance is fully self-contained — its own ring buffer,
its own seq counter, its own epoch counter, its own socket-death handling —
and `Manager` is just a lookup map around instances that share no mutable
state. A failure on one network's uplink cannot reach another's state
*through* `Manager`, because there's nothing there for it to reach. The
per-process isolation the first option offers was already structural,
making it not worth the operational cost.

## Epoch and seq semantics

Seq is monotonic **per network**, for that network's `Keeper` instance's
whole lifetime, and never resets — including across a reconnect. Epoch is a
separate counter, incremented on every successful `Dial`, and every ring
entry carries both. This is deliberately not "seq resets per connection":

- A stale brain checkpoint from a dead TCP session can never accidentally
  match a valid seq in a later connection, because seq never repeats.
- `Since(afterSeq)` can still tell a genuinely evicted checkpoint (`ok=false`,
  a real gap) apart from `afterSeq=0` ("no prior checkpoint," always
  satisfiable from whatever's currently retained) — these are different
  things and the ring's `since` logic distinguishes them explicitly.
- The epoch on each `LineMsg` is the entry's own epoch, captured at
  ring-push time — never "whichever epoch is current when delivered." A
  line from before a drop keeps its old epoch forever, even if delivered
  after a later redial. This is what lets a client resuming after a
  close-then-redial see the epoch boundary directly on the data, rather
  than needing to infer it from delivery order.

## The keeper-only-adds-never-removes invariant

The keeper's own actions (autonomous PING response, and — once built — its
own idle-liveness probe) must never cause a line to be *removed* from the
stream the brain sees. Concretely:

- A server `PING` still reaches the brain in seq order even though the
  keeper already answered it — the keeper adds a `PONG` to the wire, it
  doesn't intercept the `PING` from what the brain gets.
- If the keeper ever originates its own liveness probe (not yet built), the
  server's `PONG` reply to it arrives as an ordinary inbound line, seq'd
  normally like anything else — the brain has to tolerate an unsolicited
  `PONG`, not have it hidden.

This keeps the ring/seq stream a faithful, complete record of the wire —
which matters because the eventual blob-store transcript is derived from
it, and a transcript with silently-removed lines would validate against a
reality that never happened.

## Wire protocol

**Length-prefixed, typed frames.** 4-byte big-endian length, 1-byte type
tag, then a JSON body. Control messages and IRC lines are different frame
types, not one shape disambiguated by inspecting the body.

**Codec: JSON, not protobuf.** The property that actually matters — an old
keeper must be able to decode a frame from a newer brain that added a field
it doesn't know about, and vice versa — is satisfied by `encoding/json`
ignoring unrecognized fields on decode by default. Verified directly
(`TestUnknownFieldIgnoredNotRejected`), not assumed. Protobuf was the
"obvious" choice but rejected: this repo has no codegen step and no
`protoc` in the toolchain, and adding one is a bigger commitment than a
local unix-socket IPC protocol needs. JSON is human-readable on the wire,
which mattered while the protocol was still being iterated on.

**IRC lines are opaque bytes, never a string.** `LineMsg.Raw` is `[]byte`.
IRC lines are not guaranteed valid UTF-8 (Latin-1 networks exist), and
`json.Marshal` silently mangles invalid UTF-8 in a Go `string` into the
replacement character — a silent corruption bug, not a crash. A `[]byte`
field is base64-encoded by `encoding/json` without inspecting content.
Verified with genuinely invalid UTF-8 through a full frame round-trip
(`TestLineMsgPreservesNonUTF8Bytes`).

**Version negotiation**: the client advertises the highest version it
supports in `Hello`; the keeper negotiates down to the highest both sides
support, or rejects if there's no overlap. Permanent contract in the sense
that an old keeper must be able to serve a new brain.

**`WriteRequest`/`WriteResult`** (added for part 3a): the only way a
live-mode client can put a line on a network's uplink — `Keeper.WriteLine`
reached over the wire, live mode only. `WriteRequestMsg.Line` is a plain
`string`, not `[]byte` like `LineMsg.Raw`: unlike a line the *server* sent,
which isn't guaranteed valid UTF-8, a line the brain sends is always
something it constructed itself from known-safe content, so the asymmetry
with `LineMsg` is deliberate, not an oversight. `WriteResultMsg` reports
`OK`/`Error` for that one write; it does not report delivery to the remote
server, only that the keeper accepted and wrote it locally.

## Attach modes

- **Validate**: the keeper delivers a read-only snapshot and *no* live
  lines, structurally — there is no code path in `serveValidate` capable of
  writing a `msgLine` frame, so this isn't a permission check that could be
  bypassed, it's an absence of the capability. Covers every network in one
  pass: cutover is atomic across the whole brain process, so there is
  nothing per-network to validate separately. A validate-mode failure on
  even one network aborts the whole reload — the one place the "per-network
  failures degrade one network" principle below doesn't apply, because a
  partial cutover isn't a real thing when the brain is one process.
- **Live**: the keeper streams every network from the client's requested
  `FromSeq` (default, absent from the map: that network's own tracked
  resume watermark — `Keeper.DeliveredSeq`, advanced only by an explicit
  `SeqAck` — not from oldest retained; see "Blob store and gap-only
  resume" below) once the client signals `LiveReady`. Full-duplex after
  that — the brain may send `DialRequest`/`CloseRequest` at any point, and
  the keeper pushes `Line` and unsolicited `NetworkEvent` (connect/
  disconnect) frames as they happen.

**Single-live-attach is enforced at the `Listener`, process-wide, not
per-network** — there is one brain, and two brains consuming any network
concurrently is the corruption case this exists to prevent. It is
first-come-first-served, not an access control; see the security section
for why that distinction matters.

## Dial/Close are wire operations, and what that dissolved

`DialRequestMsg.Config` is `keeper.DialConfig` sent verbatim — the keeper
does nothing but hand it to `Dial()`, no local storage. This was added after
the protocol already existed, once it became clear that without it, the
keeper and brain had only ever been exercised in-process — the split was a
design, not a running system, until dial control crossed the socket.

This one change retired most of an earlier config-ownership question. Once
the brain sends dial config per attempt, and the keeper already reads TLS
material from disk at dial time and never caches it (see below), almost
every field that looked like it might need to be "keeper config with
per-network override" — host, port, TLS on/off, cert/key paths, CA bundle,
TLS min/max version, bind host, SNI, `TLSNoVerify`, and (once decided)
`ReadIdleTimeout` — is just brain config sent per dial, not keeper config at
all. What's left keeper-process-level is small and was kept that way
deliberately, because the invariant worth defending is "the keeper stores no
per-network configuration whatsoever," and loosening it for one field
attracts others:

- unix socket path — restart-required, no live-reload path.
- ring buffer capacity — restart-required, same reasoning.
- the logger — reloadable via the same `internal/log` `Sink`/`Reload`
  pattern `internal/server` already uses (see Deferred work).

It also dissolved a network add/remove notification problem that looked
real for one round: since `Dial` is how a network comes to exist at all
(`Manager.EnsureNetwork` is called from the dial handler), the keeper never
needs to *announce* an addition — the brain is the source of truth for
which networks exist. `HelloAckMsg.Networks` only needs to answer the
reverse question, once, at attach time: what did the keeper already have
dialed that survived the brain's own restart. That's a deliberate asymmetry,
not an unclosed gap — no push-notification mechanism is needed for
additions, only a one-time report of what's already there.

### The read-from-disk-at-dial-time invariant

TLS material (`CertFile`/`KeyFile`/`CAFile`) is read from disk inside
`Dial`, on every call, never cached. This is what lets an operator rotate a
cert file on disk and have it picked up on the next reconnect with no
keeper involvement at all — the keeper doesn't need to know a rotation
happened. Do not "optimize" this by pre-loading certificates into a
long-lived struct; that silently breaks rotation and the breakage is
invisible until a cert expires in production, which is the worst possible
time to discover it.

### ReadIdleTimeout went in DialConfig, not keeper config

Raised as an open question, decided explicitly: `DialConfig.ReadIdleTimeout`
(0 = keeper default), not a keeper-wide value with per-network override.
The purity argument for keeper-wide (it's socket mechanism, not IRC config)
is real but weaker than the invariant it would break — networks genuinely
differ in PING cadence and idle behavior, so per-network tuning is a real
need, and a keeper-wide-with-override split reintroduces a second config
channel for exactly one field.

## Failure policy

Re-reviewed specifically for multi-network; four of five original bullets
were confirmed unaffected, one was revised:

- **No blob on attach → drop and re-register the uplink.** Built: the blob
  is per-network (one `blobStore` per `Keeper` instance), and
  `internal/server.registerNetworkLocked` seeds `Session` from whatever
  the delivered snapshot holds — including empty, for a network the
  keeper reports `Connected` but that never derived any blob state yet.
- **Validate-mode failure → abort the reload, old brain untouched.** Stays
  whole-process — see the attach-modes section above.
- **Cold start failure (no brain has ever attached) → keeper exits.** Stays
  whole-process: this is about the keeper's unix socket never having seen a
  connection at all, not about any specific network's uplink state.
- **Sudden brain crash after a successful attach → one respawn, then
  exit if it recurs.** Stays whole-process: one brain serves all networks,
  a brain crash is inherently brain-level.
- **Buffer overflow → kill the process.** *Revised, and built.* The
  original policy assumed one network per process. With several, killing
  the whole process for one network's overflow takes down every other
  network's uplink — the exact failure this project exists to prevent,
  just triggered differently. **Revised policy: a ring overflow on
  network N, while N has zero live line-subscribers, closes N's uplink
  and clears N's blob store and resume watermark** — loud, logged, one
  reconnect on one network.

Two different "overflow" scenarios exist, both now built:

1. **A live-attached client falls behind delivery for one network and its
   per-connection buffer fills.** `Keeper.SubscribeLines`'s `Overflow`
   signal fires once, and the `Listener` kills that one connection with
   an explicit error rather than silently dropping — dropping would hand
   the client a holed stream with no indication, the same gap problem
   `Since`'s `ok=false` exists to prevent one layer down. Only that one
   connection dies; every other network on it and every other attached
   brain's networks are unaffected, since each network has its own
   `Keeper`, ring, and subscriber set.
2. **The ring itself evicts independent of whether anyone is consuming**
   (a netsplit burst outpacing ring capacity with nobody attached to
   notice). Wired in `Keeper.readLoop`: a push that evicts, observed while
   `hasLineSubscribers()` is false, closes the connection itself (loudly
   logged) rather than waiting for a future attach to discover the gap via
   `since()`'s `ok=false` and have its whole live session killed over one
   network's history. The self-close resets that network's resume
   watermark to 0 in addition to the blob (both STEP-3-wiring-point
   consequences, but the watermark reset is scoped to this path only — an
   *ordinary* disconnect leaves the watermark alone, since seq is
   monotonic across epochs and a stale-but-still-in-range watermark from a
   clean disconnect remains valid for the next attach; only an eviction
   that outran the watermark specifically needs it force-reset to avoid a
   future attach requesting a seq the ring can no longer serve).

## Blob store and gap-only resume

**Status: built.** The brain pushes entries as it learns things, not at
shutdown, and a resumed attach receives *no* replay of pre-detach
traffic — only the gap since its own last acked line, with everything
else (self-nick, ISUPPORT, enabled caps, account, channel keys)
reconstructed from the delivered blob snapshot instead of from watching
old lines go by again. This module's own git history is worth reading
plainly: an early attempt at gap-only delivery (skipping replay without a
blob store to carry the state replay was standing in for) was tried,
found to break resumed-session state reconstruction, and reverted before
ever shipping — building the blob store is what made the same approach
safe to actually land.

**The resume watermark.** Each `Keeper` instance tracks `deliveredSeq`,
advanced only by an explicit `SeqAckMsg` from the brain — sent once
`Session.HandleLine` has fully finished processing a line, including any
blob push that processing triggered (`Driver.PushBlob` blocks for its own
`BlobPushResultMsg` before returning, which is what lets the ack, sent
right after `HandleLine` returns in `internal/server`'s single demux
goroutine, be correct by construction: one goroutine, push-then-return,
ack-after-return, every time, no locking between the two needed). A brain
that crashes between receiving a line and finishing its blob push simply
never sends that line's ack — the watermark never passes it, and the next
attach receives that line, and everything after it, again. `HelloMsg
.FromSeq` absent for a network means "start from `DeliveredSeq`"; an
explicit entry (including an explicit `0`) overrides it, an escape hatch
for tooling, not something a normal attach needs to set.

The brain pushes entries as it learns things, not at shutdown: parse a
`005`, emit it immediately (`internal/session/state_numerics.go`'s
`stateWelcomeNumericLocked`); join a channel, emit it
(`internal/session/downstream.go`'s `persistChannel`, the same call site
that already persists it to SQL). Nothing durable exists only in brain
memory.

Each entry carries a brain-chosen key and a mode:

| mode | semantics | built keys |
|---|---|---|
| `append` | accumulate in order under this key | `isupport` |
| `replace` | latest-wins | `cloak`, `self-nick`, `account`, `channel:#foo`, `caps` |
| `delete` | remove the key | `channel:#foo` (on `PART`/self-`KICK`) |

The keeper matches on the key string and applies the mode
(`internal/keeper/blob.go`). It never inspects the value — that's what
keeps the store bounded on a long-running session without the keeper
understanding IRC. (The version-tag idea raised below for a differently-
built brain to recognize a format it doesn't understand is not built —
values today are a fixed, package-internal encoding, JSON for the
structured keys and plain bytes for the scalar ones; revisit if this
protocol's cross-version compatibility promise ever needs to extend to
blob value shapes, not just frame shapes.)

**Ordering requirement**: push the derived entry *before* advancing the
consumed seq. Built via the resume-watermark design above:
`Driver.PushBlob` blocks for confirmation, and the `SeqAck` for a line is
only sent after that line's full processing (including any blob push)
returns — so a brain that acts on a line and dies before its blob push
lands never advances the watermark past it, and the next attach receives
that line again, not a gap. Duplicate entries on replay are harmless if
keyed idempotently (every built key is); a gap is not — this is why the
ack, not the push, is what's allowed to lag.

### The transcript is not a snapshot of the registration window

State the general rule once: entries derived from server state must be kept
current for the life of the connection, not captured once during the
registration handshake and left to go stale. Two instances of this, not
two separate rules:

- **ISUPPORT** — `append` mode on the `isupport` key, built. `005`
  is conventionally a registration-time numeric, but the mode doesn't
  assume that; if a server ever sent it again mid-session, the blob would
  correctly keep accumulating rather than treating registration's `005`
  lines as the final word.
- **CAP** — built (below). `ACK`/`DEL` are not registration-only
  either — a script or the user can `REQ` a capability well after
  connection, and the server can `DEL` one unsolicited at any point in the
  session. The blob entry tracks this for as long as the connection
  lives, not just through `CAP END` — `internal/session/upstream.go`'s
  `handleCAPLine` pushes the resolved set on every `ACK`/`DEL` that
  actually changes it, pre- and post-registration alike.

### CAP state: what goes in the blob, and why it can't wait for `CAP LIST`

**The blob carries the set of capabilities currently *enabled* on this
connection — not what the server offers.** `CAP LS` and `CAP NEW`
advertise availability; neither means a capability is active. Only `ACK`
does. So the update rule, applied for the life of the connection, not just
through registration:

- **`ACK` adds** — at registration, and later, whether the `REQ` that
  earned it was triggered by a `CAP NEW` offer or issued by a script or
  the user.
- **`DEL` removes** — unsolicited, and requires no acknowledgement. The
  removal path is driven purely by observing `DEL`, unlike the addition
  path, which requires seeing our own `REQ` acknowledged.
- **`NEW` and `NAK` change nothing.** An offer is not a negotiation, and a
  refusal leaves the enabled set exactly as it was.

Concretely: keep updating the blob entry on every `ACK` and `DEL`, not just
up to `CAP END` — a small addition at an existing call site, not new
machinery.

**Why this can't be deferred to `CAP LIST` on attach.** `CAP LIST` does
report the enabled set, so recovering it that way looked viable at first —
it isn't, because of ordering. A resumed brain must begin parsing from
`seq+1` immediately on attach. If it fires `CAP LIST` and waits for the
reply before it knows the enabled set, it's parsing the backlog with an
unknown capability set for the length of that round trip — and lines
carrying `@time=…;msgid=…` message-tag prefixes get **mis-parsed rather
than rejected** in that window. Silent corruption, not a visible failure,
which is the worse of the two available alternatives:
- carry the set in the blob and have it at attach time with zero round
  trip (chosen), or
- buffer every line until `CAP LIST` returns (rejected — a network round
  trip gating when replay can start is exactly the kind of dependency the
  blob store exists to avoid).

This keeps CAP consistent with the standing design decision rather than
carving out an exception for it: the transcript is source of truth, and
`CAP LIST`/`WHOIS` are opportunistic cross-checks — fire them when
convenient, log any disagreement with the blob, never depend on either for
correctness.

**One deliberate exception to the raw-bytes rule: store the resolved set,
not the sequence of `ACK`/`DEL` lines.** Everywhere else, the transcript
principle is "the keeper stores raw bytes, the brain interprets them" —
this is the one place that inverts it, so it's recorded as a deliberate
exception, not drift. Reasoning: the enabled-cap set is small and bounded,
the `ACK`/`DEL` resolution rules above are unambiguous with no
interpretive judgement left to preserve by keeping the raw sequence, and
storing the transitions themselves would grow without bound over a long
session while forcing every resumed brain to replay the whole sequence
just to arrive at the same answer a single resolved value already gives
it directly. `replace` mode, one key.

**Clearing is keeper-side and unconditional.** Wired in
`internal/keeper/keeper.go`'s `readLoop`, at the exact line where
a network's state transitions to `NotConnected` — deliberate or not. Any
state derived from an epoch (ISUPPORT, CAP, identity, channel list) is
stale the instant that epoch's connection is gone, whether it ended because
the brain asked for `Close` or because the socket died on its own; the next
`Dial` starts a new epoch and a blank transcript. This must not be gated on
"deliberate vs. died" — a deliberate close followed by a redial to the same
network still needs fresh registration, so the old blob is exactly as
invalid either way. (The actual clear call must happen after releasing the
keeper's internal mutex, not inside that critical section — the mutex also
guards `Dial`/`Close`/`State` and shouldn't be held across a blob-store
call.)

**Channel keys had a close analogue already in the bouncer, and the blob
wiring reused it exactly as expected**: `internal/session`'s
`persistChannel`/`persistRemoveChannel` (called from `state.go` at the
exact points a key is learned or a channel is parted) each got one
`s.pushBlob` call added, not new state-capture logic.

**On resume, the blob snapshot is the only source for this state, not a
fallback confirmed by replay.** `internal/server.registerNetworkLocked`
seeds a resumed network's `Session` (`Session.SeedFromBlob`) from the
delivered snapshot before any gap-only line reaches it, then calls
`completeRegistration` directly — there is no replayed registration burst
left for `Session` to self-detect completion from. Known gap, not fixed:
nick-recovery state and the source-prefix from the uplink's own `001`
(cosmetic, used for synthetic message sources) have no blob key and are
not restored on resume.

## Registration state machine (`internal/registration`)

New package, not a reshaping of `internal/uplink` — the old code stays
working throughout, so the two can be compared and the switch happens once
the new path is proven, not by construction.

**Pure.** `Step(State, Input) (State, []Action)`. No I/O, no sockets, no
timers, no writes. `Input.Replay` marks whether this message is replayed
transcript rather than a line just received live — `Step`'s own logic never
branches on it, which is the property that makes the replay path provably
identical to the live path: same function, same code, different flag.
`Replay` is copied onto every emitted `Action` purely so a caller can gate
execution of that action's real-world side effect (auto-join, on-connect
perform, "connected" notices to downstream clients) at one place, rather
than checking a flag at every effect call site scattered through the
codebase. `Step` itself performs none of those effects — it only says
"registration is done," never "run the perform script."

**The transcript corpus is the asset that outlives this file.** Ten real
captures (`testdata/registration/`) — ergo (including one deliberately
provoked into a real CAP NAK by requesting a nonexistent capability — the
NAK itself is genuine ircd behavior, only the trigger was engineered), a
genuine connection to the Undernet network, and six more real ircd
implementations reached via `docker/ircd`, one of which (`ircd-irc2.txt`)
turned out to be old enough to never send a single CAP line at all — a real
no-CAP registration path, not a hypothetical. All kept as raw bytes with
capture timings, not normalized or synthesized. This is what proves
replay-identical-to-live is real rather than asserted
(`TestReplayIdenticalToLive` runs every fixture twice, once live and once
replay, and diffs the final `State`), and it's the regression net for every
future ISUPPORT or CAP change: a new server quirk becomes a new fixture,
not a new hand-written test case guessing at what the server might send.

**What's built**: CAP LS/REQ/ACK/NAK negotiation (including real multi-line
302 LS and the real-NAK and no-CAP paths above), welcome numerics 001–005
(`irc.ISUPPORT` reused directly, not reimplemented), completion on 376/422,
the SASL `AUTHENTICATE` exchange (PLAIN, EXTERNAL, and SCRAM-SHA-256 — the
last verified with a genuine `xdg-go/scram` server conversation on the
other end, real crypto on both sides, not a mocked challenge), and the
nick-collision ladder retry (432/433/437).

**Why SASL and the nick ladder are tested synthetically, not from the
corpus**: no real server reachable during transcript capture required SASL
or produced a nick collision — every reachable nick was free, and no
server offered credentials to test against. Table-driven tests
(`registration_sasl_test.go`, `registration_nick_test.go`) hand-drive `Step`
against the same numerics/`AUTHENTICATE` sequences `internal/uplink`'s own
tests use as reference behavior instead. This is the accepted fallback
specifically because a real capture wasn't obtainable, not a shortcut taken
in place of one — prefer a real capture over a synthetic test whenever one
becomes reachable.

## IPC security

The keeper↔brain unix socket. The threat is not sniffing — a unix socket's
data path isn't exposed to other processes. **The threat is another local
process connecting and speaking the protocol.** It would get every line
from every network including private messages, the ability to inject lines
as the user, and — because the brain sends `DialConfig` — the ability to
make the keeper connect anywhere and load any keypair the user can read.
The keeper becomes a confused deputy.

Two independent layers, both in `internal/keeper`:

1. **A 0700 directory the socket lives inside** (`ensureSocketDir`).
   `net.Listen("unix", path)` creates the socket file itself with
   `0777 &^ umask` — there is a real window between creation and any
   `chmod` — so the directory is what actually gates access, never the
   socket's own mode. `Serve` refuses to bind rather than silently loosen
   or tighten a pre-existing directory's permissions; a wrong mode is a
   deployment error to surface, not correct out from under whoever set it
   up. **Never `/tmp`**: world-writable means the directory's own
   permissions protect nothing, and unlinking a stale socket there on
   startup races a symlink an attacker placed at that path.
   `$XDG_RUNTIME_DIR` (`/run/user/UID`, already 0700, already per-user,
   already cleaned up by the OS on logout) is the natural home
   (`DefaultRuntimeDir`).
2. **Peer credential verification** on every accepted connection
   (`SO_PEERCRED` on Linux, `LOCAL_PEERCRED` on BSD/macOS, via
   `syscall.RawConn.Control`), plus a pre-connect ownership check on the
   client side (`verifySocketOwner`). This is defence in depth, not the
   primary control — the directory should already have prevented an
   unauthorized connection — but it catches a misconfigured deployment
   (directory permissions loosened after the fact) rather than silently
   trusting the directory forever. The Linux implementation is tested
   directly against a real socket pair. The BSD/macOS implementation
   (`peercred_bsd.go`) is written against the documented `LOCAL_PEERCRED`
   API and cross-compiles cleanly (`go build`/`go vet` under
   `GOOS=darwin,GOARCH=arm64`, `GOOS=darwin,GOARCH=amd64`,
   `GOOS=freebsd,GOARCH=amd64`) — this catches syntax errors, wrong struct
   field names, and wrong syscall signatures, but does not prove the code
   works. Status is "compiles, never run," not "never compiled": it still
   needs verification on an actual BSD/macOS host or in CI before being
   trusted. The listener's expected UID is injectable
   (`WithExpectedUID`/`ListenerOption`), which made the rejection path
   itself testable end-to-end without a second real user account
   (`TestUIDMismatchRejectedEndToEnd` sets the expected UID to
   `os.Getuid()+1` and confirms `Attach` is genuinely rejected through the
   whole path — accept loop, `SO_PEERCRED` retrieval, comparison,
   rejection — not just the two ends separately).

**Never a Linux abstract socket** (`@name`, no filesystem presence). Any
process in the network namespace could connect regardless of directory
permissions — a hard no, not offered even as a convenience option.

**The listener is never TCP-capable**, not behind a flag. `Serve` takes a
filesystem path and hardcodes `net.Listen("unix", ...)`; there is no
network-type parameter to change. Every guarantee above is a property of
unix sockets specifically — a `-listen tcp` flag added later "for
convenience" would silently void all of them. There's a comment to this
effect directly on `Listener` aimed at a future session that might add one
helpfully.

**No line contents logged at normal levels.** Audited: current logging in
`internal/keeper` and `cmd/keeper-soak` is metadata only (state, epoch,
error strings, counts) — no raw line ever reaches a log call. `connio.Conn`
supports raw-traffic debug logging (`SetLogger`) but the keeper never wires
it up; there's a comment at the one call site (`connio.New` in `Dial`)
explaining why not to add it casually.

**Two properties written down explicitly so they aren't assumed later:**

- **Single-live-attach is not a security control.** It's
  first-come-first-served — a hostile local process that passes the UID
  check and connects first either takes the stream or denies the real
  brain. Fine once UID checking exists (it does now); just don't mistake
  the exclusivity itself for protection.
- **Same-UID separation is not a security boundary.** Another process
  running as the same user can `ptrace` the keeper or read its memory under
  most default configurations. The protection here is against *other
  users* on a shared host — that's the threat this design is scoped to,
  and it's worth saying so explicitly rather than letting "peer
  credential checking" sound like it implies more than it does.

## Working discipline: the revert-and-confirm policy

**For any test written to prove a specific fix or a specific property,
revert the change and confirm the test fails before trusting that it
passes.** This is policy, not habit — it has been applied four times across
this project and caught two tests that looked like they proved something
but didn't:

- A goroutine-leak test (`TestRemoveNetworkWhileLiveStreamingStopsFanIn`)
  passed with the fix reverted, because `Keeper.Close` always stops a
  *different* goroutine (the read loop) regardless of the fan-in leak the
  test was meant to catch — a naive "count dropped by at least one"
  assertion was satisfied by the wrong goroutine exiting. Required
  measuring the baseline with the goroutine-under-test already running and
  asserting a drop of at least two, not "any drop."
- `TestReplayIdenticalToLive` — the load-bearing claim of the entire replay
  design — was verified non-vacuously by injecting a `!Replay` branch into
  `Step` and confirming all nine transcript fixtures caught the divergence
  with the actual differing states printed.
- A hang-vs-fail-cleanly gap in `TestServeRefusesBadSocketDirPermissions`:
  disabling the permission check didn't fail the test, it hung the whole
  test binary, because `Serve` proceeded to `Accept()` forever with nothing
  to connect. The fix wasn't to the security code — it was already
  correct — but to the test, which needed a bounded wait around `Serve`
  so a real regression would fail fast and readably instead of timing out
  the whole suite.

The pattern each time: write the test, deliberately break the thing it's
supposed to prove, confirm the test actually notices, restore the fix,
confirm green again. A test that appears to prove a fix but doesn't is
worse than no test, because it retires the concern without earning it.

## Soak testing

Three long-running instances via `cmd/keeper-soak` (deliberately kept out
of `internal/keeper` — throwaway test infrastructure, not part of the
keeper API): one against a local `ergo` fixture over plaintext
(`docker/ircd`, port 6667), one against the same `ergo` fixture over TLS
(port 6697, self-signed cert, `-tls-no-verify`), and one against a real
remote network. All log state/epoch/goroutine-count/ring-occupancy on an
interval to a size-capped rotating log.

The claim under test is **flat goroutine count over days**, sampled
alongside epoch — epoch climbing while goroutines stay flat is the healthy
trend; goroutines climbing at all, regardless of epoch, is the leak signal.
`cmd/keeper-soak` supports `SIGUSR1` to force a deliberate close-and-redial
on demand (reusing the same `redialLoop` an unexpected drop triggers), for
exercising the reconnect path on a network that might otherwise sit
connected for the whole soak. Observed once for free during this work: an
unrelated `ircd`-tagged test run against the same local `ergo` container
knocked both local soak instances' connections down (`EOF`); both detected
it, redialed, and re-registered within 5 seconds, goroutines still flat,
zero drops — the reconnect path exercised by a real disconnect, not just a
deliberate `SIGUSR1`.

**TLS gap: closed.** `docker/ircd/ergo.yaml` now configures a TLS listener
on 6697 (cert generated via `openssl req -x509`, though ergo's container
ends up serving its own auto-generated self-signed cert regardless — moot,
since soak and test clients dial with `-tls-no-verify`/`TLSNoVerify`); a
third soak instance runs against it. A clean plaintext run was never TLS
confidence; this closes that gap rather than continuing to repeat the
caveat.

## Part 3a: wiring `internal/registration` to the keeper (`internal/brain`)

New package, no consumers yet — `internal/uplink` is untouched, nothing in
the existing bouncer depends on `internal/brain`. It owns no protocol logic
(that's `internal/registration`) and no transport (that's `internal/keeper`
and its wire protocol); `Driver` only connects the two:

- **`registration.Start` had to be added.** `registration.Step` only ever
  reacts to a server-sent message — nothing produces the opening `CAP LS
  302`/`PASS`/`NICK`/`USER` lines, because in the old `internal/uplink`
  code those were written directly by `register()` before the read loop
  ever started, outside the state machine entirely. This was a real gap,
  not a design choice to preserve: without it, `Driver`'s first live test
  hung forever waiting for a `Line` event that would never arrive, because
  nothing had ever sent `NICK`/`USER` to the server. `Start` is a pure
  function (no `State` in or out) mirroring `uplink.go`'s `register` line
  for line. It also means the whole 10-fixture corpus had only ever
  exercised *reaction* to server lines — the client's own opening half of
  registration was untested and invisible until this was built, and would
  otherwise have stayed invisible until 3b surfaced it as "the new path
  connects to nothing."

### Invariant: `Start` must never fire during replay

Replay means folding a captured transcript into a connection that is
**already established and, per the transcript, already registered or
mid-registration** — it exists so a code reload can resume from a keeper
blob without redoing work the server already saw. If `Start` fired on a
replay run, the brain would send `CAP LS`/`NICK`/`USER` down that already-
live socket on every single reload — best case ignored, realistically a
spurious nick change, a CAP renegotiation, or a server-side error, on every
reload of a running bouncer.

This is the same *class* of hazard as the perform-script-must-not-fire-on-
replay problem `Input.Replay` already exists to solve, but that mechanism
does not reach it: `Replay` rides on `Action`s that `Step` produces in
reaction to an `Input`, and `Start` is not a `Step` call — it has no
`Input` to carry the flag on. Bolting a `Replay`-flagged output onto
`Start` (the way `Step`'s other actions work) would not be enough either:
that only marks the danger, it doesn't prevent it, and it leaves every
future call site responsible for remembering to check the flag before
acting on `Start`'s output.

**The rule, enforced at the lowest layer, not by caller discipline:**
`Start(nick, pass, username, realname, replay bool) []Action` takes
`replay` directly and returns `nil` — not `Replay`-flagged actions, `nil`
— whenever it's `true`. A caller can pass `replay` unconditionally, every
time, without needing a separate "and also don't call `Start` at all
during replay" rule to remember. Pinned two ways: `TestStartIsNoOpDuringReplay`
checks it directly and in isolation; `TestReplayIdenticalToLive` checks it
against the full corpus — a replay run's action log must have *zero*
opening actions, not opening actions flagged `Replay=true` (verified via
revert-and-confirm: removing the guard fails `TestStartIsNoOpDuringReplay`
and all 10 corpus fixtures of `TestReplayIdenticalToLive`, not silently).

`internal/brain.Driver` has no resume/replay path yet — that's blob-store-
driven work, not built — so its one call site
(`Driver.StartRegistration`) hardcodes `replay: false`, with a comment
that any future resume support must go through a different entry point
that never calls `StartRegistration` at all, not through this one with a
flag flipped. **Whichever of the three options gets picked when resume is
actually built — a bool, two named entry points, or `Driver` branching on
whether the blob store held a transcript — this invariant (replay never
produces opening actions) is the contract to preserve, not the specific
mechanism above.**
- **`Driver.Run`** is the only reader of a live `AttachClient`'s event
  stream (`AttachClient` has no fan-out). It steps `Line` events for any
  tracked network through `registration.Step`, turns `ActionSend` into a
  real `SendWrite` over the wire, and republishes `DialResult`/
  `CloseResult`/`WriteResult`/`NetworkEvent` on its own channels so a
  caller issuing a `Dial`/`Close` on the same client has somewhere to read
  the result, since `Driver` owns the only read.
- **`Driver.StartRegistration`** sends the `registration.Start` lines for a
  tracked network — call it once a `Dial` is confirmed connected (there's
  nothing to write to before then). Kept separate from `RegisterNetwork`
  (which only sets up tracking state) deliberately: `Driver` has no
  standing notion of "this connection has already had its opening lines
  sent," and a reconnect legitimately needs them resent.
- **Proven two ways**, per the revert-and-confirm policy: a fake-server
  unit test (`driver_test.go`, `TestDriverRegistersOverKeeperWire`) that
  genuinely failed (hung on a real timeout) before `Start`/
  `StartRegistration` existed and passes with them — the fix and the
  failing-without-it check happened as one sequence, not a retrofit — and
  a real `ircd`-tagged live test (`driver_live_test.go`) that dials real
  `ergo` (plaintext and TLS) and the real remote network through the full
  wire path: `Manager`+`Listener`+`AttachClient`+`Driver`, no shortcuts
  through `Keeper`'s local API. All three completed registration and
  reached `PhaseComplete` with `GotWelcome`. (Rerunning the `ergo` cases
  immediately after `internal/keeper`'s own `ircd`-tagged suite hit ergo's
  own connection-rate throttle — a real ircd being an ircd, not a defect;
  both had already passed cleanly in isolation.)

Explicitly not done here, and not to be conflated with this: swapping
`internal/uplink` itself over to the keeper (part 3b) — see the warning
below.

## Pre-3b regression net

`internal/uplink` is a working, `internal/registration` is a from-scratch
rebuild of the same logic verified against ten real transcripts — before
cutting over the answer to "does the corpus actually cover what the old
code does" needs to be written down, not assumed. Read against
`register()`, `sasl.go`, `nick.go`, and `handleCAP` in `internal/uplink`:

**Real gap, fixed — no reaction to disconnect during registration.**
`internal/uplink.register()` enforced a flat 60s no-traffic deadline on
every single `ReadLine` call during the handshake; silence past that
failed registration outright. The new path had nothing equivalent at the
`Driver` layer. Closed — see the "Driver registration failure paths"
section below for what was built (a brain-owned registration deadline plus
`NetworkEvent{Disconnected}` resolving a stuck `State`) and a keeper-level
ordering bug the fix surfaced along the way.

**Correctly out of scope for the corpus, but not yet built anywhere in the
new path** — the 10 fixtures stop at 376/422 by construction, and
`registration.Step` is deliberately registration-only, so none of this
being absent from `internal/registration` is a defect in that package.
It's a punch list for whatever 3b's ongoing (post-registration) brain loop
turns out to be:
- **ISON-based nick recovery** (`internal/uplink/nick.go`'s
  `nickRecoveryLoop`, 30s ticker, primary/alt reclaim via `ISON`/`303`) —
  only the pure ladder functions (`buildNickLadder`/`nextNickInLadder`)
  were ported; the whole ongoing recovery loop was not, because it isn't a
  registration-phase concern.
- **User-mode tracking** (221 `RPL_UMODEIS`, unsolicited `MODE`,
  `u.umodes`/`UserModeString`) — spans the entire connection lifetime, not
  just registration, and answers downstream clients' own-mode queries.
  `registration.State` has no umode field, correctly, since this outlives
  registration.
- **Post-registration CAP traffic** (`handleCAP`'s `u.Registered()`
  branch — `CAP NEW`/`CAP ACK` arriving after connection, `OnCapsChanged`)
  — `stepCAP`'s `default: // NEW, DEL, LIST` case correctly no-ops these,
  since dynamic post-registration cap changes are a session-level concern,
  not a registration one.

**Checked and confirmed not a gap:**
- **Server `PING` during registration** — handled by the keeper
  autonomously at the transport layer regardless of registration state
  (see the keeper-only-adds-never-removes invariant above); `Step`
  correctly falls through to its no-op default on `PING`, because the
  keeper already answered it before the line ever reached `Step`.
- **432/433/437 nick-collision handling**, including the easy-to-miss
  437-is-overloaded case (pre-welcome nick collision vs. post-welcome
  channel-unavailable, disambiguated by `Param(0) == "*"`) — verified
  faithfully ported in `stepNickError`, which checks both `GotWelcome` and
  `Param(0)` explicitly, matching `register()`'s branch exactly.
- **900 (`RPL_LOGGEDIN`) account tracking** — deliberately not stored in
  `State`; `stepLoggedIn`'s doc comment states this explicitly (session-
  level code tracks the account post-registration, mirroring where
  `internal/uplink` stores `u.account` too — it's set from `900` but read
  nowhere in the registration path itself).
- **The ident-lookup wait some old ircds need** (bahamut/ircd-irc2 took up
  to 70s in the real captures — see the corpus-building notes) — `Step`
  has no timing logic at all, so a slow ircd doesn't stress it directly;
  what matters is whether the *caller's* timeout tolerates that wait.
  Keeper's `defaultReadIdleTimeout` is 10 minutes, comfortably above the
  slowest real capture, so no default-timeout regression here — but see
  the disconnect-handling gap above: a generous keeper-side timeout
  doesn't help if `Driver` wouldn't notice the eventual disconnect anyway.

## Driver registration failure paths

Closes the one must-fix item from the regression net above. Two related
gaps, built together:

- **`DefaultRegistrationTimeout` (90s), armed by `Driver.StartRegistration`,
  disarmed on any terminal phase.** Total-since-`Start`, not an idle reset
  — it isn't renewed by lines arriving, only cleared once `handleLine`
  observes `ActionRegistered`/`ActionFailed`, or by firing itself
  (`armDeadline`/`disarmDeadline`/`failRegistration` in `driver.go`). This
  is deliberately a brain decision, not a keeper one: the keeper's own
  `ReadIdleTimeout` is a socket-liveness backstop with no notion of
  "registration" at all, and stays exactly that — a server that trickles
  unrelated traffic without ever reaching 001/376/422 would sit well
  within a 10-minute socket backstop indefinitely. 90s, not
  `internal/uplink`'s old 60s: the real transcript corpus includes a
  genuine ~70s ident-lookup wait on two old ircds (bahamut, ircd-irc2) —
  60s would have failed both. 90s keeps comfortable margin above the
  slowest real capture without approaching the keeper's 10-minute
  backstop.
- **`Driver.Run` reacts to `NetworkEvent{Disconnected}` during
  registration** by resolving the pending `State` to `PhaseFailed`
  (`handleNetworkEvent` → `failRegistration`), instead of leaving it stuck
  with no `Result` ever sent. `failRegistration` is guarded on `Phase` so
  it's safe to call from either this path or the deadline (or both,
  racing) without double-firing a `Result` over an already-terminal
  `State` — proven by `TestDriverNoSpuriousResultAfterCompletion`, whose
  revert-and-confirm failure mode was more interesting than the one it was
  written for: disabling the guard didn't just risk a *second* `Result`,
  it let a network's own later, ordinary disconnect race ahead of and
  clobber its own already-successful completion.

**A keeper-level ordering bug surfaced while testing the disconnect
path, and got fixed as part of this, not deferred.** `Keeper.readLoop`
always calls `publishLine` for a connection's final line strictly before
`publish(EventDisconnected)` for that same connection — but
`liveSession.fanInNetwork` multiplexes `SubscribeLines`' `Lines` channel
and `Subscribe`'s `events` channel through one `select`, and Go's choice
among simultaneously-ready cases is unspecified, not FIFO across two
independent channels. Before this round, nothing downstream cared about
the relative order of a line and a disconnect event, so the reordering was
invisible. It stopped being invisible the moment `Driver` started treating
`NetworkEvent{Disconnected}` as a registration-failure signal: a server
that sends its final welcome line and disconnects immediately after (no
deliberate gap — plausible, not just adversarial) could have that
disconnect event race ahead of the completion line and spuriously fail a
registration that had actually already succeeded. Fixed in
`fanInNetwork`: before forwarding an event, drain whatever's already
buffered on `Lines` first, restoring the order `readLoop` actually
published in. `TestWireFinalLineDeliveredBeforeDisconnectEvent` pins this
directly (50 redial iterations on one live connection, asserting the line
always precedes its connection's own disconnect event).

*Revert-and-confirm note, reported plainly rather than glossed over:* the
deadline and disconnect-handling fixes both reverted cleanly (test fails
without the fix, passes with it, every time). The fan-in ordering fix did
not revert cleanly in the final test shape — a lighter, heavier-weight
version of the test (fresh `Manager`+`Listener` per iteration instead of
one reused connection across redials) did hit a real anomaly with the fix
disabled, but that version was itself resource-heavy enough to be a
confounding variable, and the corrected, lighter test (single connection,
50 redials) passed 50/50 with the fix disabled in this environment. The
fix is kept — it is correct by construction from `readLoop`'s strict
publish order and Go's documented `select` semantics, and it is harmless
even if the race turns out to be effectively unreachable on this
machine's scheduler timing — but this is recorded as a defensive fix
confirmed by code reading, not by a reproduced failure, which is a
weaker claim than every other revert-and-confirm result in this document
and should be read as such.

## Part 3b-i: mechanical substitutions (`internal/uplink` still untouched)

The defining property of this half: the tree builds and the bouncer works
with `internal/uplink`'s old read loop fully intact throughout. Nothing
about `internal/uplink` changed in this round; every item below is new
keeper/brain-side infrastructure that 3b-ii will eventually point the old
callers at, not a modification of the old callers themselves.

### Shutdown vs. disconnect: `Keeper.QuitClose`

`internal/uplink`'s `writeQuit` served two purposes at once by virtue of
being the only shutdown path: bouncer-wide shutdown and a deliberate
per-network disconnect both funneled through it, and both closed the
connection. After the split those are genuinely two different operations,
and conflating them is the one mistake that costs everything — see the
warning from the round that first flagged this: *"every reload sends QUIT
and drops every uplink, the exact failure this project exists to
prevent."*

- **Disconnect from a network** (deliberate — config change, user detach):
  `Keeper.QuitClose(line, timeout)` writes `line` with a bounded write
  deadline, then closes the connection regardless of whether the write
  completed — one wire round trip (`QuitCloseRequestMsg`/
  `QuitCloseResultMsg`), not a `WriteRequest` followed by a `CloseRequest`,
  because bounding the whole write-then-close sequence with a single
  deadline is what `writeQuit` got for free by reaching directly into the
  raw `net.Conn` it owned (`c.Underlying().SetWriteDeadline(...)`); the
  brain has no socket of its own to do that on, so the keeper does it on
  the brain's behalf. `internal/brain.Driver.QuitNetwork` is the one and
  only Driver method that can produce this — its doc comment says so
  explicitly, in the same terms as this section.
- **Brain going away** (a code reload — the entire reason this project
  exists): holds the socket, sends nothing, calls nothing. This isn't
  implemented as a special case anywhere — it's the *absence* of a call to
  `QuitNetwork`/`QuitClose`. `Driver.Run` returning (its `ctx` canceled, or
  the underlying connection closing because the process is actually
  exiting) never reaches either.
- **Proven directly**: `TestBrainExitSendsNoQuit` cancels `Driver.Run`'s
  context and closes the client connection (the real mechanism, since
  `ctx` cancellation alone cannot unblock a goroutine parked in
  `client.Next()` — a plain network read with no `ctx`-tied deadline; this
  is now documented on `Run`'s own doc comment, found by this test
  failing against its first, less realistic version) with no
  `QuitNetwork` call anywhere in the test, then confirms two things: the
  fake server received nothing at all (not even a byte, let alone `QUIT`)
  in a bounded window, and the keeper's `Manager` still reports the
  network `Connected` afterward — the uplink genuinely survived.
  Revert-and-confirm was attempted at two different injection points
  inside `Run` (reacting to `ctx.Err()`, and reacting to `client.Next()`
  erroring) and neither could make the test fail: both injection points
  only run after the connection is already closed by the test's own
  realistic simulation of process exit, so a wire write attempted from
  either one is structurally a no-op — there is no connection left to send
  QUIT on. That is itself informative, not a failure of the test: it means
  the danger this guards against can only come from a *caller*
  deliberately invoking `QuitNetwork` as part of a (not-yet-written)
  shutdown sequence, before closing the connection — not from anything
  `Driver` could do to itself. There is no such shutdown-orchestration
  code yet (no `cmd/brain` equivalent exists); the guarantee to re-verify
  once that code exists is "shutdown orchestration never calls
  `QuitNetwork` in response to a reload signal," and that will need its
  own test written against that future code, not this one.

### `flood.go`'s liveness check → keeper backpressure (signal built, pacer not)

`internal/uplink/flood.go`'s `enqueueFlood` read `u.conn == nil` before
queuing a line — a raw socket check the brain can't do, since it has no
socket. Two separable pieces, deliberately not conflated:

- **The backpressure signal** (built, tested): `WriteResultMsg` gained
  `Refused bool`. `Refused=true` means there was nothing to write to at
  all — either the network is unknown to the `Manager`, or it's known but
  has no live connection (`Keeper.WriteLine`'s `errNotConnected`) — the
  same category `u.conn == nil` was checking for, and the right reaction
  is the same one that check produced: don't send this line, don't retry
  it, wait for reconnection. `Refused=false` with `OK=false` means a write
  was attempted on a connection that appeared live and failed anyway
  (e.g. the socket died in the gap between a pacer's liveness assumption
  and this write reaching the keeper) — ordinary connection-loss handling
  applies, not a pacing decision. `TestWireWriteRequestRefusedWhenNotConnected`
  and `TestWireWriteRequestUnknownNetwork` pin both cases the signal
  distinguishes, both revert-and-confirmed.
- **The paced writer itself** (not built): reimplementing
  `floodDrainLoop`'s actual behavior — a per-network FIFO queue, an
  `internal/flood.ByteBucket` (already a pure, dependency-free package;
  reusable as-is, no changes needed) gating each send, one write in
  flight at a time, backing off on `Refused` — is real, non-trivial new
  logic, not a mechanical substitution, and building it surfaced a real
  design question worth resolving deliberately rather than rushing: a
  paced writer needs to read *its own* write's `WriteResultMsg` back to
  know when to send the next queued line, but `Driver.WriteResults()` is
  one shared channel with exactly one intended reader per the existing
  "Driver owns the only read of the underlying client" model — and
  `handleLine` already consumes `SendWrite` on that same client for
  `registration.Action{Kind: ActionSend}`. Two independent producers of
  `WriteRequest` wanting their own correlated results is a real
  architectural question (per-request correlation IDs on the wire?
  `Driver` itself owns pacing and exposes a higher-level `SendPaced`
  instead of exposing raw `WriteResults()` to two callers?) to answer as
  its own decision when this is actually built, not a gap to paper over
  with a stub.

### `dial()` moving out: already done, confirmed not regressed

Checked, not built: `internal/brain` has zero dial-related functions —
`grep`-confirmed. There was never a duplicate of `internal/uplink.dial()`'s
TLS-resolution logic to delete, because `Driver` has sent `keeper.DialConfig`
over the wire from the moment `Driver` existed (part 3a); the "moves out"
step was already complete before this round started. `internal/uplink.dial()`
itself is untouched, exactly as 3b-i requires. Re-confirmed the invariant
this depends on still holds: `Keeper.buildTLSConfig` reads certificate/key/CA
material from disk fresh on every call, with its own doc comment saying not
to hoist it out of the per-dial path — unchanged.

## Part 3b-ii, stage 1: keeper process orchestration

Checking what "cut the read loop over" (3b-ii proper) actually requires
turned up a missing prerequisite: there was no real, separate keeper
process anywhere in this repo. `cmd/keeper-soak` is throwaway test
infrastructure holding a `Keeper` in-process, never over the wire.
`internal/daemon`'s only re-exec use was gobnc backgrounding itself
(`Reborn`); SIGHUP only rehashes config; `stop` was a plain SIGTERM that
kills the whole process, uplinks included. Nothing in `cmd/gobnc`/
`internal/daemon`/`internal/server` knew how to find, start, or attach to
a keeper. This stage builds that missing piece as its own reviewable
checkpoint — proven live across two genuinely separate OS processes —
without touching `internal/uplink`, `internal/server`, or `cmd/gobnc`'s
real startup path. Nothing in the actual bouncer uses any of this yet;
that's the next stage.

### `cmd/keeper` — the real keeper binary

Thin `package main`: wires `keeper.NewManager` + `keeper.NewListener` to a
socket path, serves until SIGTERM/SIGINT. Writes its own pidfile via
`internal/daemon.WritePidFile` (already exported and generic — path + pid,
not gobnc-specific — no need for a keeper-specific pidfile helper).

Not daemonized by the binary itself — it always runs in the foreground of
whatever process context it's given. Detaching it (Setsid, redirected
stdio) is the spawning caller's job (`internal/keeperboot`, below), not
something duplicated inside `cmd/keeper`.

**Keeper exit is graceful toward every upstream it's holding**, unlike
brain exit. This is the mirror image of the shutdown-vs-disconnect
distinction from 3b-i: a brain restart must send nothing, because the
keeper keeps holding the sockets through it, but a *keeper* shutdown
genuinely has no process left to hold them — every server on the other
end deserves a real `QUIT`, not a connection reset that reads as a ping
timeout. `Manager.QuitCloseAll(reason string, perNetworkTimeout,
overallTimeout time.Duration)` (`internal/keeper/manager.go`) does this:
iterates `Manager.All()`, fires `Keeper.QuitClose` on each concurrently,
bounded per-network and, defensively, overall. Default reason is
`version.QuitMessage()` — the same default `internal/uplink/shutdown.go`'s
`GracefulQuit` already uses, so operators see the same wire behavior
regardless of which process now owns the socket. `cmd/keeper`'s SIGTERM
handler calls `QuitCloseAll`, waits for it, then cancels `Listener.Serve`'s
context.

*Revert-and-confirm note*: `QuitCloseAll`'s "every network gets QUIT"
property and its lock-equivalent concurrency shape both reverted cleanly.
Its `overallTimeout` bound did not — reported honestly in
`TestQuitCloseAllBoundedByOverallTimeout`'s own doc comment: a single short
`QUIT` line fits in the OS send buffer virtually instantly regardless of
whether the peer reads it, and `Keeper.Close()` closing the underlying
`net.Conn` unblocks its read loop promptly on its own, so there was no
cheap, reliable way found to make a real `QuitClose` call stay stuck long
enough to distinguish "the outer bound fired" from "the operation just
finished fast anyway." The bound is kept because it's correct by
construction and free to have, not because that test proves it's
load-bearing — the same category of honestly-reported gap as the fan-in
ordering fix in the previous section.

Binary name: built as `gobnc-keeper` (a bare `keeper` binary risks PATH
collisions) from directory `cmd/keeper`.

### `internal/keeperboot` — find-or-start-and-attach

One entry point: `EnsureRunning(ctx, opts) (Result, error)`. Sequence:

1. Try `keeper.Attach`. Already running → done, no lock ever touched.
2. Not attachable → acquire an exclusive, cross-process lock
   (`flock(2)` on a dedicated lock file, polled with `LOCK_NB` so the wait
   is bounded by both `ctx` and a timeout rather than risking an
   indefinite block).
3. Re-check attach (double-checked locking — a racing caller may have
   just finished spawning one while this one waited for the lock).
4. Still not attachable: if the pidfile names a genuinely live process
   but the socket still isn't accepting, refuse outright rather than
   spawn a second keeper — that inconsistent state means something is
   actually wrong, and spawning on top of it is how you get two keepers
   both trying to bind the same socket path.
5. Otherwise spawn `gobnc-keeper` detached (adapted from
   `internal/daemon.Reborn`'s re-exec pattern — `Setsid`, stdio to
   `/dev/null`, its own `-log-file` so its output isn't just discarded),
   poll for the socket to become attachable (bounded), attach, release
   the lock.

**The lock is load-bearing, not defensive extra.** `Listener.Serve`
unconditionally removes any pre-existing file at the socket path before
listening (`_ = os.Remove(sockPath)`, listener.go). Two `gobnc` processes
racing to spawn a keeper at the same moment would, without the lock, have
the second one delete the first keeper's live socket out from under it —
orphaning a keeper that's still running but now unreachable at that path.
`TestEnsureRunningLockPreventsDoubleSpawn` pins this directly: with the
lock removed, two concurrent `EnsureRunning` calls against the same
not-yet-running keeper spawn two keepers roughly 2 times out of 3 in this
environment — a genuine, reproduced race, not a hypothetical one.

Socket/pidfile/lock paths default to `config.DefaultStateDir()` (exported
this round — was `defaultStateDir()`, gobnc's existing
`pid_file`/`control_socket`/`log_file` convention) rather than
`internal/keeper.DefaultRuntimeDir()` alone, which has no `~/.gobnc`
fallback and isn't gobnc-path-aware.

Spawning itself sits behind an unexported, package-private seam
(`Options.spawn`, same pattern as `DialConfig.Dial`) so the full
attach/lock/refuse/spawn decision tree is unit-tested without ever
actually forking a process — only `cmd/brain-register-demo`'s live runs
exercise the real `exec.Command` path.

### `cmd/brain-register-demo` — the live, cross-process proof

Fixes a stale forward-reference: `internal/brain/driver.go`'s package doc
already pointed at "cmd/brain-register-demo" as a runnable end-to-end
proof; it didn't exist until this round. `internal/keeperboot.EnsureRunning`
→ `keeper.AttachClient` → `brain.NewDriver`, dial one real network, drive a
real registration, log every state transition. On SIGINT/SIGTERM: exit,
nothing more — no `Driver.QuitNetwork` call anywhere in its shutdown path.
If it finds the target network already `Connected` in `client.Networks` on
attach, it skips dialing entirely and just resumes watching — the
survives-a-restart case, not an error case.

**Proven live**, against real `ergo` and the real remote network, exactly
per the checklist this stage was scoped around:

1. Fresh run: `keeperboot` spawns a new `gobnc-keeper` (no prior one),
   dials, registers for real.
2. SIGINT the demo. The keeper process (confirmed by PID) keeps running,
   untouched — the demo's exit sent nothing.
3. Run the demo again: `"attached to an existing keeper"`, `client.Networks`
   already reports the network `Connected` at the *same* epoch as the
   first run (no reconnect happened in between) with `LastSeq` having
   advanced — the same uplink, the same TCP connection, survived across
   two completely independent process lifetimes of the brain-equivalent
   demo. This is the concrete instance of the property this entire
   project exists to produce.
4. SIGTERM the keeper itself. Confirmed on the real ircd's own server log
   (`ergo`'s `quit` event, timestamp-matched to the signal) that this was
   a genuine `QUIT`, not a connection reset — the one shutdown path where
   disconnecting is correct and expected.

### What's still not done

`cmd/gobnc serve`'s real startup is **not** wired to any of this —
nothing in the actual bouncer consumes `internal/keeperboot` yet, because
`internal/uplink` still owns its own sockets directly. That wiring, plus
the actual `Run()`/`session()`/`register()` replacement, is 3b-ii proper
(the next stage) — deliberately not pulled forward into this one.

## Part 3b-ii, stage 2a: the missing post-registration pieces

Before touching `session.go` at all, mapped exactly how it depends on
`uplink.Uplink` — every method, every `Handler` callback, and what's
synchronous-order-dependent — to settle a question the deferred-work list
had left open: keep `Uplink`'s external shape and swap its internals, or
restructure `session.Session` to consume `Driver` directly.

**Finding that settled it:** almost all of `session`'s real state
(channels, users, umodes after the initial seed, IRCd family detection) is
already independently derived by `session` watching raw traffic via
`OnMessage` — not read fresh from `Uplink` each time. `Uplink` is a thin
traffic source plus a handful of one-time registration-seed getters.
Preserving `Uplink`'s exact synchronous `Handler`-callback shape on top of
`Driver`'s inherently async, channel-delivered event model would mean
fighting a sync/async mismatch for no real benefit, since there isn't much
"internals" worth hiding behind a facade. **Decision: restructure
`session.Session` to consume `Driver`'s channels directly** — that's stage
2b, not this one.

This stage builds what stage 2b will need first, as new code in
`internal/brain` — `internal/uplink`, `internal/session`, `internal/server`
all still completely untouched — because three real gaps turned up:

### `Driver.Lines()` — the post-registration relay that didn't exist

`Driver.handleLine` ran every line through `registration.Step` regardless
of phase, but `Step` is a no-op once `Phase` is terminal — so once a
network finished registering, its lines simply vanished inside `Driver`
instead of reaching anything. `session`'s eventual `OnMessage` equivalent
needs every line, forever, not just the registration-phase ones.
`Driver.Lines() <-chan keeper.LineMsg` republishes every line for every
tracked network unconditionally, mirroring the existing `WriteResults()`/
`NetworkEvents()` non-blocking-publish pattern exactly.
`TestDriverLinesRepublishesPostRegistrationTraffic` proves it with a real
line sent after a real 376.

### Auto-join

`internal/uplink.finishRegister` called `u.joinChannels()` synchronously
right after registering; `registration.Step` deliberately does nothing
past `PhaseComplete`, correctly, but nothing had replaced that follow-up
action. `Driver.SetChannels(id, []ChannelJoin)` configures the list;
`joinChannels` fires the moment `handleLine` observes `ActionRegistered`
for that network, not from a downstream consumer of `Results()` (coupling
it to whether anyone happens to be listening would make "did the join
actually happen" nondeterministic). Proven with a fake server that
captures whatever the client sends after registering rather than scripting
a fixed exchange — a bug that made `Driver` merely *believe* it joined
would show up as silence, not a false pass — and live, against real `ergo`
and the real remote network, both showing a genuine `JOIN` line back from
the server.

### ISON-based nick recovery

Ported from `internal/uplink/nick.go`'s `nickRecoveryLoop`/
`handleRecoveryISON`/`onSelfNickChange` — same 30s-default ticker shape
(`DefaultNickRecoveryInterval`, overridable via `WithNickRecoveryInterval`
for tests), same primary-then-alt preference logic, verbatim
`isonTargets`. Lives on `Driver` itself (`nickRecMu`/`nickRecStops`/
`isonPending`/`currentNick`, new file `internal/brain/nickrecovery.go`)
rather than a separate component — this is `Driver`'s own policy, the same
sense `registration.Step`-driving and auto-join are. Reacts to the same
parsed traffic `handleLine` already produces (303/NICK), not a second
independent parse. **Deliberately not ported**:
`internal/uplink.handleRecoveryNickError`, which only ever decided whether
a 432/433 caused by our own reclaim attempt should be hidden from downlink
fan-out — `Driver` has no fan-out concept yet (stage 2b), so there's
nothing here for it to suppress; it doesn't affect recovery's own
correctness.

Proven live end-to-end, not just unit-level: `TestDriverNickRecoveryReclaimsFreedNick`
forces a real nick-collision ladder step (primary rejected once, real
`433`, registration completes on the alt), then drives a real ISON
exchange — reads as still-taken on the first tick (must not reclaim),
reads as free on the second (must reclaim, a real `NICK` write reaching
the wire), and confirms recovery actually stops afterward. That last
property turned out to be triple-redundant by design (three independent
checks all converge — `handleSelfNickChange`'s own stop-on-primary,
`nickRecoveryTick`'s own defensive re-check, and `isonTargets` itself
returning empty for a nick already on primary) — matching
`internal/uplink`'s original layering, confirmed by disabling all three at
once via revert-and-confirm (any one alone left the other two standing).

### `Driver.Reconnect` — and two real races it surfaced

`Reconnect(id)` closes and redials using whatever `DialConfig` was last
passed to the new `Dial(id, cfg, fromSeq)` wrapper (which records it for
exactly this purpose), resets `registration.State` fresh from the
network's stored `NetworkConfig`, and stops any nick-recovery loop from
the connection being replaced — `internal/uplink.Uplink.ForceReconnect`'s
equivalent. Building and live-testing it surfaced two genuine races,
neither hypothetical:

1. **Stale disconnect from the superseded epoch.** A disconnect
   notification for the connection being replaced can still be in flight
   (published by the keeper, not yet delivered) at the moment `Reconnect`
   runs. Left alone, that stale event can arrive after the fresh `State`
   is installed and — since it isn't yet in a terminal `Phase` — get
   misread as a failure of the *new* attempt. Fixed by having `Reconnect`
   bump `Driver`'s per-network epoch watermark optimistically, before
   `SendClose`/`SendDial` even run; `handleNetworkEvent` drops any
   disconnect whose epoch is behind that watermark. Reproduced 7/8 times
   with the bump disabled via revert-and-confirm — a real, frequent race,
   not an edge case.
2. **Close and Dial racing each other.** Found live, against a real ircd,
   not the fake servers this session's other tests use: `SendClose` and
   `SendDial` are each dispatched to their own goroutine by the keeper's
   listener with no ordering guarantee between them, so firing both
   back-to-back could have the Dial's `k.Dial()` run before the Close's
   `k.Close()` had actually finished, failing with `ErrAlreadyConnected`.
   Fixed entirely within `internal/brain`, no keeper change needed:
   `Reconnect` now registers a one-shot waiter before `SendClose` and
   blocks (bounded, 5s) for the real `CloseResult` before sending the
   `Dial` — a second, independent path alongside the normal
   `CloseResults()` republish, so an external caller reading that channel
   is unaffected. This is the caller-side fix for an inherently async
   protocol, not a defect in the keeper's dispatch — nothing about
   handling unrelated requests concurrently is wrong in general, only
   this specific "the next request depends on the previous one having
   finished" case, which is `Reconnect`'s own requirement to satisfy.
   Reverting the wait reproduced the exact same `ErrAlreadyConnected`
   live, against real `ergo`, immediately — the fake-server unit test
   never caught this one at all (too fast/local to expose the goroutine
   scheduling race), which is itself worth remembering: this session's
   fake-server tests are necessary but not sufficient, and the live
   `ircd`-tagged proof against a real server is not a formality.

### A related hazard found and documented, not fixed

Running `cmd/brain-register-demo` live against its own resumed session
(the process restarts, correctly detects the network is already connected
and skips redialing) turned up a related but distinct hazard: the demo
originally called `RegisterNetwork` unconditionally before checking
"already connected." Doing so installs a fresh `registration.State`, and
the keeper delivers a resumed network's full retained backlog through the
exact same live `Line` events genuinely new traffic arrives on — there is
no wire-level distinction between the two. The fresh `State` stepped
through the replayed backlog (CAP negotiation, welcome numerics, MOTD) all
over again, reaching `PhaseComplete` a second time and re-firing
`ActionRegistered`'s side effects against a connection that was never
actually re-registered. Fixed in the demo (only call `RegisterNetwork` on
the genuinely-fresh path) and documented as an explicit warning on
`RegisterNetwork` itself for any future caller. Not fixed at the `Driver`
level, deliberately: this is the same class of hazard
`registration.Start`'s replay guard exists for, and the real fix is resume
support built on the blob store — not available yet, and not this stage's
job to build early.

### Proven live

`cmd/brain-register-demo` extended: `-channels` configures auto-join,
every line from `Driver.Lines()` is logged (visible proof the relay
works), and `SIGUSR1` forces a `Driver.Reconnect` on demand — the same
role `cmd/keeper-soak`'s own `SIGUSR1` handler plays for the lower-level
`Keeper` API. All four pieces confirmed against both real `ergo` and the
real remote network: a real nick-collision ladder fallback followed by a
real ISON reclaim, a real `JOIN` reaching the wire the moment registration
completes, every post-registration line visibly relayed, and a real
`SIGUSR1`-triggered reconnect producing a new epoch and a fresh
registration on the same tracked network — all without touching
`internal/uplink`, `internal/session`, or `internal/server`.

## Deferred work (updated 2026-08-15 — check current status before relying on this list)

**This section went stale for six commits' worth of real work** (everything
from `8a3671b` through `9e7b83d`) despite this file's own opening promise to
be kept current against the code. Recorded here as a fact about how this
document was actually maintained, not just a correction of its contents:
**Part 3b-ii — the read-loop cutover — is done.** `internal/uplink` is
deleted (`8a3671b`). `cmd/gobnc serve`'s real startup goes through
`internal/keeperboot` (`internal/server/server.go`). `internal/session` and
`internal/server` were rewritten against `Driver`/`AttachClient` as
planned. Brain-restart-while-keeper-holds-the-uplink resume is live and has
had two real bugs found and fixed against it since cutover: duplicate chat
history on replay (`d8dc83d`, fixed with an idempotent
`(network_id, target, keeper_seq)` storage key) and re-driven registration
re-sending CAP/NICK/USER into an already-registered connection (`9e7b83d`,
fixed with `Driver.RegisterResumedNetwork`). The
`Keeper.QuitClose`/`Driver.QuitNetwork`/`Manager.QuitCloseAll` shutdown-vs.-
disconnect distinction this section previously flagged as "not load-bearing
anywhere" is now load-bearing: `cmd/keeper`'s SIGTERM handler calls
`QuitCloseAll` for real.

Also done, contrary to what this list previously said:
- **User-mode tracking** and **post-registration CAP handling**
  (`CAP NEW`/dynamic `ACK`/`DEL`) — as this section speculated, both fell
  out of `Driver.Lines()` wiring into `internal/session`'s own traffic
  watching (`tracker.go`'s `221` handling, `upstream.go`'s
  `handleCAPLine`/`broadcastCapNotify`), no dedicated `Driver`-side port
  needed.
- **The paced flood-control writer** — built (`internal/brain/flood.go`,
  `floodDrainLoop`), not just the backpressure signal it depends on.

Also done, as of today (2026-08-15), completing the correction above:
- **Blob store, gap-only resume, and ring-overflow case 2** — all built
  together, since they turned out to be one connected piece of work, not
  three separate ones (see "Blob store and gap-only resume" above for the
  full account, including the checkpoint design and why full replay was
  never actually the intended end state, only what the code did before
  the blob store existed to replace it).
- **Log destination reload** (`cmd/keeper`) — built. `cmd/keeper/main.go`
  now uses `internal/log`'s `Sink`/`Reload` pattern (the same one
  `cmd/gobnc` already used), with a `-debug`/`-d` flag and a `SIGHUP`
  handler that reopens `-log-file` at the same path (the logrotate case —
  every other flag here is only ever read once at startup, so there's
  nothing else for a reload to pick up). No changes needed to `Keeper`'s
  own logger field — `Reload` re-points the same `*slog.Logger`'s
  underlying handler in place. Verified live: start, `SIGHUP`, confirm the
  same file keeps receiving new lines; `mv` the file out from under it,
  `SIGHUP` again, confirm a fresh file appears at the same path with new
  output landing there instead of the moved one.
- **Debug logging of the keeper<->brain control frames** — built. Every
  `Hello`/`HelloAck`/`Dial`/`Close`/`Write`/`QuitClose`/`BlobPush`/`SeqAck`
  request and result, plus unsolicited `NetworkEvent`, gets a
  `slog.Debug` line on both `Listener` (`cmd/keeper -debug`) and
  `AttachClient` (`keeper.WithAttachLogger`, threaded through
  `internal/keeperboot.Options.Logger` from `internal/server`'s own
  logger) — network ID, message kind, small scalars only, never
  `WriteRequestMsg.Line`/`BlobPushMsg.Value`/`LineMsg.Raw`, preserving the
  IPC security section's "no line contents at normal levels" invariant.
  Verified live against a real spawned keeper process: `Hello`/`HelloAck`
  now appear on both sides at `-debug`, which is the specific gap that
  prompted this (previously only raw uplink traffic, wired in `634f4f4`,
  showed up in `-debug` output — the control frames underneath it never
  did).

Still genuinely open:

- **BSD/macOS peer-credential code** — still cross-compiles cleanly for
  darwin/freebsd, still never run on a real BSD/macOS host or in CI.
  Unchanged status: "compiles, never run." Needs a real BSD/macOS host —
  nothing more to build without one.

**One new gap, not previously flagged, found while auditing current status
against this list:** `internal/brain/nickrecovery.go`'s doc comment still
says `internal/uplink.handleRecoveryNickError` (which hid a 432/433 caused
by the ISON-driven auto-reclaim's own `NICK` attempt from downlink fan-out)
was "deliberately not ported" because "`Driver` has no fan-out concept yet
(stage 2b)". Stage 2b fan-out now exists, and this was never revisited:
`internal/session/tracker.go`'s `RouteMessage` has no case for
`432`/`433`/`437`, so a nick error produced by the recovery loop's own
autonomous reclaim attempt (as opposed to a client-issued `/nick`) falls
through `HandleMessage`'s default path and broadcasts to every attached
client — a phantom "Nickname is already in use" notice with no user action
behind it, on every network configured for nick recovery where reclaim
races the server. Worth its own decision (suppress recovery-originated nick
errors from fan-out, the way `internal/uplink` did, or something else) when
next touching this area — not fixed here.

## Reconnect test coverage (2026-08-15)

Before today, every resume-shaped test exercised either the raw keeper
protocol with a fake client, or the in-process `resumeTestKeeper` harness
(a real `Manager`+`Listener`, but never a real separate OS process) — the
real `internal/keeperboot` spawn path (`exec.Command`, `Setsid`) was only
ever exercised by the manual `cmd/brain-register-demo`, never by a test.
Closed: `internal/server/keeper_process_live_test.go`'s
`TestKeeperProcessSurvivesServerRestart` (`ircd`-tagged) builds a real
`gobnc-keeper` binary, spawns it for real via `keeperboot.EnsureRunning`,
registers against a real ircd (`docker/ircd`'s ergo), tears down and
rebuilds a fresh `Server` attaching to the same still-running keeper
subprocess, and asserts the epoch is unchanged (the real uplink survived,
not a redial) and the resumed nick is correct from the blob alone.
Revert-and-confirmed against the fix it exercises (`Session.SeedFromBlob`).

Also added: `TestGapOnlyResumeSkipsAckedLines` and
`TestRingOverflowNoSubscriberSelfCloses` (`internal/keeper`, wire-protocol
level) and `TestGapOnlyResumeSeedsStateFromBlobNotReplay`
(`internal/server`, in-process keeper) — all revert-and-confirmed.

**Not covered, and worth doing next, not claimed as done here:** redial
after a genuine socket death occurring mid-gap (as opposed to a deliberate
detach); a brain restart while one network is still mid-registration and
another on the same keeper is already complete; concurrent admin
`Dial`/`Close`/`Reconnect` racing a fresh attach at the `Session`/`Server`
level (`internal/keeper/client_concurrent_test.go` only stresses the raw
protocol, not the real stack above it); ring-overflow occurring during a
brain-down window immediately before a resume attach, combined with the
blob-carried-state-survives-eviction scenario the blob store's late-attach
case exists for in the first place.
