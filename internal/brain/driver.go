// Package brain is the wiring layer between internal/keeper's transport
// and internal/registration's pure protocol logic — it owns no protocol
// logic of its own (that's internal/registration) and no transport of its
// own (that's internal/keeper); it only connects the two. This is new
// code with no consumers yet: internal/uplink is untouched, and nothing in
// the existing bouncer depends on this package. See docs/keeper-design.md
// for the split this completes and cmd/brain-register-demo for a runnable
// end-to-end proof against real servers.
package brain

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// DefaultRegistrationTimeout bounds how long a network is given to reach a
// terminal registration phase (PhaseComplete or PhaseFailed) after
// StartRegistration, measured once from the opening lines, not reset on
// each line received. This is a brain decision, not a keeper one — see
// docs/keeper-design.md's Part 3a section — because the keeper's own
// ReadIdleTimeout is a socket-liveness backstop (default 10 minutes) with
// no notion of "registration" at all; a server that keeps a trickle of
// traffic flowing (or one that just completes the TCP handshake and says
// nothing) without ever reaching 001/376/422 would sit well within that
// backstop indefinitely.
//
// 90s, not internal/uplink's old 60s: the real transcript corpus
// (testdata/registration/) includes a genuine ~70s ident-lookup wait on
// two old ircds (bahamut, ircd-irc2) — 60s would have failed both. 90s
// keeps comfortable margin above the slowest real capture without
// approaching the keeper's 10-minute socket backstop, so a merely slow
// (but real) registration still succeeds while a genuinely stalled one
// fails in well under two minutes instead of not failing at all.
const DefaultRegistrationTimeout = 90 * time.Second

// DriverOption configures optional Driver behavior at construction time.
type DriverOption func(*Driver)

// WithRegistrationTimeout overrides DefaultRegistrationTimeout — mainly for
// tests, which need this in the millisecond range rather than waiting out
// 90 real seconds to prove the deadline fires.
func WithRegistrationTimeout(d time.Duration) DriverOption {
	return func(drv *Driver) { drv.registrationTimeout = d }
}

// WithLogger sets where Driver logs raw uplink traffic (see Driver.log's
// doc comment). Omitted entirely by tests that construct a Driver
// directly — a nil logger silently disables this logging, not a panic or
// a fallback to slog.Default().
func WithLogger(log *slog.Logger) DriverOption {
	return func(drv *Driver) { drv.log = log }
}

// NetworkConfig is what Driver needs to register one network. PrimaryNick/
// AltNick/NickRecovery/SASL go straight to registration.New; Pass/Username/
// Realname are only used to build the opening lines (see
// registration.Start) and aren't otherwise known to the state machine.
type NetworkConfig struct {
	PrimaryNick  string
	AltNick      string
	NickRecovery bool
	SASL         registration.SASLConfig

	Pass     string
	Username string
	Realname string

	// Name is a purely cosmetic display label for this network (e.g. the
	// store.Network.Name a caller already has) — used only for raw-traffic
	// log lines (see Driver.log's doc comment); Driver has no other notion
	// of network identity beyond the opaque keeper.NetworkID.
	Name string
}

// Result is published once a network's registration reaches a terminal
// state (registration.PhaseComplete or registration.PhaseFailed).
type Result struct {
	Network keeper.NetworkID
	State   registration.State
}

// ChannelJoin is one channel to auto-join once a network finishes
// registering — deliberately a small, brain-local type rather than
// importing internal/store.Channel, matching keeper.NetworkID's own
// doc-commented reasoning: this package shouldn't take on a dependency on
// a larger one just to reuse a field shape.
type ChannelJoin struct {
	Name string
	Key  string // empty: no key
}

// Driver pumps one keeper.AttachClient's live event stream: Line events
// for a network still registering are stepped through
// registration.Step, and any resulting ActionSend is turned into a real
// keeper.AttachClient.SendWrite call. Driver is the only reader of the
// client's event stream (AttachClient has no fan-out — only one goroutine
// can call Next in a loop), so it also republishes every other event kind
// (DialResult, CloseResult, NetworkEvent) on its own channels; a caller
// that issues a Dial or Close over the same client must read the
// corresponding result from Driver, not from the client directly.
type Driver struct {
	client *keeper.AttachClient

	// log, if set (see WithLogger), gets every raw uplink line Driver
	// sends or receives, via gobnclog.IRC — the brain-side replacement for
	// what internal/uplink.Uplink used to get for free by owning a
	// connio.Conn with SetLogger attached directly. The keeper deliberately
	// never logs raw content itself (see keeper.Dial's doc comment on
	// c.SetLogger) — Driver is the only remaining single choke point for
	// every uplink line in the new split (see this type's own doc comment),
	// matching connio.Conn's old role. nil is a valid, silent default
	// (gobnclog.IRC no-ops on a nil logger) — tests that construct a Driver
	// directly don't need one.
	log *slog.Logger

	registrationTimeout  time.Duration
	nickRecoveryInterval time.Duration
	keepaliveIdle        time.Duration
	keepaliveGrace       time.Duration
	minBackoff           time.Duration
	maxBackoff           time.Duration
	maxFloodQueue        int

	mu              sync.Mutex
	states          map[keeper.NetworkID]registration.State // presence = tracked
	configs         map[keeper.NetworkID]NetworkConfig
	channels        map[keeper.NetworkID][]ChannelJoin
	dialConfigs     map[keeper.NetworkID]keeper.DialConfig             // last config passed to Dial; see Reconnect
	epochs          map[keeper.NetworkID]uint64                        // current known epoch per network; see handleNetworkEvent
	deadlines       map[keeper.NetworkID]*time.Timer                   // armed while registering; see armDeadline
	currentNick     map[keeper.NetworkID]string                        // see nickrecovery.go
	closeWaiters    map[keeper.NetworkID]chan keeper.CloseResultMsg    // see Reconnect
	blobPushWaiters map[keeper.NetworkID]chan keeper.BlobPushResultMsg // see PushBlob

	// nickRecMu guards the maps below, separately from mu — matches
	// internal/uplink's own separate nickRecMu, since nick-recovery state
	// transitions (start/stop/isonPending/hold) are logically independent of
	// registration/config state and giving them their own lock avoids
	// nick-recovery bookkeeping contending with the hot registration path.
	nickRecMu    sync.Mutex
	nickRecStops map[keeper.NetworkID]chan struct{}
	isonPending  map[keeper.NetworkID]bool
	nickRecHeld  map[keeper.NetworkID]bool // client NICK; see StopNickRecovery

	// reconnMu guards auto-reconnect bookkeeping, separately from mu for the
	// same reason nickRecMu is separate — see reconnect.go.
	reconnMu        sync.Mutex
	backoff         map[keeper.NetworkID]time.Duration
	reconnectTimers map[keeper.NetworkID]*time.Timer
	stopped         map[keeper.NetworkID]bool

	// keepMu guards idle-PING keepalive state, separately from mu for the
	// same reason nickRecMu is — see keepalive.go.
	keepMu    sync.Mutex
	keepStops map[keeper.NetworkID]chan struct{}
	lastRX    map[keeper.NetworkID]int64

	// floodMu guards flood-pacing state, separately from mu for the same
	// reason nickRecMu is — see flood.go.
	floodMu sync.Mutex
	flood   map[keeper.NetworkID]*floodState

	results          chan Result
	lines            chan keeper.LineMsg
	dialResults      chan keeper.DialResultMsg
	closeResults     chan keeper.CloseResultMsg
	writeResults     chan keeper.WriteResultMsg
	quitCloseResults chan keeper.QuitCloseResultMsg
	netEvents        chan keeper.NetworkEventMsg
}

// NewDriver wraps an already-attached, live-mode client. Call SendLiveReady
// on it before Run, same as any other live-mode use of AttachClient —
// Driver doesn't do that for you, since the caller may want to finish
// setting up (e.g. calling RegisterNetwork for networks already listed in
// client.Networks) before delivery starts.
func NewDriver(client *keeper.AttachClient, opts ...DriverOption) *Driver {
	d := &Driver{
		client:               client,
		registrationTimeout:  DefaultRegistrationTimeout,
		nickRecoveryInterval: DefaultNickRecoveryInterval,
		keepaliveIdle:        KeepaliveIdle,
		keepaliveGrace:       KeepaliveGrace,
		minBackoff:           DefaultMinBackoff,
		maxBackoff:           DefaultMaxBackoff,
		states:               make(map[keeper.NetworkID]registration.State),
		configs:              make(map[keeper.NetworkID]NetworkConfig),
		channels:             make(map[keeper.NetworkID][]ChannelJoin),
		dialConfigs:          make(map[keeper.NetworkID]keeper.DialConfig),
		epochs:               make(map[keeper.NetworkID]uint64),
		deadlines:            make(map[keeper.NetworkID]*time.Timer),
		currentNick:          make(map[keeper.NetworkID]string),
		closeWaiters:         make(map[keeper.NetworkID]chan keeper.CloseResultMsg),
		blobPushWaiters:      make(map[keeper.NetworkID]chan keeper.BlobPushResultMsg),
		nickRecStops:         make(map[keeper.NetworkID]chan struct{}),
		isonPending:          make(map[keeper.NetworkID]bool),
		nickRecHeld:          make(map[keeper.NetworkID]bool),
		keepStops:            make(map[keeper.NetworkID]chan struct{}),
		lastRX:               make(map[keeper.NetworkID]int64),
		backoff:              make(map[keeper.NetworkID]time.Duration),
		reconnectTimers:      make(map[keeper.NetworkID]*time.Timer),
		stopped:              make(map[keeper.NetworkID]bool),
		flood:                make(map[keeper.NetworkID]*floodState),
		results:              make(chan Result, 16),
		lines:                make(chan keeper.LineMsg, 8192),
		dialResults:          make(chan keeper.DialResultMsg, 16),
		closeResults:         make(chan keeper.CloseResultMsg, 16),
		writeResults:         make(chan keeper.WriteResultMsg, 16),
		quitCloseResults:     make(chan keeper.QuitCloseResultMsg, 16),
		netEvents:            make(chan keeper.NetworkEventMsg, 16),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Results publishes one Result per network, once it reaches a terminal
// registration phase.
func (d *Driver) Results() <-chan Result { return d.results }

// Lines republishes every line Driver sees for a tracked network,
// unconditionally — before registration, during it, and forever after.
// registration.Step only cares about registration-phase traffic and is a
// no-op past a terminal Phase, so without this, everything a network says
// after it finishes registering would simply vanish inside Driver. This
// is the channel a future session-equivalent consumer reads for its own
// message fan-out, state tracking, and reply routing — Driver itself does
// not interpret post-registration content at all, only relays it (the
// same "keeper stores bytes, the layer above interprets them" principle
// applied one level up).
func (d *Driver) Lines() <-chan keeper.LineMsg { return d.lines }

// DialResults, CloseResults, WriteResults, QuitCloseResults, and
// NetworkEvents republish every frame of that kind Driver's read loop
// sees, since Driver owns the only read of the underlying client.
func (d *Driver) DialResults() <-chan keeper.DialResultMsg           { return d.dialResults }
func (d *Driver) CloseResults() <-chan keeper.CloseResultMsg         { return d.closeResults }
func (d *Driver) WriteResults() <-chan keeper.WriteResultMsg         { return d.writeResults }
func (d *Driver) QuitCloseResults() <-chan keeper.QuitCloseResultMsg { return d.quitCloseResults }
func (d *Driver) NetworkEvents() <-chan keeper.NetworkEventMsg       { return d.netEvents }

// QuitNetwork deliberately disconnects from network: it asks the keeper
// (via QuitCloseRequest) to write a final line — reason wrapped as
// "QUIT :reason", or bare "QUIT" if reason is empty — with a bounded
// deadline, then close the uplink. timeout<=0 uses the keeper's default.
//
// This is the ONLY Driver method that sends anything resembling QUIT.
// Driver.Run returning (ctx canceled, e.g. because the brain process
// itself is exiting for a code reload) never calls this and never will —
// it just stops reading, and the keeper keeps holding every uplink
// exactly as it was. Conflating "the brain is going away" with "disconnect
// this network" is the one mistake that costs everything on this project
// (see docs/keeper-design.md): every reload would send QUIT and drop
// every uplink, which is the exact failure the keeper/brain split exists
// to prevent. If a future caller ever needs "disconnect every network on
// brain exit," that has to be an explicit decision made by whoever drives
// shutdown, spelled out in real code — never a side effect of Run
// returning.
func (d *Driver) QuitNetwork(id keeper.NetworkID, reason string, timeout time.Duration) error {
	line := "QUIT"
	if reason != "" {
		line += " :" + reason
	}
	return d.client.SendQuitClose(id, line, timeout)
}

// SetChannels sets the channels Driver auto-joins for id the moment its
// registration next reaches PhaseComplete — mirrors
// internal/uplink.Uplink.SetChannels/joinChannels, which did the same
// thing synchronously inside the old finishRegister. registration.Step
// deliberately stops at PhaseComplete and takes no further action (join
// isn't a registration concern), so without this there is nothing left to
// perform the join at all. Safe to call before or after RegisterNetwork,
// and at any point before the network finishes registering again (e.g.
// before a Reconnect) — it only takes effect on the next ActionRegistered
// this Driver observes for id.
func (d *Driver) SetChannels(id keeper.NetworkID, channels []ChannelJoin) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.channels[id] = append([]ChannelJoin(nil), channels...)
}

// joinChannels sends JOIN for every channel configured via SetChannels for
// id — called once, right when Driver itself observes ActionRegistered
// for id (see handleLine), not by a downstream consumer of Results(),
// since coupling this to something reading a channel would make "does the
// join actually happen" depend on whether anyone happens to be listening.
func (d *Driver) joinChannels(id keeper.NetworkID) {
	d.mu.Lock()
	channels := d.channels[id]
	d.mu.Unlock()
	for _, ch := range channels {
		line := "JOIN " + ch.Name
		if ch.Key != "" {
			line += " " + ch.Key
		}
		_ = d.sendLine(id, line) // best-effort, matching ActionSend's own handling
	}
}

// RegisterNetwork (re)starts registration tracking for id with a fresh
// registration.State. Call it before the network's first Line arrives —
// typically right after issuing (or observing) a Dial for it. Lines for a
// network with no tracked state are ignored, not buffered or errored: a
// network the caller isn't trying to register through this driver (e.g.
// one already fully attached from a prior brain instance) is simply not
// this driver's concern.
//
// Do not call this for a network you're merely re-attaching to (already
// connected and already registered on the keeper, e.g. this Driver
// instance is a fresh process resuming after a restart) unless you
// actually want to redrive registration. There is no wire-level
// distinction between "genuinely new traffic" and "this network's
// retained backlog being replayed because you just attached" — both
// arrive as ordinary Line events. A fresh registration.State fed that
// backlog steps through it (CAP negotiation, welcome numerics, MOTD) all
// over again, reaching PhaseComplete a second time and re-firing
// ActionRegistered's side effects (auto-join, nick-recovery start)
// against a connection that was never actually re-registered — found by
// running cmd/brain-register-demo live against its own resumed session.
// Same class of hazard registration.Start's replay guard exists for (see
// docs/keeper-design.md); a real fix is resume support built on the blob
// store, not available yet — until then, simply don't call RegisterNetwork
// for a network you're only resuming.
func (d *Driver) RegisterNetwork(id keeper.NetworkID, cfg NetworkConfig) {
	d.mu.Lock()
	d.configs[id] = cfg
	d.resetStateLocked(id, cfg)
	d.mu.Unlock()
}

// RegisterResumedNetwork is RegisterNetwork's counterpart for a network the
// keeper already holds live from before this brain process attached (a
// resumed network — see Server.resumedAtBoot's doc comment on the caller
// side) — the exact case RegisterNetwork's own doc comment warns not to use
// it for.
//
// It still makes id tracked (so Driver.Lines() forwards its replayed
// backlog downstream — Session rebuilds its own state entirely by watching
// that replay directly, independent of anything in this State; see
// Session.HandleLine's doc comment), but installs the State already at
// registration.PhaseComplete instead of a fresh one. Step is a documented
// no-op once Phase is terminal (see Step's own doc comment), so every line
// in the replayed backlog passes through as a pure no-op: no ActionSend
// (which would otherwise re-send CAP REQ/CAP END/etc. out to the still-live
// uplink for real, and a real ircd answers for real — a second live CAP ACK
// for traffic that already completed) and no re-fired ActionRegistered
// (which would otherwise re-run auto-join and restart nick recovery against
// a connection that was never actually re-registered).
//
// Known gap: nick-recovery state isn't actually resumed — there is no
// snapshot to resume it from (that needs the blob store
// docs/keeper-design.md defers). If id was off its primary nick when this
// brain attached, recovery does not resume until a live NICK event
// corrects Driver's assumption; this method only stops the resend/
// re-registration hazard, it does not restore full pre-restart state.
func (d *Driver) RegisterResumedNetwork(id keeper.NetworkID, cfg NetworkConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.configs[id] = cfg
	state := registration.New(cfg.PrimaryNick, cfg.AltNick, cfg.NickRecovery, cfg.SASL)
	state.Phase = registration.PhaseComplete
	state.GotWelcome = true
	d.states[id] = state
	delete(d.currentNick, id)
	if t, ok := d.deadlines[id]; ok {
		t.Stop()
		delete(d.deadlines, id)
	}
	// A recovery loop from whatever this brain instance's own state
	// (there shouldn't be one yet — id has never been tracked by this
	// Driver before) might otherwise leak; matches resetStateLocked's own
	// belt-and-suspenders call. Safe to call while d.mu is held — see
	// resetStateLocked's own comment on this.
	d.stopNickRecovery(id)
	// The keeper already held this socket; this brain will never see an
	// EventConnected for it. Start the idle-PING loop here, the same
	// place a live EventConnected would for a fresh dial.
	d.noteRX(id)
	d.startKeepaliveIfNeeded(id)
}

// peerLabel returns id's display name (see NetworkConfig.Name) for a log
// line, falling back to the bare numeric ID for a network logged before
// RegisterNetwork ever ran for it (shouldn't happen in practice — see
// handleLine's own tracked guard — but a label is better than a panic).
func (d *Driver) peerLabel(id keeper.NetworkID) string {
	d.mu.Lock()
	name := d.configs[id].Name
	d.mu.Unlock()
	if name == "" {
		return fmt.Sprintf("net-%d", id)
	}
	return name
}

// sendLine is the one path every outgoing uplink line must go through —
// registration's opening burst and ongoing protocol responses, nick
// recovery's ISON/NICK, WriteRaw's paced client/session traffic, all of
// it — so raw-traffic logging (see Driver.log's doc comment) only needs
// instrumenting once, matching connio.Conn.WriteLine's old role as the
// single write choke point for internal/uplink.
func (d *Driver) sendLine(id keeper.NetworkID, line string) error {
	gobnclog.IRC(d.log, d.peerLabel(id), ">>", line)
	return d.client.SendWrite(id, line)
}

// UpdateNetworkConfig replaces id's stored NetworkConfig for future
// registration attempts (the next Reconnect or auto-redial) without
// resetting anything about the current live connection or its
// registration.State — unlike RegisterNetwork, which is for starting a
// genuinely fresh attempt. Mirrors internal/uplink.Uplink.SetNetwork's
// config-only side (nick/pass/SASL settings for next time); the flood and
// channel equivalents are SetFloodParams and SetChannels. Also applies
// NickRecovery's on/off change immediately to any currently running
// recovery loop, the one part of SetNetwork that did take effect on the
// live connection without waiting for a reconnect.
func (d *Driver) UpdateNetworkConfig(id keeper.NetworkID, cfg NetworkConfig) {
	d.mu.Lock()
	d.configs[id] = cfg
	d.mu.Unlock()
	if cfg.NickRecovery {
		d.startNickRecoveryIfNeeded(id)
	} else {
		d.stopNickRecovery(id)
	}
}

// resetStateLocked installs a fresh registration.State for id from cfg and
// discards any deadline left over from a prior attempt — shared by
// RegisterNetwork and Reconnect (a redial needs the same fresh-State
// treatment a first-time registration does; the difference is only where
// cfg comes from). Caller must hold d.mu.
func (d *Driver) resetStateLocked(id keeper.NetworkID, cfg NetworkConfig) {
	d.states[id] = registration.New(cfg.PrimaryNick, cfg.AltNick, cfg.NickRecovery, cfg.SASL)
	delete(d.currentNick, id)
	if t, ok := d.deadlines[id]; ok {
		t.Stop()
		delete(d.deadlines, id)
	}
	// A recovery/keepalive loop from whatever connection this State is
	// superseding is no longer valid — stopNickRecovery/stopKeepalive
	// only touch their own mutexes, safe to call while d.mu (the
	// caller's lock) is held.
	d.stopNickRecovery(id)
	d.clearNickRecoveryHold(id)
	d.stopKeepalive(id)
}

// Dial asks the keeper to dial (or redial) network id with cfg, recording
// cfg so a later Reconnect can redial with the same settings — this is
// the only way Driver learns what DialConfig a network is using. A caller
// that dials by calling AttachClient.SendDial directly instead of through
// here bypasses this bookkeeping, and Reconnect will fail for that
// network until Dial is called through Driver at least once.
func (d *Driver) Dial(id keeper.NetworkID, cfg keeper.DialConfig, fromSeq uint64) error {
	d.mu.Lock()
	d.dialConfigs[id] = cfg
	d.mu.Unlock()
	d.clearStopped(id)
	return d.client.SendDial(id, cfg, fromSeq)
}

// UpdateDialConfig records cfg as id's DialConfig for the next
// Reconnect/auto-redial, without dialing anything itself — for a caller
// that has new connection settings (e.g. a rehashed TLS cert path) it
// wants a network's *next* reconnect to pick up, but isn't ready to
// redial right now. Dial itself already records cfg as a side effect of
// actually dialing; this is the same bookkeeping in isolation.
func (d *Driver) UpdateDialConfig(id keeper.NetworkID, cfg keeper.DialConfig) {
	d.mu.Lock()
	d.dialConfigs[id] = cfg
	d.mu.Unlock()
}

// Reconnect closes and redials network id using whatever DialConfig was
// last passed to Dial for it, and resets its registration.State to fresh
// (using the NetworkConfig from the last RegisterNetwork call) — the
// equivalent of internal/uplink.Uplink.ForceReconnect, which closed the
// current connection so Run's loop redialed and internal/uplink's
// session() unconditionally re-registered from scratch on the new
// connection. Channels (SetChannels) and nick-recovery configuration for
// id are untouched — this is a redial of the same tracked network, not a
// fresh RegisterNetwork call from the caller's side.
//
// Like a fresh Dial, this does not itself call StartRegistration — the
// caller still waits for the resulting DialResult/NetworkEvent{Connected}
// and calls StartRegistration, same as any other dial. Returns an error
// if Dial was never called through this Driver for id (nothing recorded
// to redial with) or if RegisterNetwork never was (nothing to reset the
// State from).
//
// Bumps d.epochs[id] optimistically, before SendClose/SendDial even run —
// this closes a real race found while testing this method: a disconnect
// notification for the connection being replaced can still be in flight
// (published by the keeper, not yet delivered here) at the moment
// Reconnect runs. Without the bump, that stale, pre-reconnect disconnect
// event can arrive after resetStateLocked has already installed the
// fresh State for the new attempt, and — since it isn't yet in a terminal
// Phase — get misread by handleNetworkEvent as a failure of the *new*
// attempt instead of being recognized as leftover noise from the old one.
// Safe even if the coming Dial fails outright: keeper only increments its
// own epoch on a successful raw connect (see Keeper.Dial), and a
// deliberate Close (which SendClose triggers) never publishes a
// disconnect event for the connection it's closing — so there is no
// legitimate future event this bump could wrongly suppress.
//
// Waits for the actual CloseResult before sending the Dial request — a
// second real race found live (against a real ircd, not just the fake
// servers this session's other tests use): SendClose and SendDial are
// each dispatched to their own goroutine by the keeper's listener with no
// ordering guarantee between them, so firing both back-to-back can have
// the Dial's k.Dial() run before the Close's k.Close() has actually
// finished, failing with ErrAlreadyConnected. Waiting for confirmation
// first is the caller-side fix for an inherently async protocol — no
// change to the keeper's request dispatch needed, since nothing about
// that dispatch is wrong in general, only this specific "the next request
// depends on the previous one having completed" case.
func (d *Driver) Reconnect(id keeper.NetworkID) error {
	d.mu.Lock()
	dialCfg, ok := d.dialConfigs[id]
	if !ok {
		d.mu.Unlock()
		return fmt.Errorf("brain: Reconnect: network %d has no recorded DialConfig (never Dial'd through Driver)", id)
	}
	netCfg, ok := d.configs[id]
	if !ok {
		d.mu.Unlock()
		return fmt.Errorf("brain: Reconnect: network %d was never registered (RegisterNetwork was never called)", id)
	}
	d.resetStateLocked(id, netCfg)
	d.epochs[id]++
	waiter := make(chan keeper.CloseResultMsg, 1)
	d.closeWaiters[id] = waiter
	d.mu.Unlock()
	d.clearStopped(id)

	if err := d.client.SendClose(id); err != nil {
		d.mu.Lock()
		delete(d.closeWaiters, id)
		d.mu.Unlock()
		return err
	}

	select {
	case <-waiter:
		// Close confirmed complete — whether it reported OK or an error,
		// the keeper is done processing it, and a Dial is now safe to
		// send. Keeper.Close is idempotent and safe even if there was
		// nothing to close, so a "failure" here isn't a reason to abort.
	case <-time.After(closeConfirmTimeout):
		d.mu.Lock()
		delete(d.closeWaiters, id)
		d.mu.Unlock()
		return fmt.Errorf("brain: Reconnect: network %d: no CloseResult within %s", id, closeConfirmTimeout)
	}

	return d.client.SendDial(id, dialCfg, 0)
}

// closeConfirmTimeout bounds Reconnect's wait for CloseResult. Close is a
// fast, local operation on the keeper's side (cancel + socket close +
// wait for the read loop to notice) — 5s is generous headroom, not an
// expected wait.
const closeConfirmTimeout = 5 * time.Second

// notifyCloseWaiter delivers a CloseResult to a pending Reconnect call
// waiting on it, if there is one — separate from and in addition to the
// normal CloseResults() republish (trySendCloseResult), so an external
// caller reading CloseResults() is unaffected by Reconnect's own internal
// wait; the two are independent consumers of the same event.
func (d *Driver) notifyCloseWaiter(res keeper.CloseResultMsg) {
	d.mu.Lock()
	waiter, ok := d.closeWaiters[res.Network]
	if ok {
		delete(d.closeWaiters, res.Network)
	}
	d.mu.Unlock()
	if ok {
		waiter <- res
	}
}

// blobPushConfirmTimeout bounds PushBlob's wait for BlobPushResultMsg —
// same reasoning and value as closeConfirmTimeout: a fast, local operation
// on the keeper's side, generous headroom, not an expected wait.
const blobPushConfirmTimeout = 5 * time.Second

// PushBlob applies one derived entry to network's blob store on the
// keeper, and blocks until the keeper confirms it landed (or the wait
// times out). This blocking is load-bearing, not just synchronous style:
// it is what lets a caller (Session, mid-line-processing) know a push has
// genuinely completed before it lets its own processing of that line
// return — which is in turn what lets demux's AckSeq call after
// Session.HandleLine returns be correct by construction, satisfying
// docs/keeper-design.md's blob store ordering requirement (push the
// derived entry before advancing the consumed seq) without any locking
// between the two: one goroutine, push-then-return, ack-after-return, in
// that order, every time.
func (d *Driver) PushBlob(id keeper.NetworkID, key string, mode keeper.BlobMode, value []byte) error {
	waiter := make(chan keeper.BlobPushResultMsg, 1)
	d.mu.Lock()
	d.blobPushWaiters[id] = waiter
	d.mu.Unlock()

	if err := d.client.SendBlobPush(id, key, mode, value); err != nil {
		d.mu.Lock()
		delete(d.blobPushWaiters, id)
		d.mu.Unlock()
		return err
	}

	select {
	case res := <-waiter:
		if !res.OK {
			return fmt.Errorf("brain: PushBlob: network %d: %s", id, res.Error)
		}
		return nil
	case <-time.After(blobPushConfirmTimeout):
		d.mu.Lock()
		delete(d.blobPushWaiters, id)
		d.mu.Unlock()
		return fmt.Errorf("brain: PushBlob: network %d: no BlobPushResult within %s", id, blobPushConfirmTimeout)
	}
}

// notifyBlobPushWaiter delivers a BlobPushResult to a pending PushBlob call
// waiting on it, if there is one — mirrors notifyCloseWaiter exactly.
func (d *Driver) notifyBlobPushWaiter(res keeper.BlobPushResultMsg) {
	d.mu.Lock()
	waiter, ok := d.blobPushWaiters[res.Network]
	if ok {
		delete(d.blobPushWaiters, res.Network)
	}
	d.mu.Unlock()
	if ok {
		waiter <- res
	}
}

// AckSeq tells the keeper this brain has fully finished processing the
// line at seq for network — see SeqAckMsg's doc comment. Fire-and-forget,
// matching the wire message itself: there is nothing to wait for. Callers
// must only call this after every blob push that line's processing
// triggered has already completed (PushBlob already blocks for exactly
// that reason) — calling it any earlier reopens the gap the ordering
// requirement exists to close.
func (d *Driver) AckSeq(id keeper.NetworkID, seq uint64) error {
	return d.client.SendSeqAck(id, seq)
}

// StartRegistration sends the opening CAP LS/NICK/USER lines for a tracked
// network (see registration.Start) — call it once the caller has confirmed
// the network's uplink is actually connected (a DialResult with OK, or a
// NetworkEvent{Connected}), since there is nothing to write to before then.
// Calling it for a network RegisterNetwork was never called for is an
// error; calling it twice for the same connection sends the opening lines
// twice, which is the caller's mistake to avoid, not Driver's to prevent —
// Driver has no notion of "already started" separate from Phase, and a
// fresh reconnect legitimately needs the opening lines resent.
//
// Driver has no replay/resume path yet (that's blob-store-driven work, not
// built) — every call here is necessarily a fresh connection, so replay is
// hardcoded false. When resume support is added, it MUST go through a
// different entry point that never calls StartRegistration at all: Start
// is a live-only operation (see its doc comment and docs/keeper-design.md)
// precisely because resending CAP LS/NICK/USER down an already-registered
// live socket is a real hazard, not a hypothetical one to guard against
// only in the abstract.
func (d *Driver) StartRegistration(id keeper.NetworkID) error {
	d.mu.Lock()
	cfg, ok := d.configs[id]
	d.mu.Unlock()
	if !ok {
		return fmt.Errorf("brain: StartRegistration: network %d not registered", id)
	}
	for _, a := range registration.Start(cfg.PrimaryNick, cfg.Pass, cfg.Username, cfg.Realname, false) {
		if err := d.sendLine(id, a.Line); err != nil {
			return err
		}
	}
	d.armDeadline(id)
	return nil
}

// armDeadline starts (or restarts) id's registration-timeout clock. Total,
// not idle: it isn't reset by lines arriving, only cleared by reaching a
// terminal phase (see disarmDeadline) — see DefaultRegistrationTimeout's
// doc comment for why total-since-Start is the right shape, not an idle
// reset (that's the keeper's ReadIdleTimeout's job, one layer down).
func (d *Driver) armDeadline(id keeper.NetworkID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.deadlines[id]; ok {
		t.Stop()
	}
	d.deadlines[id] = time.AfterFunc(d.registrationTimeout, func() {
		d.failRegistration(id, fmt.Errorf("registration deadline exceeded (%s)", d.registrationTimeout))
		d.armReconnect(id) // still connected at the wire level; failRegistration's Close needs a redial armed to actually retry
	})
}

// disarmDeadline cancels id's pending deadline, if any. Called once a
// network reaches a terminal phase by any path (normal completion, the
// deadline itself, or a mid-registration disconnect) so a stale timer
// can't fire a second, spurious Result over an already-terminal State.
func (d *Driver) disarmDeadline(id keeper.NetworkID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.deadlines[id]; ok {
		t.Stop()
		delete(d.deadlines, id)
	}
}

// failRegistration moves a still-registering network to PhaseFailed and
// publishes the Result — the shared tail end of the registration deadline,
// a mid-registration disconnect, and a nick-ladder/SASL-required/ERROR
// ActionFailed from handleLine. Guarded on Phase so it's safe to call from
// any of those paths (or more than one, racing) without double-firing a
// Result over a State that's already terminal: whichever gets d.mu first
// wins, and the other becomes a no-op.
//
// Also deliberately closes id's connection, unconditionally: two of
// failRegistration's three callers (the deadline timer, and handleLine's
// ActionFailed branch for a nick ladder exhausted or SASL required but
// failed) reach this while the connection is still fully alive at the wire
// level — nothing else would ever tear it down, and a network stuck
// unregistered forever on a live socket is exactly the kind of silent stall
// this whole reconnect mechanism exists to avoid. Safe to call unconditionally
// even when the connection is already gone (the mid-registration-disconnect
// caller): Keeper.Close is a no-op when there's nothing to close. Callers
// are responsible for calling armReconnect themselves afterward — this
// function only closes, it doesn't schedule the redial, since one caller
// (handleNetworkEvent) already has its own single armReconnect call
// covering both the registering and already-registered disconnect cases.
func (d *Driver) failRegistration(id keeper.NetworkID, err error) {
	d.mu.Lock()
	s, tracked := d.states[id]
	if !tracked || s.Phase == registration.PhaseComplete || s.Phase == registration.PhaseFailed {
		d.mu.Unlock()
		return
	}
	s.Phase = registration.PhaseFailed
	s.Err = err
	d.states[id] = s
	if t, ok := d.deadlines[id]; ok {
		t.Stop()
		delete(d.deadlines, id)
	}
	d.mu.Unlock()
	trySendResult(d.results, Result{Network: id, State: s})
	_ = d.client.SendClose(id)
}

// State returns the current registration.State for id and whether it's
// being tracked at all.
func (d *Driver) State(id keeper.NetworkID) (registration.State, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.states[id]
	return s, ok
}

// Run reads the client's event stream until ctx is done or the connection
// ends, driving every tracked network's registration.State and
// republishing every other event kind. Blocking — run it in its own
// goroutine. The caller is responsible for calling client.SendLiveReady
// before or concurrently with Run (the read loop will simply see nothing
// until the keeper starts delivering).
//
// ctx alone cannot stop Run once it's blocked in client.Next(): that's a
// plain network read with no ctx-tied deadline, and Run only re-checks
// ctx.Err() between frames. Canceling ctx without also closing client
// leaves Run blocked indefinitely waiting for the next frame. This is not
// a gap in practice: a real brain process exiting (the scenario this
// exists for — see QuitNetwork's doc comment) closes every file
// descriptor, including the attach connection, as part of normal process
// teardown, which unblocks client.Next() with an error on its own. A
// caller that wants Run to stop without the process actually exiting must
// close client itself, not rely on ctx in isolation.
func (d *Driver) Run(ctx context.Context) error {
	defer close(d.results)
	defer close(d.lines)
	defer close(d.dialResults)
	defer close(d.closeResults)
	defer close(d.writeResults)
	defer close(d.quitCloseResults)
	defer close(d.netEvents)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ev, err := d.client.Next()
		if err != nil {
			return err
		}
		switch {
		case ev.Line != nil:
			d.handleLine(*ev.Line)
		case ev.DialResult != nil:
			if ev.DialResult.OK {
				d.recordEpoch(ev.DialResult.Network, ev.DialResult.Epoch)
				d.resetBackoff(ev.DialResult.Network)
				// EventConnected is published by Keeper.Dial before the
				// listener subscribes fan-in, so this brain typically
				// never sees it for a fresh dial. DialResult.OK is the
				// reliable "socket is live" signal — start the idle-PING
				// loop here.
				d.noteRX(ev.DialResult.Network)
				d.startKeepaliveIfNeeded(ev.DialResult.Network)
			} else {
				d.armReconnect(ev.DialResult.Network)
			}
			trySendDialResult(d.dialResults, *ev.DialResult)
		case ev.CloseResult != nil:
			d.notifyCloseWaiter(*ev.CloseResult)
			trySendCloseResult(d.closeResults, *ev.CloseResult)
		case ev.WriteResult != nil:
			if ev.WriteResult.Refused {
				d.clearFloodQueue(ev.WriteResult.Network)
			}
			trySendWriteResult(d.writeResults, *ev.WriteResult)
		case ev.QuitCloseResult != nil:
			trySendQuitCloseResult(d.quitCloseResults, *ev.QuitCloseResult)
		case ev.BlobPushResult != nil:
			d.notifyBlobPushWaiter(*ev.BlobPushResult)
		case ev.NetworkEvent != nil:
			if ev.NetworkEvent.Kind == keeper.EventConnected {
				d.recordEpoch(ev.NetworkEvent.Network, ev.NetworkEvent.Epoch)
			}
			d.handleNetworkEvent(*ev.NetworkEvent)
			trySendNetworkEvent(d.netEvents, *ev.NetworkEvent)
		}
	}
}

func (d *Driver) handleLine(line keeper.LineMsg) {
	d.mu.Lock()
	state, tracked := d.states[line.Network]
	d.mu.Unlock()
	if !tracked {
		return
	}

	gobnclog.IRC(d.log, d.peerLabel(line.Network), "<<", string(line.Raw))

	// LineMsg.Raw is []byte specifically because server-sent content isn't
	// guaranteed valid UTF-8 (see keeper's protocol doc) — irc.Parse takes
	// a string, but a Go string is just a byte container; this conversion
	// doesn't validate or normalize anything, it's the same bytes.
	msg, err := irc.Parse(string(line.Raw))
	if err != nil {
		trySendLine(d.lines, line)
		return // unparseable line; nothing registration.Step can act on
	}

	// Nick recovery reacts to the same parsed traffic, independent of
	// registration.Step — it only matters post-registration (Step
	// itself is a no-op there), and doesn't need or want Step's own
	// interpretation of these lines. A 303 that belongs to our own ISON
	// poll is consumed here and not republished: Session would otherwise
	// broadcast it to every downlink (internal/uplink hid these).
	if d.handleNickRecoveryTraffic(line.Network, msg) {
		return
	}
	trySendLine(d.lines, line)
	// 303 is excluded: ISON reclaim's own replies must not count as
	// liveness, or the keepalive PING never fires while recovery is
	// polling — see noteRX's doc comment.
	if msg.Command != "303" {
		d.noteRX(line.Network)
	}

	newState, actions := registration.Step(state, registration.Input{Msg: msg})

	// Learning our own ident/host is intentionally narrow: RPL_LOGGEDIN
	// (900, SASL), CHGHOST targeting self, or an observed nick!user@host
	// prefix matching self (Session.applyState's touchUserFromPrefixLocked,
	// fired for every line with a source — normally a self-JOIN echo).
	// A proactive USERHOST-on-001 poll used to live here, but its reply
	// isn't always the real cloak (observed diverging from the real host,
	// e.g. a raw connecting address) — a resumed network still gets a
	// narrower, blob-gated USERHOST fallback (Session.RefreshSelfUserHost)
	// when it has no cloak to seed from, since it has no JOIN echo to wait
	// on either.

	d.mu.Lock()
	d.states[line.Network] = newState
	d.mu.Unlock()

	for _, a := range actions {
		switch a.Kind {
		case registration.ActionSend:
			_ = d.sendLine(line.Network, a.Line) // best-effort; a write failure surfaces as a later WriteResult or a dead connection
		case registration.ActionRegistered:
			d.disarmDeadline(line.Network)
			d.setCurrentNick(line.Network, newState.Nick)
			trySendResult(d.results, Result{Network: line.Network, State: newState})
			d.joinChannels(line.Network)
			d.startNickRecoveryIfNeeded(line.Network)
		case registration.ActionFailed:
			d.disarmDeadline(line.Network)
			trySendResult(d.results, Result{Network: line.Network, State: newState})
			// Still connected at the wire level (nick ladder exhausted,
			// SASL required but failed, or a server ERROR that hasn't
			// necessarily torn down the socket yet) — close and arm a
			// redial, same treatment failRegistration's other callers
			// give a still-live connection. See failRegistration's doc
			// comment for why this doesn't happen on its own otherwise.
			_ = d.client.SendClose(line.Network)
			d.armReconnect(line.Network)
		}
	}
}

// handleNetworkEvent starts the idle-PING loop on EventConnected, and on
// EventDisconnected resolves a still-registering network's State to
// PhaseFailed instead of leaving it stuck with no Result ever sent — see
// the pre-3b regression net in docs/keeper-design.md, which is what this
// closes. A network already at a terminal phase (registered, or already
// failed by this same path or the deadline) is a no-op via
// failRegistration's own Phase guard.
func (d *Driver) handleNetworkEvent(ev keeper.NetworkEventMsg) {
	if ev.Kind == keeper.EventConnected {
		d.noteRX(ev.Network)
		d.startKeepaliveIfNeeded(ev.Network)
		return
	}
	if ev.Kind != keeper.EventDisconnected {
		return
	}
	d.mu.Lock()
	current := d.epochs[ev.Network]
	d.mu.Unlock()
	if ev.Epoch < current {
		// A disconnect notification for a connection Reconnect has
		// already superseded — see Reconnect's doc comment for exactly
		// why this can happen and why the fix is here, not there.
		return
	}
	// A genuine disconnect on the current epoch — whether or not
	// registration had already completed (failRegistration below is a
	// no-op past a terminal Phase, but a nick-recovery loop can very much
	// still be running at that point and needs tearing down here, not
	// just on the next fresh registration attempt).
	d.stopNickRecovery(ev.Network)
	d.clearNickRecoveryHold(ev.Network)
	d.stopKeepalive(ev.Network)
	msg := "uplink disconnected during registration"
	if ev.Error != "" {
		msg += ": " + ev.Error
	}
	d.failRegistration(ev.Network, fmt.Errorf("%s", msg))
	d.armReconnect(ev.Network)
}

// recordEpoch remembers id's current epoch, taking the higher of what's
// already known and epoch — a plain overwrite would let a legitimately
// in-order update still lose to network/goroutine reordering; taking the
// max is idempotent under any arrival order for values that only ever
// increase (both real epochs from the keeper and Reconnect's own
// optimistic bump do).
func (d *Driver) recordEpoch(id keeper.NetworkID, epoch uint64) {
	d.mu.Lock()
	if epoch > d.epochs[id] {
		d.epochs[id] = epoch
	}
	d.mu.Unlock()
}

// trySend* helpers: non-blocking, matching keeper's own publish pattern —
// a slow consumer of these channels shouldn't be able to stall the read
// loop that's also responsible for driving registration forward and for
// draining the keeper unix socket. Driver.lines is sized to absorb a
// WHO/NAMES burst (thousands of lines) so a healthy demux never hits the
// default drop; a wedged demux still must not deadlock the attach.

func trySendResult(ch chan Result, v Result) {
	select {
	case ch <- v:
	default:
	}
}

func trySendLine(ch chan keeper.LineMsg, v keeper.LineMsg) {
	select {
	case ch <- v:
	default:
	}
}

func trySendDialResult(ch chan keeper.DialResultMsg, v keeper.DialResultMsg) {
	select {
	case ch <- v:
	default:
	}
}

func trySendCloseResult(ch chan keeper.CloseResultMsg, v keeper.CloseResultMsg) {
	select {
	case ch <- v:
	default:
	}
}

func trySendWriteResult(ch chan keeper.WriteResultMsg, v keeper.WriteResultMsg) {
	select {
	case ch <- v:
	default:
	}
}

func trySendNetworkEvent(ch chan keeper.NetworkEventMsg, v keeper.NetworkEventMsg) {
	select {
	case ch <- v:
	default:
	}
}

func trySendQuitCloseResult(ch chan keeper.QuitCloseResultMsg, v keeper.QuitCloseResultMsg) {
	select {
	case ch <- v:
	default:
	}
}
