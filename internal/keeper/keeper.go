// Package keeper owns the uplink TCP/TLS socket so it can survive a brain
// restart. It does line framing, answers server PING autonomously, and keeps
// a seq'd ring buffer of recently read lines. It never parses IRC beyond
// that — CAP, SASL, registration, and everything else that decides what a
// line means belongs to the brain, not here.
//
// Dialling is brain-driven: the keeper dials when told, closes when told,
// and reports when the socket has died. It does not retry or back off on
// its own — those are decisions, and decisions live in the brain.
package keeper

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/connio"
	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// State is the keeper's connection status. There are deliberately only two:
// whether socket death is "never dialed", "closed on request", or "died on
// its own" makes no behavioral difference to a caller deciding whether to
// dial — so it isn't modeled as a separate state. LastError distinguishes
// them for logging only.
type State int

const (
	NotConnected State = iota
	Connected
)

func (s State) String() string {
	if s == Connected {
		return "connected"
	}
	return "not_connected"
}

// EventKind identifies a state-transition event.
type EventKind int

const (
	EventConnected EventKind = iota
	EventDisconnected
)

func (k EventKind) String() string {
	if k == EventConnected {
		return "connected"
	}
	return "disconnected"
}

// Event is published when the socket's state changes on its own:
// EventConnected on a successful Dial, EventDisconnected only when the
// socket died on its own (read error, deadline, EOF), with Err carrying the
// cause. A deliberate Close() publishes no Event at all (see readLoop's
// !deliberate guard) — a caller that needs "this connection is gone"
// semantics for a close it requested itself must synthesize that signal on
// its own side (internal/brain's keepalive timeout does exactly this).
type Event struct {
	Kind  EventKind
	Epoch uint64
	Err   error
}

// DialConfig is the parameters for one dial attempt.
//
// TLS material (CertFile/KeyFile/CAFile) is read from disk inside Dial, on
// every call, never cached. That is what lets an operator rotate a cert file
// on disk and have it picked up on the next reconnect with no keeper
// involvement at all — do not "optimize" this by pre-loading certificates
// into a long-lived struct; that silently breaks rotation until a cert
// expires in production.
type DialConfig struct {
	Host string
	Port int

	TLS         bool
	TLSNoVerify bool
	ServerName  string // SNI override; empty = Host
	CertFile    string // client cert, e.g. for SASL EXTERNAL; empty = none
	KeyFile     string
	CAFile      string // CA bundle; empty = OS root pool
	MinVersion  uint16 // 0 = tls.VersionTLS12
	MaxVersion  uint16 // 0 = crypto/tls default (no cap)

	BindHost string

	DialTimeout time.Duration // 0 = 30s

	// ReadIdleTimeout bounds how long this connection waits for the uplink
	// to send anything at all before being treated as dead; 0 uses the
	// keeper's own default (see defaultReadIdleTimeout). Per-dial, not
	// keeper-wide, deliberately: the keeper stores no per-network
	// configuration anywhere else, and networks genuinely differ in PING
	// cadence — a keeper-wide value with a per-network override would
	// reintroduce exactly the config-on-the-keeper-side split this package
	// spent effort removing, for one field, and it wouldn't stay one field.
	ReadIdleTimeout time.Duration

	// Dial overrides the network dial for tests. Never set in production,
	// and never sent over the wire — encoding/json cannot marshal a func
	// value at all (unlike a nil pointer, there's no zero-value encoding
	// for it; it errors unconditionally), so this must stay tagged "-" for
	// DialConfig to be usable directly as DialRequestMsg's wire body.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error) `json:"-"`
}

func (c DialConfig) addr() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// ReadIdleTimeout bounds how long the read loop waits for the uplink to send
// anything at all before treating the socket as dead. This is a plain
// socket-level safety net (most ircds PING at least this often), not IRC
// interpretation.
const defaultReadIdleTimeout = 10 * time.Minute

// Keeper holds at most one live uplink connection at a time.
//
// Keeper-process-level configuration is deliberately almost nothing: the
// unix socket path and ring capacity (both restart-required — set at
// Manager/Listener construction, no live-reload path) and this logger.
// PRE-DEPLOYMENT OBLIGATION, not yet built: log is a plain field, set once
// at construction, with no way to reload or rotate it short of restarting
// the process — which is precisely the failure this whole project exists
// to avoid. A process designed to never need restarting cannot ship with
// an unrotatable log. The answer, when it's built, is the same pattern
// internal/server already uses for its own log (logSink.Reload(), wired to
// SIGHUP) — reuse that, don't invent a second one.
type Keeper struct {
	maxLine  int
	readIdle time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	conn    *connio.Conn
	state   State
	epoch   uint64
	lastErr error
	cancel  context.CancelFunc
	done    chan struct{}

	seqCounter uint64 // atomic; monotonic for the keeper's lifetime, never reset
	ring       *ring
	blob       *blobStore

	// deliveredSeq is the resume watermark: the highest seq a brain has
	// explicitly acked as fully processed (see AckSeq). A fresh attach
	// with no explicit FromSeq for this network starts here, not from
	// oldest-retained — this is what makes resume "gap only" rather than
	// a full backlog replay. It only ever moves forward on an explicit
	// ack, never on a bare wire-write (see AckSeq's doc comment for why
	// that distinction is load-bearing), and it survives an ordinary
	// disconnect/redial on this same Keeper instance (seq is monotonic
	// across epochs and never resets — see docs/keeper-design.md). It is
	// reset only by the ring-overflow-case-2 self-close path in readLoop,
	// where the ring has evicted entries this watermark still points at.
	deliveredSeq uint64

	subMu sync.Mutex
	subs  map[chan Event]struct{}

	lineSubMu sync.Mutex
	lineSubs  map[chan Entry]*lineSubState
}

// lineSubState tracks whether a line subscriber has fallen behind. Overflow
// is signaled once (via sync.Once) rather than repeatedly, so a caller
// selecting on it gets exactly one notification regardless of how many
// further sends fail to fit while it's catching up on the close.
type lineSubState struct {
	overflow     chan struct{}
	overflowOnce sync.Once
}

// Option configures a Keeper at construction.
type Option func(*Keeper)

// WithReadIdleTimeout overrides defaultReadIdleTimeout. This is the
// keeper's fallback, used when a Dial's DialConfig.ReadIdleTimeout is 0 —
// mainly a test convenience for setting a short default across many dials
// without repeating it in every DialConfig; production per-network tuning
// belongs in DialConfig, not here (see its doc comment).
func WithReadIdleTimeout(d time.Duration) Option {
	return func(k *Keeper) { k.readIdle = d }
}

// New creates a Keeper with no connection. ringCapacity bounds the seq'd
// line buffer; size it against a netsplit burst, not average throughput.
func New(maxLine, ringCapacity int, log *slog.Logger, opts ...Option) *Keeper {
	if log == nil {
		log = slog.Default()
	}
	k := &Keeper{
		maxLine:  maxLine,
		readIdle: defaultReadIdleTimeout,
		log:      log,
		ring:     newRing(ringCapacity),
		blob:     newBlobStore(),
		subs:     make(map[chan Event]struct{}),
		lineSubs: make(map[chan Entry]*lineSubState),
	}
	for _, opt := range opts {
		opt(k)
	}
	return k
}

// State returns the current connection state and the epoch of the
// connection that state describes.
//
// Epoch is the explicit, documented way to tell "never connected" apart from
// "connected once and died" — both present as State() == NotConnected, which
// is deliberate (see the package doc), but a caller with different
// obligations for the two cases (e.g. a future blob store, which must be
// cleared on a death but has nothing to clear on a cold start) needs to
// distinguish them:
//
//	epoch == 0                        -> Dial has never succeeded; nothing to invalidate
//	epoch >  0 && State() == NotConnected -> a connection died or was closed; anything
//	                                          derived from that epoch must be treated as stale
func (k *Keeper) State() (State, uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.state, k.epoch
}

// LastError returns the reason the most recent connection ended, or nil if
// it was closed deliberately or the keeper has never dialed.
func (k *Keeper) LastError() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lastErr
}

// LastSeq returns the highest seq assigned so far, or 0.
func (k *Keeper) LastSeq() uint64 {
	return k.ring.lastSeq()
}

// Since returns buffered lines with Seq > afterSeq, oldest first. ok is
// false if afterSeq has already fallen out of the retained window.
func (k *Keeper) Since(afterSeq uint64) ([]Entry, bool) {
	return k.ring.since(afterSeq)
}

// DroppedCount reports entries evicted from the ring before being read.
func (k *Keeper) DroppedCount() uint64 {
	return k.ring.droppedCount()
}

// PushBlob applies one brain-derived entry to this network's blob store.
// The keeper matches on key/mode and never inspects value — see blob.go.
func (k *Keeper) PushBlob(key string, mode BlobMode, value []byte) {
	k.blob.Push(key, mode, value)
}

// BlobSnapshot returns this network's current resolved blob state, as
// delivered to an attaching client in HelloAckMsg.
func (k *Keeper) BlobSnapshot() []BlobEntry {
	return k.blob.Snapshot()
}

// AckSeq advances this network's resume watermark to seq, if seq is newer
// than what's already acked. Called only once a brain has fully finished
// processing the line at that seq — including any blob push that line's
// processing triggered (see SeqAckMsg's doc comment) — never on the mere
// fact that the keeper wrote the line to the wire. That distinction is
// what keeps the push-derived-entry-before-advancing-the-checkpoint
// ordering rule in docs/keeper-design.md's blob store section true by
// construction: a brain that crashes between receiving a line and pushing
// its derived blob entry never sends the ack for that line, so the
// watermark never advances past it, and the next attach receives that
// line again along with every one after it.
func (k *Keeper) AckSeq(seq uint64) {
	k.mu.Lock()
	if seq > k.deliveredSeq {
		k.deliveredSeq = seq
	}
	k.mu.Unlock()
}

// DeliveredSeq returns the current resume watermark — see the field's own
// doc comment on Keeper.
func (k *Keeper) DeliveredSeq() uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.deliveredSeq
}

// Stats is a monitoring snapshot of the ring buffer — cheap to call
// repeatedly (a soak harness on a timer, a future health check), unlike
// Since which materializes entries.
type Stats struct {
	Occupancy    int       // entries currently retained
	Capacity     int       // ring capacity
	LastSeq      uint64    // highest seq currently retained, 0 if empty
	LastLineTime time.Time // Time of the most recently pushed entry, zero if empty
	Dropped      uint64    // entries evicted before ever being read via Since
}

// Stats returns a snapshot of ring buffer occupancy and recency.
func (k *Keeper) Stats() Stats {
	return k.ring.snapshot()
}

// Subscribe registers for state-transition events. The returned channel is
// buffered and non-blocking on the publish side: a slow subscriber can miss
// an intermediate event, but State()/LastError() are always authoritative.
// Call the returned cancel func to unsubscribe.
func (k *Keeper) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 16)
	k.subMu.Lock()
	k.subs[ch] = struct{}{}
	k.subMu.Unlock()
	cancel := func() {
		k.subMu.Lock()
		if _, ok := k.subs[ch]; ok {
			delete(k.subs, ch)
			close(ch)
		}
		k.subMu.Unlock()
	}
	return ch, cancel
}

func (k *Keeper) publish(ev Event) {
	k.subMu.Lock()
	defer k.subMu.Unlock()
	for ch := range k.subs {
		select {
		case ch <- ev:
		default:
			k.log.Warn("keeper: subscriber slow, dropping event", "kind", ev.Kind, "epoch", ev.Epoch)
		}
	}
}

// lineSubBuffer is the per-subscriber live-feed depth for SubscribeLines.
// Sized to absorb a large channel's WHO/NAMES reply burst without tripping
// Overflow on a healthy brain; Overflow is still the fallback when a
// subscriber lags past this, and Listener recovers from the ring rather
// than tearing down the attach (see fanInNetwork). Overridable in tests so
// overflow can be forced without flooding tens of thousands of lines.
var lineSubBuffer = 8192

// LineSubscription is one subscriber's feed from SubscribeLines.
type LineSubscription struct {
	// Lines delivers newly read entries in order.
	Lines <-chan Entry
	// Overflow is closed exactly once, the moment this subscriber falls too
	// far behind to keep buffering (Lines is full and a new entry has
	// nowhere to go). It is not a data channel — the correct reaction to it
	// firing is to stop trusting this subscription and catch up from the
	// ring (Since), not to keep reading Lines expecting a contiguous feed.
	// Silently dropping entries here would just move the "gap in the
	// stream with no indication" problem up one layer, from the ring
	// (which does at least report gaps via Since) to a live feed that
	// would otherwise report nothing at all. See fanInNetwork.
	Overflow <-chan struct{}
}

// SubscribeLines registers for newly read lines, in order, as they're
// pushed to the ring. Unlike Since, this is a live feed, not a query — a
// subscriber that wants what it missed before subscribing should read
// Since(0) first, then apply this feed on top (duplicates by seq are
// possible at that boundary and are the caller's to dedupe). Call the
// returned cancel func to unsubscribe.
//
// The read loop's push to a subscriber is always non-blocking (see
// publishLine) — a stalled subscriber signals Overflow and is otherwise
// skipped, it never blocks delivery to other subscribers, and it never
// blocks the read loop itself. See TestSlowSubscriberDoesNotBlockOthers and
// TestReadLoopNeverBlocksOnSubscriber.
func (k *Keeper) SubscribeLines() (LineSubscription, func()) {
	ch := make(chan Entry, lineSubBuffer)
	state := &lineSubState{overflow: make(chan struct{})}
	k.lineSubMu.Lock()
	k.lineSubs[ch] = state
	k.lineSubMu.Unlock()
	cancel := func() {
		k.lineSubMu.Lock()
		if _, ok := k.lineSubs[ch]; ok {
			delete(k.lineSubs, ch)
			close(ch)
		}
		k.lineSubMu.Unlock()
	}
	return LineSubscription{Lines: ch, Overflow: state.overflow}, cancel
}

func (k *Keeper) publishLine(e Entry) {
	k.lineSubMu.Lock()
	defer k.lineSubMu.Unlock()
	for ch, state := range k.lineSubs {
		select {
		case ch <- e:
		default:
			state.overflowOnce.Do(func() { close(state.overflow) })
		}
	}
}

// hasLineSubscribers reports whether any live-attached client is currently
// streaming this network's lines — the ring-overflow-case-2 policy in
// readLoop only self-closes when this is false; a subscriber that falls
// behind is already handled by its own Overflow signal (case 1), and
// self-closing on top of that would be redundant and would race the
// subscriber's own kill.
func (k *Keeper) hasLineSubscribers() bool {
	k.lineSubMu.Lock()
	defer k.lineSubMu.Unlock()
	return len(k.lineSubs) > 0
}

var (
	// ErrAlreadyConnected is returned by Dial when a connection is already live.
	ErrAlreadyConnected = errors.New("keeper: already connected")
)

// Dial opens a new uplink connection. It blocks until the TCP+TLS handshake
// completes (or fails), then returns; the read loop continues in the
// background until the socket dies or Close is called. Dial never retries —
// one call is one attempt, and the caller (the brain) decides what to do
// with a failure.
func (k *Keeper) Dial(ctx context.Context, cfg DialConfig) error {
	k.mu.Lock()
	if k.conn != nil {
		k.mu.Unlock()
		return ErrAlreadyConnected
	}
	k.mu.Unlock()

	conn, err := dialRaw(ctx, cfg)
	if err != nil {
		return err
	}

	// Deliberately never c.SetLogger(...): connio.Conn supports raw
	// traffic logging, but every line here includes real message content
	// — private messages included — from every network this keeper holds.
	// The keeper's own logging (this file, listener.go) is metadata only
	// (state, epoch, error strings) on purpose; wiring up raw-line logging
	// "for debugging" would put private messages on disk at whatever level
	// it's called at. If line-level debugging is ever needed, it belongs
	// gated behind something explicit and loud, not a call added here.
	c := connio.New(conn, k.maxLine)

	k.mu.Lock()
	if k.conn != nil {
		// Lost a race with a concurrent Dial; reject the newcomer.
		k.mu.Unlock()
		_ = c.Close()
		return ErrAlreadyConnected
	}
	k.epoch++
	epoch := k.epoch
	k.conn = c
	k.state = Connected
	k.lastErr = nil
	loopCtx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel
	done := make(chan struct{})
	k.done = done
	k.mu.Unlock()

	readIdle := cfg.ReadIdleTimeout
	if readIdle <= 0 {
		readIdle = k.readIdle
	}

	k.publish(Event{Kind: EventConnected, Epoch: epoch})
	go k.readLoop(loopCtx, c, epoch, readIdle, done)
	return nil
}

// Close closes the current connection, if any. It is idempotent and safe to
// call whether or not a connection is live. It does not redial — dialling
// again is a separate, brain-driven decision.
//
// Clears the blob itself, deliberately, rather than leaving that to
// readLoop's own cleanup (see the epoch-staleness comment below its own
// blob.Clear() call): Close mutates k.conn to nil before readLoop's
// blocked read ever unblocks, so by the time readLoop's cleanup checks
// "is this still the current connection" (stillCurrent := k.conn == c),
// it's always false for a deliberate Close specifically — Close itself
// already made it so. Skipping the clear there is correct given that
// check's purpose (don't let a stale readLoop instance react to a
// connection that's already been superseded), but it means a deliberate
// Close needs its own clear, or the blob silently survives it — found by
// writing a direct probe (Dial, PushBlob, Close, assert BlobSnapshot is
// empty) rather than trusting the "clearing is unconditional" claim in
// docs/keeper-design.md at face value; the probe failed before this line
// was added. QuitClose and Retire both funnel through Close, so this
// covers them too.
func (k *Keeper) Close() error {
	k.mu.Lock()
	c := k.conn
	cancel := k.cancel
	done := k.done
	if c == nil {
		k.mu.Unlock()
		return nil
	}
	k.conn = nil
	k.cancel = nil
	k.done = nil
	k.state = NotConnected
	k.lastErr = nil
	k.mu.Unlock()

	k.blob.Clear()

	if cancel != nil {
		cancel()
	}
	err := c.Close()
	if done != nil {
		<-done
	}
	return err
}

// Retire closes the current connection like Close, and additionally closes
// every existing Subscribe/SubscribeLines channel — for when this Keeper is
// being permanently removed (Manager.RemoveNetwork), not a transient
// disconnect a caller is expected to redial.
//
// This distinction matters because a subscription normally survives a
// Close/Dial cycle on purpose: SubscribeLines is registered once against a
// Keeper and keeps receiving entries across however many reconnects that
// Keeper goes through, so a live-attached client watching a network doesn't
// need to resubscribe on every redial. That's correct for a transient drop,
// but it means a goroutine fanning out that subscription has no signal to
// stop on when the Keeper is never coming back — Close alone leaves it
// parked forever, watching a channel nothing will ever write to again.
// Retire closing the channels gives it the ordinary Go signal (ok=false on
// receive) to exit.
func (k *Keeper) Retire() {
	_ = k.Close()

	k.subMu.Lock()
	for ch := range k.subs {
		delete(k.subs, ch)
		close(ch)
	}
	k.subMu.Unlock()

	k.lineSubMu.Lock()
	for ch := range k.lineSubs {
		delete(k.lineSubs, ch)
		close(ch)
	}
	k.lineSubMu.Unlock()
}

// WriteLine writes one line on the current connection. Exposed for the
// brain (once the keeper protocol exists) and for tests driving a live ircd
// through minimal registration; the keeper itself only ever writes PONG.
func (k *Keeper) WriteLine(line string) error {
	k.mu.Lock()
	c := k.conn
	k.mu.Unlock()
	if c == nil {
		return errNotConnected
	}
	return c.WriteLine(line)
}

// defaultQuitTimeout bounds QuitClose's write when the caller doesn't
// specify one — matches internal/uplink's old writeQuit fallback
// (SetWriteDeadline(time.Now().Add(5*time.Second)) when its ctx had no
// deadline), kept as the same value here so the observable behavior at the
// wire doesn't change just because the socket moved to a different process.
const defaultQuitTimeout = 5 * time.Second

// QuitClose writes line to the current connection with a bounded write
// deadline, then closes the connection regardless of whether the write
// completed or timed out — a single primitive rather than a separate
// Write-then-Close pair, because bounding the whole operation with one
// deadline is what internal/uplink's writeQuit did by reaching directly
// into the raw net.Conn it owned (c.Underlying().SetWriteDeadline); the
// brain has no socket of its own to do that on once the keeper owns it, so
// the keeper does it on the brain's behalf. timeout<=0 uses
// defaultQuitTimeout.
//
// This is deliberately the ONLY path that sends a final line and tears
// down a connection in one step — it exists for one caller: the brain
// choosing to disconnect from a specific network (e.g. QUIT). It must
// never be reached merely because the brain process is exiting (a code
// reload) — that case holds the socket and calls nothing here at all. See
// docs/keeper-design.md's shutdown-vs-disconnect distinction: conflating
// the two is the exact failure this project exists to prevent.
func (k *Keeper) QuitClose(line string, timeout time.Duration) error {
	k.mu.Lock()
	c := k.conn
	k.mu.Unlock()
	if c == nil {
		return errNotConnected
	}
	if timeout <= 0 {
		timeout = defaultQuitTimeout
	}
	_ = c.Underlying().SetWriteDeadline(time.Now().Add(timeout))
	writeErr := c.WriteLine(line)
	closeErr := k.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

var errNotConnected = errors.New("keeper: not connected")

func (k *Keeper) readLoop(ctx context.Context, c *connio.Conn, epoch uint64, readIdle time.Duration, done chan struct{}) {
	defer close(done)
	var exitErr error
	var ringOverflowClose bool
	for {
		line, err := c.ReadLine(time.Now().Add(readIdle))
		if err != nil {
			exitErr = err
			break
		}
		seq := k.nextSeq()
		entry := Entry{Seq: seq, Epoch: epoch, Line: line, Time: time.Now()}
		evicted := k.ring.push(entry)
		k.publishLine(entry)

		if evicted && !k.hasLineSubscribers() {
			// Ring-overflow case 2 (see ring.go's doc comment): the ring
			// evicted an entry with nobody attached to consume it, so a
			// future attach's resume watermark now points at a gap this
			// ring can no longer fill. Rather than let that future attach
			// discover the gap on its own (Since's ok=false) and have its
			// whole live session killed over one network's history, close
			// this network's uplink now, loudly, and reset its resume
			// watermark and blob — the same "drop and re-register" outcome
			// a missing blob already produces on attach, just triggered
			// proactively instead of waiting to be discovered.
			k.log.Warn("keeper: ring overflow with no subscriber, closing uplink", "epoch", epoch)
			exitErr = fmt.Errorf("ring overflow: no subscriber consumed line before eviction")
			ringOverflowClose = true
			_ = c.Close()
			break
		}

		if msg, perr := irc.Parse(line); perr == nil && isServerPing(msg) {
			if werr := c.WriteLine(pongFor(msg)); werr != nil {
				exitErr = werr
				break
			}
		}
	}

	deliberate := ctx.Err() != nil // Close() canceled the loop context
	k.mu.Lock()
	stillCurrent := k.conn == c
	if stillCurrent {
		k.conn = nil
		k.cancel = nil
		k.done = nil
		k.state = NotConnected
		if !deliberate {
			k.lastErr = exitErr
		}
		if ringOverflowClose {
			k.deliveredSeq = 0
		}
	}
	k.mu.Unlock()

	if stillCurrent {
		// Any state derived from epoch `epoch` (ISUPPORT, CAP, identity,
		// channel list) is stale the instant the connection that produced
		// it is gone, whether it ended because the brain asked for Close
		// or because the socket died on its own; the next Dial starts a
		// new epoch and a blank transcript. Deliberately not gated on
		// `deliberate` — a deliberate close followed by a redial to the
		// same network still needs fresh registration, so the old blob is
		// just as invalid either way. Done after releasing k.mu above:
		// k.mu also guards Dial/Close/State and must not be held across a
		// blob store call.
		//
		// stillCurrent is only ever false here for one reason: Close()
		// already reassigned k.conn to nil (and its own state fields)
		// before this cleanup got a chance to run, which is exactly the
		// deliberate-Close path — so this branch, despite the comment
		// above, never actually fires for a deliberate Close; that case
		// is covered by Close()'s own k.blob.Clear() call instead. Kept
		// unconditional on stillCurrent anyway, not narrowed to "only the
		// socket-died path": a future readLoop cleanup path that reaches
		// here with stillCurrent true must never have to remember to add
		// its own clear.
		k.blob.Clear()
	}

	if !deliberate && stillCurrent {
		k.log.Warn("keeper: uplink socket died", "epoch", epoch, "err", exitErr)
		k.publish(Event{Kind: EventDisconnected, Epoch: epoch, Err: exitErr})
	}
}

func (k *Keeper) nextSeq() uint64 {
	k.mu.Lock()
	k.seqCounter++
	s := k.seqCounter
	k.mu.Unlock()
	return s
}

// isServerPing reports whether msg is a server-originated PING.
// This — recognizing PING and constructing its PONG — is the one piece of
// IRC the keeper is allowed to know.
func isServerPing(msg irc.Message) bool {
	return strings.EqualFold(msg.Command, "PING")
}

func pongFor(msg irc.Message) string {
	return "PONG :" + msg.Trailing()
}

// dialRaw performs the TCP connect and, if requested, the TLS handshake, all
// bounded by ctx — a caller cancelling ctx aborts an in-progress handshake,
// not just the TCP connect.
func dialRaw(ctx context.Context, cfg DialConfig) (net.Conn, error) {
	addr := cfg.addr()
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dctx := ctx
	if _, ok := dctx.Deadline(); !ok {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var conn net.Conn
	var err error
	if cfg.Dial != nil {
		conn, err = cfg.Dial(dctx, "tcp", addr)
	} else {
		d := net.Dialer{Timeout: timeout}
		if cfg.BindHost != "" {
			la, lerr := resolveLocalAddr(cfg.BindHost)
			if lerr != nil {
				return nil, fmt.Errorf("bind_host: %w", lerr)
			}
			d.LocalAddr = la
		}
		conn, err = d.DialContext(dctx, "tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	if !cfg.TLS {
		return conn, nil
	}
	return tlsHandshake(dctx, conn, cfg)
}

func tlsHandshake(ctx context.Context, conn net.Conn, cfg DialConfig) (net.Conn, error) {
	tlsConf, err := buildTLSConfig(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	tc := tls.Client(conn, tlsConf)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tc, nil
}

// buildTLSConfig reads certificate/key/CA material from disk fresh on every
// call. Do not hoist this out of the per-dial path.
func buildTLSConfig(cfg DialConfig) (*tls.Config, error) {
	serverName := cfg.ServerName
	if serverName == "" {
		serverName = cfg.Host
	}
	minVersion := cfg.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS12
	}
	tlsConf := &tls.Config{
		ServerName:         serverName,
		MinVersion:         minVersion,
		MaxVersion:         cfg.MaxVersion,
		InsecureSkipVerify: cfg.TLSNoVerify,
	}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		pair, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("uplink tls client cert: %w", err)
		}
		tlsConf.Certificates = []tls.Certificate{pair}
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("uplink tls ca bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("uplink tls ca bundle: no certificates found in %s", cfg.CAFile)
		}
		tlsConf.RootCAs = pool
	}
	return tlsConf, nil
}

func resolveLocalAddr(bindHost string) (*net.TCPAddr, error) {
	ip := net.ParseIP(bindHost)
	if ip == nil {
		return nil, fmt.Errorf("invalid bind_host %q", bindHost)
	}
	return &net.TCPAddr{IP: ip}, nil
}
