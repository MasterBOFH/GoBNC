package log

import (
	"context"
	"log/slog"
	"strings"
	"sync"
)

// DebugMode selects what a DebugTarget receives.
type DebugMode int

const (
	DebugOff DebugMode = iota
	DebugRaw
	DebugLog
	DebugAll
)

// DebugTarget receives matching records for one subscription (see
// DebugRegistry.Subscribe). Both methods are called from DebugRegistry.Run,
// never from the goroutine that produced the original log record — a slow
// or blocked DeliverRaw/DeliverLog (e.g. a downlink connection stalled on
// write) must never stall whatever code just logged something.
type DebugTarget interface {
	DeliverRaw(dir, line string)
	DeliverLog(level, msg string, attrs map[string]string)
}

// pendingDelivery is one record already resolved against its subscriber
// list at enqueue time — Run just replays it, no further registry lookups.
type pendingDelivery struct {
	targets []DebugTarget
	raw     bool
	dir     string
	line    string
	level   string
	msg     string
	attrs   map[string]string
}

// DebugRegistry is the live routing table between logged records and
// per-network debug subscribers (see /bnc debug in internal/session).
// Constructed once per Sink (see Setup) and untouched by Reload, so a live
// subscription survives a rehash.
type DebugRegistry struct {
	mu      sync.Mutex
	subs    map[string]map[DebugTarget]DebugMode // network -> target -> mode
	pending chan pendingDelivery
}

// NewDebugRegistry returns an empty registry. Run must be started
// separately (by whoever owns the process's run context — see
// internal/server.Server.Run) before deliveries actually happen; enqueuing
// before that, or after Run's context is done, silently drops (best-effort,
// matching the trySendX pattern used throughout internal/keeper/internal/brain).
func NewDebugRegistry() *DebugRegistry {
	return &DebugRegistry{
		subs:    make(map[string]map[DebugTarget]DebugMode),
		pending: make(chan pendingDelivery, 256),
	}
}

// Subscribe registers t for network's traffic at mode, replacing any
// existing subscription for the same (network, t) pair.
func (r *DebugRegistry) Subscribe(network string, t DebugTarget, mode DebugMode) {
	r.mu.Lock()
	if r.subs[network] == nil {
		r.subs[network] = make(map[DebugTarget]DebugMode)
	}
	r.subs[network][t] = mode
	r.mu.Unlock()
}

// Unsubscribe removes t's subscription to network, if any.
func (r *DebugRegistry) Unsubscribe(network string, t DebugTarget) {
	r.mu.Lock()
	if m := r.subs[network]; m != nil {
		delete(m, t)
		if len(m) == 0 {
			delete(r.subs, network)
		}
	}
	r.mu.Unlock()
}

// hasAny reports whether any subscription exists at all — teeHandler's
// Enabled fallback, so a live subscription forces matching records through
// regardless of the configured console/file level.
func (r *DebugRegistry) hasAny() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs) > 0
}

func (r *DebugRegistry) targetsFor(network string, want1, want2 DebugMode) []DebugTarget {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.subs[network]
	if len(m) == 0 {
		return nil
	}
	var out []DebugTarget
	for t, mode := range m {
		if mode == want1 || mode == want2 {
			out = append(out, t)
		}
	}
	return out
}

// enqueueIRC handles a raw-traffic record (see teeHandler.tap). peer is
// gobnclog.IRC's own peer argument — the network's bare display name for
// uplink traffic, or "network/clientID" for downlink traffic (see
// internal/downlink's logPeer) — split on "/" to recover the network name
// either way.
func (r *DebugRegistry) enqueueIRC(peer, dir, line string) {
	network, _, _ := strings.Cut(peer, "/")
	if network == "" {
		return
	}
	targets := r.targetsFor(network, DebugRaw, DebugAll)
	if len(targets) == 0 {
		return
	}
	select {
	case r.pending <- pendingDelivery{targets: targets, raw: true, dir: dir, line: line}:
	default: // best-effort; a burst outpacing Run just drops for this record
	}
}

// enqueueLog handles a non-raw record already known to belong to network
// (see teeHandler.tap for how that's resolved).
func (r *DebugRegistry) enqueueLog(network, level, msg string, attrs map[string]string) {
	targets := r.targetsFor(network, DebugLog, DebugAll)
	if len(targets) == 0 {
		return
	}
	select {
	case r.pending <- pendingDelivery{targets: targets, raw: false, level: level, msg: msg, attrs: attrs}:
	default:
	}
}

// Run drains queued deliveries until ctx is done — the one goroutine that
// ever calls a DebugTarget's methods (see DebugTarget's doc comment for
// why that matters). Safe to never call at all (enqueue just drops
// forever); safe to call more than once only if you like duplicate
// delivery, so don't.
func (r *DebugRegistry) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-r.pending:
			for _, t := range p.targets {
				if p.raw {
					t.DeliverRaw(p.dir, p.line)
				} else {
					t.DeliverLog(p.level, p.msg, p.attrs)
				}
			}
		}
	}
}

// teeHandler wraps the reloadable handler chain (see swapHandler) with a
// side channel into a DebugRegistry, so /bnc debug works independent of
// the configured console/file log level and survives Reload untouched —
// unlike the handlers buildHandler constructs, this is built once in Setup
// and never rebuilt.
type teeHandler struct {
	inner slog.Handler
	reg   *DebugRegistry
	attrs []slog.Attr // bound via WithAttrs — r itself never carries these, see tap
}

func newTeeHandler(inner slog.Handler, reg *DebugRegistry) *teeHandler {
	return &teeHandler{inner: inner, reg: reg}
}

func (t *teeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return t.inner.Enabled(ctx, level) || t.reg.hasAny()
}

func (t *teeHandler) Handle(ctx context.Context, r slog.Record) error {
	t.tap(r)
	if t.inner.Enabled(ctx, r.Level) {
		return t.inner.Handle(ctx, r)
	}
	return nil
}

// tap resolves r (plus any attrs bound via WithAttrs, which r itself never
// carries — slog.Handler's own contract, see prettyHandler's identical
// attrs bookkeeping) against the registry. Two shapes: gobnclog.IRC's raw
// lines (Message=="irc", peer/dir/line attrs — see internal/log.IRC) route
// by peer; everything else routes by a "network" attr (bound on every
// internal/session-derived logger) or, failing that, a plain "name" attr
// (internal/server.Server's own network-lifecycle log lines use this
// instead — see the DebugRegistry doc in the plan/PR description for why
// this alias lives here rather than touching those call sites).
func (t *teeHandler) tap(r slog.Record) {
	if !t.reg.hasAny() {
		return
	}
	attrs := make(map[string]string, len(t.attrs)+r.NumAttrs())
	for _, a := range t.attrs {
		attrs[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})

	if r.Message == "irc" {
		peer := attrs["peer"]
		if peer == "" {
			return
		}
		t.reg.enqueueIRC(peer, attrs["dir"], attrs["line"])
		return
	}

	network := attrs["network"]
	if network == "" {
		network = attrs["name"]
	}
	if network == "" {
		return
	}
	delete(attrs, "network")
	delete(attrs, "name")
	t.reg.enqueueLog(network, r.Level.String(), r.Message, attrs)
}

func (t *teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &teeHandler{
		inner: t.inner.WithAttrs(attrs),
		reg:   t.reg,
		attrs: append(append([]slog.Attr(nil), t.attrs...), attrs...),
	}
}

func (t *teeHandler) WithGroup(name string) slog.Handler {
	return &teeHandler{inner: t.inner.WithGroup(name), reg: t.reg, attrs: t.attrs}
}
