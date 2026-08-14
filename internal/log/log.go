// Package log provides a thin slog wrapper with common helpers.
package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

// Options configures console/file logging.
type Options struct {
	Level     string    // console: debug, info, warn, error
	FileLevel string    // JSON file level; empty means same as Level
	Console   io.Writer // default stderr; human-readable when Level is debug
	// File, if set, receives JSON logs at FileLevel (or Level).
	// In debug console mode this is the structured sink; console stays text.
	File string
}

// Sink owns the active log outputs and can Reload on rehash.
type Sink struct {
	swap     *swapHandler
	mu       sync.Mutex
	closer   func() error
	registry *DebugRegistry
}

// DebugRegistry returns the sink's live /bnc-debug routing table (see
// internal/log/debug.go) — constructed once here, in Setup, and never
// touched by Reload, so a live subscription survives a rehash. nil-safe:
// returns nil for a nil Sink, matching this package's other nil-receiver
// methods.
func (s *Sink) DebugRegistry() *DebugRegistry {
	if s == nil {
		return nil
	}
	return s.registry
}

// New returns a JSON slog logger at the given level writing to w (default stderr).
// Prefer Setup for process logging (debug text console + optional JSON file).
func New(level string, w io.Writer) *slog.Logger {
	l, _, _ := Setup(Options{Level: level, Console: w})
	return l
}

// Setup builds a logger from Options.
// Debug console: text to console, JSON to File when File is set.
// Other levels: JSON to console; also JSON to File when File is set.
// The returned Sink closes the log file and can Reload options after rehash.
func Setup(opts Options) (*slog.Logger, *Sink, error) {
	inner, closer, err := buildHandler(opts)
	if err != nil {
		return nil, nil, err
	}
	sh := &swapHandler{}
	sh.store(inner)
	reg := NewDebugRegistry()
	tee := newTeeHandler(sh, reg)
	return slog.New(tee), &Sink{swap: sh, closer: closer, registry: reg}, nil
}

// Reload replaces the active handlers (level / log file) without replacing the Logger.
// Child loggers from Logger.With keep working. On failure the previous sink stays active.
func (s *Sink) Reload(opts Options) error {
	if s == nil {
		return fmt.Errorf("nil log sink")
	}
	inner, closer, err := buildHandler(opts)
	if err != nil {
		return err
	}
	s.mu.Lock()
	old := s.closer
	s.swap.store(inner)
	s.closer = closer
	s.mu.Unlock()
	if old != nil {
		_ = old()
	}
	return nil
}

// Close closes the log file (if any).
func (s *Sink) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	c := s.closer
	s.closer = func() error { return nil }
	s.mu.Unlock()
	if c == nil {
		return nil
	}
	return c()
}

func buildHandler(opts Options) (slog.Handler, func() error, error) {
	console := opts.Console
	if console == nil {
		console = os.Stderr
	}
	consoleLv := parseLevel(opts.Level)
	fileLv := consoleLv
	if opts.FileLevel != "" {
		fileLv = parseLevel(opts.FileLevel)
	}
	pretty := consoleLv <= slog.LevelDebug

	var handlers []slog.Handler
	closer := func() error { return nil }

	if pretty {
		handlers = append(handlers, newPrettyHandler(console, consoleLv))
	} else {
		handlers = append(handlers, slog.NewJSONHandler(console, &slog.HandlerOptions{Level: consoleLv}))
	}

	if opts.File != "" {
		f, err := os.OpenFile(opts.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, nil, fmt.Errorf("open log file: %w", err)
		}
		// Tighten perms on pre-existing world-readable logs.
		_ = os.Chmod(opts.File, 0o600)
		handlers = append(handlers, slog.NewJSONHandler(f, &slog.HandlerOptions{Level: fileLv}))
		closer = f.Close
	}

	var h slog.Handler
	if len(handlers) == 1 {
		h = handlers[0]
	} else {
		h = multiHandler(handlers)
	}
	return h, closer, nil
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// swapHandler delegates to a replaceable root handler so Reload updates all loggers.
type swapHandler struct {
	v atomic.Value // slog.Handler
}

func (h *swapHandler) store(inner slog.Handler) {
	h.v.Store(inner)
}

func (h *swapHandler) load() slog.Handler {
	return h.v.Load().(slog.Handler)
}

func (h *swapHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.load().Enabled(ctx, level)
}

func (h *swapHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.load().Handle(ctx, r)
}

func (h *swapHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &dynHandler{swap: h, attrs: attrs}
}

func (h *swapHandler) WithGroup(name string) slog.Handler {
	return &dynHandler{swap: h, group: name}
}

// dynHandler re-resolves against the current swap root so Reload stays visible.
type dynHandler struct {
	swap   *swapHandler
	parent *dynHandler
	group  string
	attrs  []slog.Attr
}

func (d *dynHandler) resolve() slog.Handler {
	var chain []*dynHandler
	for x := d; x != nil; x = x.parent {
		chain = append(chain, x)
	}
	h := d.swap.load()
	for i := len(chain) - 1; i >= 0; i-- {
		c := chain[i]
		if c.group != "" {
			h = h.WithGroup(c.group)
		}
		if len(c.attrs) > 0 {
			h = h.WithAttrs(c.attrs)
		}
	}
	return h
}

func (d *dynHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return d.resolve().Enabled(ctx, level)
}

func (d *dynHandler) Handle(ctx context.Context, r slog.Record) error {
	return d.resolve().Handle(ctx, r)
}

func (d *dynHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &dynHandler{swap: d.swap, parent: d, attrs: attrs}
}

func (d *dynHandler) WithGroup(name string) slog.Handler {
	return &dynHandler{swap: d.swap, parent: d, group: name}
}

// multiHandler fans a record out to several handlers.
type multiHandler []slog.Handler

func (h multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, hh := range h {
		if hh.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var first error
	for _, hh := range h {
		if !hh.Enabled(ctx, r.Level) {
			continue
		}
		if err := hh.Handle(ctx, r.Clone()); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (h multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(multiHandler, len(h))
	for i, hh := range h {
		out[i] = hh.WithAttrs(attrs)
	}
	return out
}

func (h multiHandler) WithGroup(name string) slog.Handler {
	out := make(multiHandler, len(h))
	for i, hh := range h {
		out[i] = hh.WithGroup(name)
	}
	return out
}

// With returns a child logger with attrs.
func With(l *slog.Logger, args ...any) *slog.Logger {
	if l == nil {
		l = slog.Default()
	}
	return l.With(args...)
}

// FromContext returns a logger from ctx or Default.
func FromContext(ctx context.Context) *slog.Logger {
	if v := ctx.Value(ctxKey{}); v != nil {
		if l, ok := v.(*slog.Logger); ok && l != nil {
			return l
		}
	}
	return slog.Default()
}

type ctxKey struct{}

// ContextWith stores logger in ctx.
func ContextWith(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}
