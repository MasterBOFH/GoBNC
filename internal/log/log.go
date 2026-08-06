// Package log provides a thin slog wrapper with common helpers.
package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Options configures console/file logging.
type Options struct {
	Level   string    // debug, info, warn, error
	Console io.Writer // default stderr; human-readable when Level is debug
	// File, if set, receives JSON logs at the configured level.
	// In debug mode this is the structured sink; console stays text.
	File string
}

// New returns a JSON slog logger at the given level writing to w (default stderr).
// Prefer Setup for process logging (debug text console + optional JSON file).
func New(level string, w io.Writer) *slog.Logger {
	l, _, _ := Setup(Options{Level: level, Console: w})
	return l
}

// Setup builds a logger from Options.
// Debug mode: text to console, JSON to File when File is set.
// Other levels: JSON to console; also JSON to File when File is set.
// The returned closer flushes/closes the log file (may be a no-op).
func Setup(opts Options) (*slog.Logger, func() error, error) {
	console := opts.Console
	if console == nil {
		console = os.Stderr
	}
	lv := parseLevel(opts.Level)
	debug := lv <= slog.LevelDebug

	var handlers []slog.Handler
	var closer func() error = func() error { return nil }

	if debug {
		handlers = append(handlers, newPrettyHandler(console, lv))
	} else {
		handlers = append(handlers, slog.NewJSONHandler(console, &slog.HandlerOptions{Level: lv}))
	}

	if opts.File != "" {
		f, err := os.OpenFile(opts.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("open log file: %w", err)
		}
		handlers = append(handlers, slog.NewJSONHandler(f, &slog.HandlerOptions{Level: lv}))
		closer = f.Close
	}

	var h slog.Handler
	if len(handlers) == 1 {
		h = handlers[0]
	} else {
		h = multiHandler(handlers)
	}
	return slog.New(h), closer, nil
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
