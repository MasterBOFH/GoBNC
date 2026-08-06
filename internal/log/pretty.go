package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/mattn/go-isatty"
)

// IRC logs a raw IRC line at debug level (peer e.g. "Undernet" or "Undernet/c1", dir "<<" or ">>").
func IRC(l *slog.Logger, peer, dir, line string) {
	if l == nil {
		return
	}
	l.Debug("irc", "peer", peer, "dir", dir, "line", RedactIRC(line))
}

// RedactIRC masks secrets in PASS / AUTHENTICATE lines for logging.
func RedactIRC(line string) string {
	upper := strings.ToUpper(line)
	if strings.HasPrefix(upper, "PASS ") {
		rest := strings.TrimSpace(line[5:])
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			return "PASS " + rest[:i+1] + "***"
		}
		return "PASS ***"
	}
	if strings.HasPrefix(upper, "AUTHENTICATE ") && !strings.EqualFold(strings.TrimSpace(line[13:]), "+") {
		return "AUTHENTICATE ***"
	}
	return line
}

// prettyHandler is a columnized, optionally colored console handler for debug mode.
type prettyHandler struct {
	w      io.Writer
	level  slog.Level
	mu     sync.Mutex
	color  bool
	attrs  []slog.Attr
	groups []string
}

func newPrettyHandler(w io.Writer, level slog.Level) *prettyHandler {
	color := false
	if f, ok := w.(*os.File); ok {
		color = isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
	}
	return &prettyHandler{w: w, level: level, color: color}
}

func (h *prettyHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *prettyHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := append([]slog.Attr(nil), h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})

	ts := r.Time.Local().Format("15:04:05.000")
	level := levelLabel(r.Level)
	peer, dir, line, rest := splitPrettyAttrs(attrs)

	var b strings.Builder
	b.Grow(128 + len(r.Message) + len(line))
	b.WriteString(h.paint(colorDim, fmt.Sprintf("%-12s", ts)))
	b.WriteByte(' ')
	b.WriteString(h.levelPaint(r.Level, fmt.Sprintf("%-5s", level)))
	b.WriteByte(' ')

	if r.Message == "irc" || (dir != "" && line != "") {
		if peer == "" {
			peer = "-"
		}
		if dir == "" {
			dir = "·"
		}
		b.WriteString(h.paint(colorCyan, fmt.Sprintf("%-18s", truncate(peer, 18))))
		b.WriteByte(' ')
		b.WriteString(h.dirPaint(dir, fmt.Sprintf("%-2s", dir)))
		b.WriteByte(' ')
		b.WriteString(h.dirPaint(dir, line))
		if r.Message != "irc" && r.Message != "" {
			b.WriteString(h.paint(colorDim, "  "+r.Message))
		}
	} else {
		comp := peer
		if comp == "" {
			comp = "-"
		}
		b.WriteString(h.paint(colorCyan, fmt.Sprintf("%-18s", truncate(comp, 18))))
		b.WriteByte(' ')
		b.WriteString(fmt.Sprintf("%-2s", " "))
		b.WriteByte(' ')
		b.WriteString(r.Message)
		if rest != "" {
			b.WriteString(h.paint(colorDim, "  "+rest))
		}
	}
	b.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

func (h *prettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := *h
	nh.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &nh
}

func (h *prettyHandler) WithGroup(name string) slog.Handler {
	nh := *h
	nh.groups = append(append([]string(nil), h.groups...), name)
	return &nh
}

func splitPrettyAttrs(attrs []slog.Attr) (peer, dir, line, rest string) {
	var extras []string
	for _, a := range attrs {
		switch a.Key {
		case "peer", "uplink", "network", "downlink":
			if peer == "" {
				peer = a.Value.String()
			} else if a.Key != "peer" {
				extras = append(extras, a.Key+"="+a.Value.String())
			}
		case "dir":
			dir = a.Value.String()
		case "line":
			line = a.Value.String()
		default:
			extras = append(extras, a.Key+"="+fmt.Sprint(a.Value.Any()))
		}
	}
	return peer, dir, line, strings.Join(extras, " ")
}

func levelLabel(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "ERROR"
	case l >= slog.LevelWarn:
		return "WARN"
	case l >= slog.LevelInfo:
		return "INFO"
	default:
		return "DEBUG"
	}
}

const (
	colorReset   = "\033[0m"
	colorDim     = "\033[2m"
	colorRed     = "\033[31m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorBlue    = "\033[34m"
	colorMagenta = "\033[35m"
	colorCyan    = "\033[36m"
	colorBold    = "\033[1m"
)

func (h *prettyHandler) paint(code, s string) string {
	if !h.color {
		return s
	}
	return code + s + colorReset
}

func (h *prettyHandler) levelPaint(l slog.Level, s string) string {
	if !h.color {
		return s
	}
	switch {
	case l >= slog.LevelError:
		return colorBold + colorRed + s + colorReset
	case l >= slog.LevelWarn:
		return colorBold + colorYellow + s + colorReset
	case l >= slog.LevelInfo:
		return colorBold + colorGreen + s + colorReset
	default:
		return colorBold + colorBlue + s + colorReset
	}
}

func (h *prettyHandler) dirPaint(dir, s string) string {
	if !h.color {
		return s
	}
	switch dir {
	case "<<", "in", "<":
		return colorMagenta + s + colorReset
	case ">>", "out", ">":
		return colorGreen + s + colorReset
	default:
		return s
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}