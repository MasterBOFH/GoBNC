// Package connio provides line-oriented IRC connection helpers.
package connio

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
)

// Conn is a thread-safe IRC line writer/reader.
type Conn struct {
	c       net.Conn
	r       *bufio.Reader
	maxLine int
	wmu     sync.Mutex
	closed  bool
	log     *slog.Logger
	peer    string
}

// New wraps a net.Conn with the given max line length (including trailing CRLF).
// If maxLine <= 0, irc.MaxServerLine is used.
func New(c net.Conn, maxLine int) *Conn {
	if maxLine <= 0 {
		maxLine = irc.MaxServerLine
	}
	return &Conn{c: c, r: bufio.NewReaderSize(c, maxLine), maxLine: maxLine}
}

// SetLogger enables debug raw traffic logging for this connection.
func (c *Conn) SetLogger(l *slog.Logger, peer string) {
	c.log = l
	c.peer = peer
}

// ReadLine reads one IRC line (no CRLF), with optional deadline.
// Returns irc.ErrLineTooLong if the peer exceeds maxLine (remainder drained through '\n').
func (c *Conn) ReadLine(deadline time.Time) (string, error) {
	if !deadline.IsZero() {
		_ = c.c.SetReadDeadline(deadline)
	}
	line, err := ReadLimitedLine(c.r, c.maxLine)
	if err != nil {
		return "", err
	}
	if c.log != nil {
		gobnclog.IRC(c.log, c.peer, "<<", line)
	}
	return line, nil
}

// ReadLimitedLine reads until '\n', accepting at most max bytes including the delimiter.
// On overflow it drains through the next '\n' (or EOF) and returns irc.ErrLineTooLong.
func ReadLimitedLine(r *bufio.Reader, max int) (string, error) {
	if max <= 0 {
		max = irc.MaxServerLine
	}
	var buf []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			if len(buf) > 0 && errors.Is(err, io.EOF) {
				return strings.TrimRight(string(buf), "\r\n"), nil
			}
			return "", err
		}
		buf = append(buf, b)
		if b == '\n' {
			return strings.TrimRight(string(buf), "\r\n"), nil
		}
		if len(buf) >= max {
			if err := drainToNewline(r); err != nil && !errors.Is(err, io.EOF) {
				return "", err
			}
			return "", irc.ErrLineTooLong
		}
	}
}

func drainToNewline(r *bufio.Reader) error {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		if b == '\n' {
			return nil
		}
	}
}

// WriteLine writes a line with CRLF.
func (c *Conn) WriteLine(line string) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	raw := line
	if !strings.HasSuffix(line, "\r\n") {
		line += "\r\n"
	} else {
		raw = strings.TrimRight(line, "\r\n")
	}
	if c.log != nil {
		gobnclog.IRC(c.log, c.peer, ">>", raw)
	}
	_, err := io.WriteString(c.c, line)
	return err
}

// Underlying returns the net.Conn.
func (c *Conn) Underlying() net.Conn { return c.c }

// Close closes the connection.
func (c *Conn) Close() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.c.Close()
}
