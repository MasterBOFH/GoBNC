// Package connio provides line-oriented IRC connection helpers.
package connio

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
)

// Conn is a thread-safe IRC line writer/reader.
type Conn struct {
	c      net.Conn
	r      *bufio.Reader
	wmu    sync.Mutex
	closed bool
	log    *slog.Logger
	peer   string
}

// New wraps a net.Conn.
func New(c net.Conn) *Conn {
	return &Conn{c: c, r: bufio.NewReader(c)}
}

// SetLogger enables debug raw traffic logging for this connection.
func (c *Conn) SetLogger(l *slog.Logger, peer string) {
	c.log = l
	c.peer = peer
}

// ReadLine reads one IRC line (no CRLF), with optional deadline.
func (c *Conn) ReadLine(deadline time.Time) (string, error) {
	if !deadline.IsZero() {
		_ = c.c.SetReadDeadline(deadline)
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if c.log != nil {
		gobnclog.IRC(c.log, c.peer, "<<", line)
	}
	return line, nil
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
