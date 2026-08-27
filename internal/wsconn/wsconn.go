// Package wsconn adapts an IRCv3 WebSocket transport
// (https://ircv3.net/specs/extensions/websocket) to a net.Conn whose byte
// stream is line-framed, so the rest of GoBNC — connio's line reader, the
// keeper's ring, the downlink's bufio reader — consumes a WebSocket uplink
// or client exactly as it does a raw TCP/TLS one, with no awareness that
// the transport underneath is message-framed.
//
// The spec's framing rule is the whole reason a plain net.Conn adapter
// won't do: each WebSocket message is exactly one IRC line with NO trailing
// CRLF. So on read we append '\n' to each received message (the delimiter
// the line reader above expects) and on write we strip a trailing CRLF
// before sending the line as one message. coder/websocket's own NetConn
// helper is a byte stream — it neither delimits messages on read nor strips
// CRLF on write — so it cannot serve this; this type exists for exactly
// that gap.
//
// Subprotocol: binary.ircv3.net is preferred over text.ircv3.net because
// IRC lines are not guaranteed valid UTF-8 (Latin-1 networks exist) and the
// keeper already carries lines as opaque bytes for that reason; a text
// message would corrupt or drop a non-UTF-8 line. text is offered as a
// fallback, and a server that negotiates neither still works (we send
// whatever the negotiated type is, defaulting to binary).
package wsconn

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// Subprotocols is the IRCv3 WebSocket subprotocol offer, most-preferred
// first — see the package doc for why binary leads.
var Subprotocols = []string{"binary.ircv3.net", "text.ircv3.net"}

// msgTypeFor maps a negotiated subprotocol to the WebSocket message type we
// send. text.ircv3.net → text; everything else (binary.ircv3.net, or no
// subprotocol negotiated) → binary, the safe default for arbitrary bytes.
func msgTypeFor(subprotocol string) websocket.MessageType {
	if subprotocol == "text.ircv3.net" {
		return websocket.MessageText
	}
	return websocket.MessageBinary
}

// conn is the net.Conn adapter. One WebSocket message ↔ one IRC line.
type conn struct {
	ws      *websocket.Conn
	base    net.Conn // the underlying transport, for LocalAddr/RemoteAddr only
	msgType websocket.MessageType

	// ctx bounds the whole connection; Close cancels it so a blocked Read
	// or Write unblocks. Per-call deadlines derive child contexts from it.
	ctx    context.Context
	cancel context.CancelFunc

	rDeadline time.Time
	wDeadline time.Time

	// leftover holds bytes of a message not yet consumed by a Read whose
	// buffer was smaller than the message (message + its appended '\n').
	leftover []byte
}

// newConn wraps an established *websocket.Conn. maxLine bounds a single
// inbound message (DoS guard, per the spec's size-limit recommendation);
// <=0 leaves coder's default in place.
func newConn(ws *websocket.Conn, base net.Conn, subprotocol string, maxLine int) *conn {
	if maxLine > 0 {
		ws.SetReadLimit(int64(maxLine))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &conn{
		ws:      ws,
		base:    base,
		msgType: msgTypeFor(subprotocol),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// readCtx / writeCtx derive a per-call context honoring the current
// deadline. A zero deadline means no timeout — just the connection ctx, so
// Close still unblocks it.
func deriveCtx(base context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		return base, func() {}
	}
	return context.WithDeadline(base, deadline)
}

func (c *conn) Read(p []byte) (int, error) {
	if len(c.leftover) > 0 {
		n := copy(p, c.leftover)
		c.leftover = c.leftover[n:]
		return n, nil
	}
	ctx, cancel := deriveCtx(c.ctx, c.rDeadline)
	defer cancel()
	_, data, err := c.ws.Read(ctx)
	if err != nil {
		return 0, mapErr(err)
	}
	// One message = one line; the reader above splits on '\n', which the
	// message does not carry (spec: no trailing CRLF), so add it.
	data = append(data, '\n')
	n := copy(p, data)
	if n < len(data) {
		c.leftover = data[n:]
	}
	return n, nil
}

func (c *conn) Write(p []byte) (int, error) {
	ctx, cancel := deriveCtx(c.ctx, c.wDeadline)
	defer cancel()
	// The line reader/writer above frames with CRLF; the spec forbids it on
	// the wire, so strip it. Written as whole lines by connio.WriteLine and
	// the downlink writer (one io.WriteString per line), but split on any
	// embedded newline defensively so a multi-line write still maps to one
	// message per line rather than one malformed message.
	body := strings.TrimRight(string(p), "\r\n")
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimRight(line, "\r")
		if err := c.ws.Write(ctx, c.msgType, []byte(line)); err != nil {
			return 0, mapErr(err)
		}
	}
	// Report the whole buffer consumed: the CRLF we stripped was real input
	// the caller expects accounted for, and a short count trips io.ErrShortWrite.
	return len(p), nil
}

// mapErr normalizes a clean WebSocket close to io.EOF, so the line reader
// above sees the same end-of-stream it gets from a TCP FIN rather than a
// websocket-specific error it wouldn't recognize.
func mapErr(err error) error {
	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return io.EOF
	}
	return err
}

func (c *conn) Close() error {
	c.cancel()
	// StatusNormalClosure is the clean-shutdown code; best-effort, the
	// underlying transport close is what actually frees the socket.
	return c.ws.Close(websocket.StatusNormalClosure, "")
}

func (c *conn) LocalAddr() net.Addr  { return c.base.LocalAddr() }
func (c *conn) RemoteAddr() net.Addr { return c.base.RemoteAddr() }

func (c *conn) SetDeadline(t time.Time) error {
	c.rDeadline, c.wDeadline = t, t
	return nil
}
func (c *conn) SetReadDeadline(t time.Time) error  { c.rDeadline = t; return nil }
func (c *conn) SetWriteDeadline(t time.Time) error { c.wDeadline = t; return nil }

// Client performs the IRCv3 WebSocket client handshake over an
// already-established transport (the keeper's TCP+TLS net.Conn — TLS, SNI,
// cert-from-disk and bind-host all stay the keeper's concern, below this
// layer) and returns a line-framed net.Conn plus the negotiated
// subprotocol.
//
// urlStr is the WebSocket URL whose host supplies the HTTP Host header and
// whose path is the upgrade path (e.g. "wss://irc.example:443/"). The
// scheme is not used to originate TLS — transport is already encrypted — so
// pass the real host:port; ctx bounds only the handshake, not the returned
// connection's lifetime.
func Client(ctx context.Context, transport net.Conn, urlStr string, maxLine int) (net.Conn, string, error) {
	ws, resp, err := websocket.Dial(ctx, urlStr, &websocket.DialOptions{
		Subprotocols: Subprotocols,
		// Hand coder the transport we already dialled and TLS-wrapped,
		// for every address, so it does the HTTP Upgrade over it and
		// never opens its own socket or repeats TLS. HTTP/1.1 only:
		// WebSocket upgrades require it, and h2 would break the hijack.
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				DialContext:       func(context.Context, string, string) (net.Conn, error) { return transport, nil },
				DialTLSContext:    func(context.Context, string, string) (net.Conn, error) { return transport, nil },
				ForceAttemptHTTP2: false,
			},
		},
	})
	if err != nil {
		return nil, "", err
	}
	_ = resp
	sub := ws.Subprotocol()
	return newConn(ws, transport, sub, maxLine), sub, nil
}
