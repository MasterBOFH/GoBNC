package wsconn

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// wsEchoServer stands up a real coder/websocket server on a TCP listener
// that speaks the IRCv3 subprotocols. Each received message is echoed back
// with " ->" appended, and it can be told to send unsolicited lines. This
// is a genuine WS peer (full RFC 6455 framing, masking, close handshake),
// not a mock — the adapter is exercised against the real protocol.
type wsEchoServer struct {
	ln       net.Listener
	got      chan string
	sendType websocket.MessageType
	subs     []string // advertised; empty = default (binary,text)
}

func newWSServer(t *testing.T, subs []string) *wsEchoServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &wsEchoServer{ln: ln, got: make(chan string, 64), subs: subs}
	if s.subs == nil {
		s.subs = Subprotocols
	}
	hs := &http.Server{Handler: http.HandlerFunc(s.handle)}
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })
	return s
}

func (s *wsEchoServer) url() string {
	return "ws://" + s.ln.Addr().String() + "/"
}

func (s *wsEchoServer) handle(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: s.subs})
	if err != nil {
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	c.SetReadLimit(-1)
	for {
		typ, data, err := c.Read(r.Context())
		if err != nil {
			return
		}
		s.got <- string(data)
		reply := append([]byte(nil), data...)
		reply = append(reply, []byte(" ->")...)
		if err := c.Write(r.Context(), typ, reply); err != nil {
			return
		}
	}
}

func dialClient(t *testing.T, s *wsEchoServer) (net.Conn, string) {
	t.Helper()
	raw, err := net.Dial("tcp", s.ln.Addr().String())
	if err != nil {
		t.Fatalf("tcp dial: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// url host is cosmetic here (Host header); the transport is `raw`.
	nc, sub, err := Client(ctx, raw, "ws://"+s.ln.Addr().String()+"/", 4096)
	if err != nil {
		t.Fatalf("Client handshake: %v", err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	return nc, sub
}

func TestClientPrefersBinarySubprotocol(t *testing.T) {
	s := newWSServer(t, Subprotocols)
	_, sub := dialClient(t, s)
	if sub != "binary.ircv3.net" {
		t.Fatalf("negotiated %q, want binary.ircv3.net (preferred)", sub)
	}
}

func TestClientFallsBackToTextWhenOnlyTextOffered(t *testing.T) {
	s := newWSServer(t, []string{"text.ircv3.net"})
	nc, sub := dialClient(t, s)
	if sub != "text.ircv3.net" {
		t.Fatalf("negotiated %q, want text.ircv3.net", sub)
	}
	// Text transport must still carry a line intact.
	roundTrip(t, s, nc, "PRIVMSG #x :hi")
}

// roundTrip writes one line and asserts (a) the server received it WITHOUT
// any CRLF, and (b) the adapter reads back the echo as a clean line.
func roundTrip(t *testing.T, s *wsEchoServer, nc net.Conn, line string) {
	t.Helper()
	if _, err := io.WriteString(nc, line+"\r\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case got := <-s.got:
		if got != line {
			t.Fatalf("server received %q, want %q (CRLF must be stripped, message = one line)", got, line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server never received the line")
	}
	br := bufio.NewReader(nc)
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	echo, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if got := strings.TrimRight(echo, "\r\n"); got != line+" ->" {
		t.Fatalf("read %q, want %q", got, line+" ->")
	}
}

func TestWriteStripsCRLFAndReadAppendsNewline(t *testing.T) {
	s := newWSServer(t, Subprotocols)
	nc, _ := dialClient(t, s)
	roundTrip(t, s, nc, "NICK bob")
}

// A message larger than the Read buffer must be reassembled across Read
// calls (the leftover path), and still surface as one line to bufio.
func TestLargeMessageSpansReadBuffer(t *testing.T) {
	s := newWSServer(t, Subprotocols)
	nc, _ := dialClient(t, s)
	long := "PRIVMSG #x :" + strings.Repeat("y", 2000)
	if _, err := io.WriteString(nc, long+"\r\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	<-s.got
	// Read into deliberately tiny buffers to force the leftover path.
	var acc []byte
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 7)
	for !strings.Contains(string(acc), "\n") {
		n, err := nc.Read(buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		acc = append(acc, buf[:n]...)
	}
	if got := strings.TrimRight(string(acc), "\r\n"); got != long+" ->" {
		t.Fatalf("reassembled %q (len %d), want the full echoed line (len %d)", got[:20]+"...", len(got), len(long)+3)
	}
}

// A non-UTF-8 line survives the binary subprotocol byte-for-byte — the
// reason binary is preferred (a Latin-1 network's bytes would be mangled
// over a text message).
func TestBinaryPreservesNonUTF8(t *testing.T) {
	s := newWSServer(t, Subprotocols)
	nc, sub := dialClient(t, s)
	if sub != "binary.ircv3.net" {
		t.Skipf("need binary, got %q", sub)
	}
	line := "PRIVMSG #x :\xff\xfe latin1"
	if _, err := io.WriteString(nc, line+"\r\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case got := <-s.got:
		if got != line {
			t.Fatalf("server got %q, want the raw bytes %q", got, line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no message")
	}
}

// A clean server close surfaces to the reader as io.EOF, exactly like a TCP
// FIN — so the keeper's read loop treats a WS hang-up as an ordinary
// disconnect, not an unrecognized websocket error.
func TestServerCloseSurfacesAsEOF(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	hs := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: Subprotocols})
		if err != nil {
			return
		}
		c.Close(websocket.StatusNormalClosure, "bye")
	})}
	go func() { _ = hs.Serve(ln) }()
	defer hs.Close()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nc, _, err := Client(ctx, raw, "ws://"+ln.Addr().String()+"/", 4096)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer nc.Close()
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = nc.Read(make([]byte, 64))
	if err != io.EOF {
		t.Fatalf("read after server close: err=%v, want io.EOF", err)
	}
}

// A read deadline that elapses returns an error (so the keeper's
// ReadIdleTimeout can detect a silent uplink), and the connection ctx is
// cancelled on Close so a blocked read unblocks.
func TestReadDeadlineFires(t *testing.T) {
	s := newWSServer(t, Subprotocols) // echoes only; sends nothing unsolicited
	nc, _ := dialClient(t, s)
	_ = nc.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	start := time.Now()
	_, err := nc.Read(make([]byte, 64))
	if err == nil {
		t.Fatal("read past an elapsed deadline returned nil error")
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("read blocked %s past its 150ms deadline", el)
	}
}

func TestCloseUnblocksRead(t *testing.T) {
	s := newWSServer(t, Subprotocols)
	nc, _ := dialClient(t, s)
	done := make(chan error, 1)
	go func() { _, err := nc.Read(make([]byte, 64)); done <- err }()
	time.Sleep(100 * time.Millisecond)
	_ = nc.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Read did not unblock after Close")
	}
}

func TestConnIsNetConn(t *testing.T) {
	var _ net.Conn = (*conn)(nil)
	_ = fmt.Sprint(Subprotocols)
}
