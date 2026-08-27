package keeper

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/wsconn"
	"github.com/coder/websocket"
)

// wsIRCd is a fake ircd that speaks the IRCv3 WebSocket subprotocols. It
// records each line the keeper sends and can push lines back — a real WS
// peer, so Keeper.Dial's WebSocket path is exercised end to end (handshake,
// framing, the ring seeing framed lines), not just the wsconn unit.
type wsIRCd struct {
	ln   net.Listener
	got  chan string
	push chan string
}

func newWSIRCd(t *testing.T) *wsIRCd {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &wsIRCd{ln: ln, got: make(chan string, 64), push: make(chan string, 64)}
	hs := &http.Server{Handler: http.HandlerFunc(s.handle)}
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })
	return s
}

func (s *wsIRCd) handle(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: wsconn.Subprotocols})
	if err != nil {
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	ctx := r.Context()
	go func() {
		for {
			select {
			case line := <-s.push:
				if c.Write(ctx, websocket.MessageBinary, []byte(line)) != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		s.got <- string(data)
	}
}

// TestKeeperDialsWebSocketUplink proves Keeper.Dial's WebSocket path:
// the handshake completes, a line the keeper sends arrives at the server as
// one CRLF-free message, and a line the server pushes is framed back into
// the keeper's ring as an ordinary seq'd entry.
func TestKeeperDialsWebSocketUplink(t *testing.T) {
	srv := newWSIRCd(t)
	host, port := hostPort(srv.ln.Addr().String())

	k := New(1<<20, 4096, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port, WebSocket: true}); err != nil {
		t.Fatalf("Dial (websocket): %v", err)
	}
	t.Cleanup(func() { _ = k.Close() })

	if err := k.WriteLine("NICK gobnc"); err != nil {
		t.Fatalf("WriteLine: %v", err)
	}
	select {
	case got := <-srv.got:
		if got != "NICK gobnc" {
			t.Fatalf("server received %q, want %q (one CRLF-free WS message)", got, "NICK gobnc")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server never received the keeper's line over WebSocket")
	}

	// A server push must land in the ring as a normal line with a seq.
	sub, unsub := k.SubscribeLines()
	defer unsub()
	srv.push <- ":irc.example 001 gobnc :Welcome"
	select {
	case e := <-sub.Lines:
		if e.Line != ":irc.example 001 gobnc :Welcome" {
			t.Fatalf("ring line = %q, want the pushed 001", e.Line)
		}
		if e.Seq == 0 {
			t.Fatalf("framed WS line got seq 0, want a real sequence number")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pushed WS line never reached the ring")
	}

	// The keeper answers a server PING autonomously over WebSocket too —
	// its one piece of IRC interpretation must work regardless of transport.
	srv.push <- "PING :cookie123"
	select {
	case got := <-srv.got:
		if got != "PONG :cookie123" {
			t.Fatalf("server received %q, want autonomous PONG over WebSocket", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("keeper did not answer PING over WebSocket")
	}
}

// A WebSocket dial to a plain (non-WebSocket) server fails the handshake
// rather than silently proceeding — the transport is closed and the error
// surfaces, so the brain's reconnect/backoff sees a real failure.
func TestKeeperWebSocketHandshakeFailsOnPlainServer(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()
	go func() {
		c := srv.accept(t)
		// Speak no HTTP/WS at all; just hold and drain.
		buf := make([]byte, 256)
		for {
			if _, err := c.Read(buf); err != nil {
				return
			}
		}
	}()
	host, port := hostPort(srv.addr())

	k := New(1<<20, 4096, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := k.Dial(ctx, DialConfig{Host: host, Port: port, WebSocket: true})
	if err == nil {
		_ = k.Close()
		t.Fatal("WebSocket dial to a non-WebSocket server returned nil error")
	}
	if st, _ := k.State(); st == Connected {
		t.Fatalf("state=%v after a failed WS handshake, want not Connected", st)
	}
}
