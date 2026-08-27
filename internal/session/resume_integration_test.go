//go:build integration

// Full-stack held-resume integration: a real in-process keeper (Manager +
// Listener + AttachClient) + brain.Driver + Session + SQLite store, with a
// client attached, driven through an actual uplink drop and draft/resume-0.5
// reconnect against a fake resume-capable ircd. Proves the pieces wired
// together do what the unit tests prove in isolation: on a resumable drop
// the client is held (not kicked), the stored token is presented on the
// auto-redial, RESUME SUCCESS resumes the session, and the client is kept.
//
//	go test -tags=integration ./internal/session/ -run FullStackHeldResume -v
package session

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

// resumeFakeIRCd accepts two connections on one listener: the first
// registers and is issued a token, then is dropped on command; the second
// resumes it. The in-process keeper answers PING autonomously, so no ping
// cookie handling is needed here.
type resumeFakeIRCd struct {
	ln      net.Listener
	mu      sync.Mutex
	attempt int
	dropC   chan struct{} // closed to drop conn 1
}

func newResumeFakeIRCd(t *testing.T) *resumeFakeIRCd {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &resumeFakeIRCd{ln: ln, dropC: make(chan struct{})}
	t.Cleanup(func() { _ = ln.Close() })
	go f.serve(t)
	return f
}

func (f *resumeFakeIRCd) hostPort() (string, int) {
	h, p, _ := net.SplitHostPort(f.ln.Addr().String())
	var port int
	fmt.Sscanf(p, "%d", &port)
	return h, port
}

func (f *resumeFakeIRCd) drop() { close(f.dropC) }

func (f *resumeFakeIRCd) serve(t *testing.T) {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.attempt++
		attempt := f.attempt
		f.mu.Unlock()
		go f.handle(conn, attempt)
	}
}

func (f *resumeFakeIRCd) handle(conn net.Conn, attempt int) {
	defer conn.Close()
	send := func(s string) { _, _ = io.WriteString(conn, s+"\r\n") }
	buf := make([]byte, 4096)
	var pending string
	readLine := func() (string, bool) {
		for {
			if i := strings.Index(pending, "\r\n"); i >= 0 {
				line := pending[:i]
				pending = pending[i+2:]
				return line, true
			}
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				return "", false
			}
			pending += string(buf[:n])
		}
	}
	burst := func(nick string) {
		send(":fake 001 " + nick + " :Welcome")
		send(":fake 002 " + nick + " :Your host is fake")
		send(":fake 003 " + nick + " :Created today")
		send(":fake 004 " + nick + " fake test-1.0 a a")
		send(":fake 005 " + nick + " NICKLEN=30 :are supported by this server")
		send(":fake 376 " + nick + " :End of MOTD")
	}
	nick := "gobnc"
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case strings.HasPrefix(line, "CAP LS"):
			send(":fake CAP * LS :message-tags draft/resume-0.5")
		case strings.HasPrefix(line, "NICK "):
			nick = strings.TrimSpace(line[len("NICK "):])
		case strings.HasPrefix(line, "CAP REQ"):
			send(":fake CAP * ACK :message-tags draft/resume-0.5")
			if attempt == 1 {
				send(":fake RESUME TOKEN abc.def")
			}
		case strings.HasPrefix(line, "RESUME "):
			send(":fake RESUME SUCCESS :" + nick)
			burst(nick)
		case line == "CAP END":
			burst(nick)
			if attempt == 1 {
				<-f.dropC // hold conn 1 open until the test drops it
				return
			}
		}
	}
}

func TestFullStackHeldResume(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	if _, err := db.UpsertNetwork(ctx, store.Network{
		Name: "n", Host: "irc.example", Port: 1, Nick: "gobnc", Enabled: true,
		Username: "u", Realname: "r",
	}); err != nil {
		t.Fatal(err)
	}
	netCfg, err := db.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	s := New(netCfg, db, nil, nil, nil)

	// A client attaches before the uplink registers (awaitingUplink).
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}

	fake := newResumeFakeIRCd(t)
	host, port := fake.hostPort()
	tu := newTestUplink(t, s, netCfg, host, port)
	_ = tu

	// Registers on conn 1 and is issued a token.
	waitUntil(t, 5*time.Second, func() bool { return s.Registered() })
	waitUntil(t, 3*time.Second, func() bool {
		tok, _ := db.ResumeToken(ctx, netCfg.ID)
		return tok != ""
	})
	if !hasCmd(d, "001") {
		t.Fatalf("client never saw the initial welcome: %v", sentCommands(d))
	}
	d.clearSent()

	// Drop the uplink; the keeper detects EOF and the session must hold the
	// client (resumable: cap negotiated + token stored), not kick it.
	fake.drop()
	waitUntil(t, 5*time.Second, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.resuming
	})
	if d.wasClosed() || hasCmd(d, "ERROR") {
		t.Fatalf("client kicked on a resumable drop: closed=%v sent=%v", d.wasClosed(), sentCommands(d))
	}

	// The auto-redial presents the token; RESUME SUCCESS resumes the
	// session and the held client is kept.
	waitUntil(t, 10*time.Second, func() bool { return s.Registered() })
	s.mu.RLock()
	resuming := s.resuming
	s.mu.RUnlock()
	if resuming {
		t.Fatal("still resuming after the reconnect completed")
	}
	if d.wasClosed() {
		t.Fatal("held client was closed after a successful resume")
	}
	// The duplicate welcome must not have been re-shown to the held client.
	if hasCmd(d, "001") {
		t.Fatalf("held client saw a duplicate welcome on resume: %v", sentCommands(d))
	}
	sawResumed := false
	for _, m := range d.snapshot() {
		if m.Command == "NOTICE" && strings.Contains(m.Trailing(), "resumed") {
			sawResumed = true
		}
	}
	if !sawResumed {
		t.Fatalf("no 'Session resumed' NOTICE: %v", d.snapshot())
	}
}
