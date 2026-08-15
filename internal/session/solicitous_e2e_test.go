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

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestSolicitousE2EConcurrentWHOXAndHold exercises two downlinks against a
// scripted uplink: concurrent WHOX demux with lag, plus LIST hold-until-end.
// The WHOX replies (354/315) come from the fake server itself, over the
// real keeper<->brain wire (see runSolicitousServer) — not injected
// directly into Session — so this actually proves demux-by-token holds up
// through the full real architecture, not just in-process. The LIST
// hold/release replies (323) stay directly injected: that half is testing
// RequestTracker's own serialization of two same-command requests from
// different clients, a Session-local property with no keeper/brain wire
// step in the middle to verify.
func TestSolicitousE2EConcurrentWHOXAndHold(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "me", Username: "u", Realname: "r"}
	s := New(netCfg, nil, nil, nil, nil)

	uplinkWHO := make(chan string, 8)
	uplinkLIST := make(chan string, 4)

	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runSolicitousServer(conn, time.Now().Add(12*time.Second), uplinkWHO, uplinkLIST)
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool {
		if !s.Registered() {
			return false
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.isupport != nil && s.isupport.WHOX
	})

	d1 := &fakeDL{id: "c1", caps: map[string]bool{}}
	d2 := &fakeDL{id: "c2", caps: map[string]bool{}}
	if err := s.Attach(d1); err != nil {
		t.Fatal(err)
	}
	if err := s.Attach(d2); err != nil {
		t.Fatal(err)
	}
	d1.clearSent()
	d2.clearSent()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d1, irc.Message{Command: "WHO", Params: []string{"#a", "%tuhnfr,1"}})
	}()
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d2, irc.Message{Command: "WHO", Params: []string{"#b", "%tuhnfr,2"}})
	}()

	var who1, who2 string
	for i := 0; i < 2; i++ {
		select {
		case w := <-uplinkWHO:
			if strings.Contains(w, "#a") {
				who1 = w
			} else if strings.Contains(w, "#b") {
				who2 = w
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for uplink WHO")
		}
	}
	wg.Wait()
	if who1 == "" || who2 == "" {
		t.Fatalf("missing WHO lines: %q %q", who1, who2)
	}
	tok1 := whoToken(who1)
	tok2 := whoToken(who2)
	if tok1 == "" || tok2 == "" || tok1 == tok2 {
		t.Fatalf("tokens: %q %q from %q / %q", tok1, tok2, who1, who2)
	}

	// The fake server (runSolicitousServer) replies with real 354/315 lines
	// itself, over the real wire, once it has seen both WHO requests — no
	// direct HandleMessage injection here anymore (see this test's own doc
	// comment).
	waitUntil(t, 2*time.Second, func() bool {
		return countCmd(d1, "354") >= 1 && countCmd(d2, "354") >= 1
	})
	if countCmd(d1, "354") != 1 || countCmd(d2, "354") != 1 {
		t.Fatalf("354 demux d1=%d d2=%d sent1=%v sent2=%v",
			countCmd(d1, "354"), countCmd(d2, "354"), d1.snapshot(), d2.snapshot())
	}
	// Client tokens restored (they sent ,1 / ,2); bodies must not cross.
	m1 := firstCmd(d1, "354")
	m2 := firstCmd(d2, "354")
	if m1 == nil || m2 == nil {
		t.Fatal("missing 354")
	}
	if got := m1.Param(1); got != "1" {
		t.Fatalf("c1 token restore: %q", got)
	}
	if got := m2.Param(1); got != "2" {
		t.Fatalf("c2 token restore: %q", got)
	}
	if m1.Param(2) != "#a" || m2.Param(2) != "#b" {
		t.Fatalf("crossed bodies: c1=%v c2=%v", m1.Params, m2.Params)
	}

	d1.clearSent()
	d2.clearSent()

	listDone := make(chan error, 1)
	go func() {
		listDone <- s.HandleClientMessage(d1, irc.Message{Command: "LIST"})
	}()
	select {
	case line := <-uplinkLIST:
		if !strings.HasPrefix(line, "LIST") {
			t.Fatalf("want LIST, got %q", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first LIST not sent upstream")
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- s.HandleClientMessage(d2, irc.Message{Command: "LIST"})
	}()
	<-secondStarted
	time.Sleep(100 * time.Millisecond)
	select {
	case line := <-uplinkLIST:
		t.Fatalf("second LIST sent too early: %q", line)
	default:
	}

	time.Sleep(50 * time.Millisecond)
	s.HandleMessage(irc.Message{Command: "323", Params: []string{"me", "End of LIST"}})
	select {
	case err := <-listDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first LIST handler stuck")
	}
	select {
	case line := <-uplinkLIST:
		if !strings.HasPrefix(line, "LIST") {
			t.Fatalf("want second LIST, got %q", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second LIST not released after 323")
	}
	s.HandleMessage(irc.Message{Command: "323", Params: []string{"me", "End of LIST"}})
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second LIST handler stuck")
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Log("script:", err)
		}
	case <-time.After(2 * time.Second):
	}
}

func countCmd(d *fakeDL, cmd string) int {
	n := 0
	for _, m := range d.snapshot() {
		if m.Command == cmd {
			n++
		}
	}
	return n
}

func firstCmd(d *fakeDL, cmd string) *irc.Message {
	for _, m := range d.snapshot() {
		if m.Command == cmd {
			msg := m
			return &msg
		}
	}
	return nil
}

func whoToken(line string) string {
	i := strings.LastIndex(line, ",")
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(line[i+1:])
}

func runSolicitousServer(c net.Conn, deadline time.Time, who, list chan<- string) error {
	br := newLineBuf(c)
	read := func() (string, error) {
		_ = c.SetReadDeadline(deadline)
		return br.readLine()
	}
	write := func(s string) error {
		_, err := io.WriteString(c, s+"\r\n")
		return err
	}
	for _, want := range []string{"CAP LS", "NICK", "USER"} {
		line, err := read()
		if err != nil || !strings.Contains(line, want) {
			return fmt.Errorf("%s: %q %v", want, line, err)
		}
	}
	if err := write("CAP * LS :message-tags server-time batch cap-notify"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || !strings.Contains(line, "CAP REQ") {
		return fmt.Errorf("CAP REQ: %q %v", line, err)
	}
	if err := write("CAP * ACK :message-tags server-time batch cap-notify"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	if err := write(":server 005 me CHANMODES=b,k,l,imnpst PREFIX=(ov)@+ CASEMAPPING=rfc1459 WHOX :are supported by this server"); err != nil {
		return err
	}
	if err := write(":server 001 me :Welcome"); err != nil {
		return err
	}
	if err := write(":server 002 me :host"); err != nil {
		return err
	}
	if err := write(":server 003 me :created"); err != nil {
		return err
	}
	if err := write(":server 004 me server ircd iow nt"); err != nil {
		return err
	}
	if err := write(":server 376 me :End of /MOTD command."); err != nil {
		return err
	}

	whoTokByChan := map[string]string{}
	for time.Now().Before(deadline) {
		_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		line, err := br.readLine()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
		switch {
		case strings.HasPrefix(line, "PING"):
			_ = write("PONG " + strings.TrimPrefix(line, "PING "))
		case strings.HasPrefix(line, "WHO "):
			select {
			case who <- line:
			default:
			}
			chName, tok := "", whoToken(line)
			if strings.Contains(line, "#a") {
				chName = "#a"
			} else if strings.Contains(line, "#b") {
				chName = "#b"
			}
			if chName != "" && tok != "" {
				whoTokByChan[chName] = tok
			}
			// Reply only once both channels' WHO have arrived — real
			// replies over the real wire, deliberately in reversed
			// (#b-then-#a) and cross-numeric order, the same
			// out-of-request-order shape the original in-process
			// injection used to stress demux under lag.
			if tokA, tokB := whoTokByChan["#a"], whoTokByChan["#b"]; tokA != "" && tokB != "" {
				time.Sleep(50 * time.Millisecond)
				_ = write(":server 354 me " + tokB + " #b n2")
				_ = write(":server 354 me " + tokA + " #a n1")
				_ = write(":server 315 me #a :End")
				_ = write(":server 315 me #b :End")
			}
		case strings.HasPrefix(line, "LIST"):
			select {
			case list <- line:
			default:
			}
		}
	}
	return context.DeadlineExceeded
}
