package session

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestRequestTrackerWHOISKeepsClientCaseOnWire: the folded nick is the
// routing key only. What goes to the uplink — both the immediate write
// (WhoisWire) and a held write released later (Outbound) — must be the
// nick exactly as the client spelled it, or the server echoes the folded
// form back in 318 and the client sees "End of /WHOIS" for a nick it
// never typed.
func TestRequestTrackerWHOISKeepsClientCaseOnWire(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459

	var wire []string
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"MrIron"}, CaseMapping: cm, WhoisWire: &wire})
	if !w1 {
		t.Fatal("first WHOIS must write")
	}
	if len(wire) != 1 || wire[0] != "MrIron" {
		t.Fatalf("WhoisWire=%v want [MrIron]", wire)
	}

	// Different form (WHOIS nick nick) for the same nick: held behind the
	// first exchange; its released line must also carry the client's case.
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "WHOIS", WhoisTargets: []string{"mrIRON"}, CaseMapping: cm, Remote: whoisRemoteNick})
	if w2 {
		t.Fatal("WHOIS nick nick behind WHOIS nick must be held")
	}

	// Replies route by the folded key regardless of the server's spelling.
	got := rt.RouteAll(irc.Message{Command: "311", Params: []string{"me", "MRIRON", "u", "h", "*", "r"}}, cm)
	requireDests(t, got, "c1")
	got = rt.RouteAll(irc.Message{Command: "318", Params: []string{"me", "MrIron", "End"}}, cm)
	requireDests(t, got, "c1")

	ready := rt.TakeReady()
	if len(ready) != 1 {
		t.Fatalf("held write not released: %+v", ready)
	}
	if p := ready[0].Params; len(p) != 2 || p[0] != "mrIRON" || p[1] != "mrIRON" {
		t.Fatalf("released WHOIS params=%v want [mrIRON mrIRON]", p)
	}
	got = rt.RouteAll(irc.Message{Command: "318", Params: []string{"me", "mrIRON", "End"}}, cm)
	requireDests(t, got, "c2")
}

// TestWHOISClientCaseReachesServerWire drives a real downlink WHOIS with a
// mixed-case nick through Session and the keeper/brain wire to a fake ircd
// that insists on receiving the nick exactly as typed and echoes it in
// 318 — the user-visible symptom was "End of /WHOIS" naming a lowercased
// nick.
func TestWHOISClientCaseReachesServerWire(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "me", Username: "u", Realname: "r"}
	s := New(netCfg, nil, nil, nil, nil)

	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runWHOISCaseServer(conn, time.Now().Add(10*time.Second), "MrIron")
	}()
	newTestUplink(t, s, netCfg, host, port)
	waitUntil(t, 5*time.Second, s.Registered)

	d := &fakeDL{id: "d", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	if err := s.HandleClientMessage(d, irc.Message{Command: "WHOIS", Params: []string{"MrIron"}}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fake server never finished the WHOIS exchange")
	}
	waitUntil(t, 5*time.Second, func() bool { return countCmd(d, "318") >= 1 })
	for _, m := range d.snapshot() {
		if m.Command == "318" && m.Param(1) != "MrIron" {
			t.Fatalf("318 names %q, want the nick as queried: MrIron", m.Param(1))
		}
	}
}

// runWHOISCaseServer registers, then expects exactly one "WHOIS <want>"
// with the nick's original spelling and answers with 311/318 echoing the
// spelling it was sent, as real ircds do.
func runWHOISCaseServer(server net.Conn, deadline time.Time, want string) error {
	br := newLineBuf(server)
	read := func() (string, error) {
		_ = server.SetReadDeadline(deadline)
		return br.readLine()
	}
	write := func(s string) error {
		_, err := io.WriteString(server, s+"\r\n")
		return err
	}
	for _, w := range []string{"CAP LS", "NICK", "USER"} {
		line, err := read()
		if err != nil || !strings.Contains(line, w) {
			return fmt.Errorf("%s: %q %v", w, line, err)
		}
	}
	if err := write("CAP * LS :"); err != nil {
		return err
	}
	if line, err := read(); err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	for _, l := range []string{":server 001 me :Welcome", ":server 376 me :End of /MOTD command."} {
		if err := write(l); err != nil {
			return err
		}
	}
	for {
		line, err := read()
		if err != nil {
			return fmt.Errorf("reading WHOIS: %w", err)
		}
		if !strings.HasPrefix(line, "WHOIS ") {
			continue
		}
		nick := strings.TrimPrefix(strings.TrimSpace(strings.TrimPrefix(line, "WHOIS ")), ":")
		if nick != want {
			return fmt.Errorf("uplink got WHOIS %q, want the client's spelling %q", nick, want)
		}
		if err := write(fmt.Sprintf(":server 311 me %s user host * :Real Name", nick)); err != nil {
			return err
		}
		return write(fmt.Sprintf(":server 318 me %s :End of /WHOIS list.", nick))
	}
}
