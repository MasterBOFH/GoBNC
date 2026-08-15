package session

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestReplyRoutingStressLagsAndConcurrentPolls pushes
// TestWHOISRoutingThroughRealKeeperBrainWireMultiDownlink and
// TestSolicitousE2EConcurrentWHOXAndHold further, past "several different
// targets, replies mildly reordered" into the actually adversarial shapes
// those two didn't cover, all through the same real keeper<->brain wire:
//
//   - Lag: every reply line the fake server writes sits behind a random
//     5-25ms sleep, and unrelated targets' replies are woven through each
//     other rather than sent burst-by-burst — the closest a loopback TCP
//     test can get to real network jitter without a real network.
//   - Two clients WHOIS the *same* nick concurrently — RequestTracker
//     coalesces the second onto the in-flight write and fans the one
//     reply burst to both, rather than sending a duplicate WHOIS.
//   - A repeat poll: a client that already completed one WHOIS immediately
//     issues another for a new target, concurrently with a second client
//     asking for that same nick — proving a finished request's queue
//     entry is fully cleaned up, and the new pair coalesces again.
//   - WHOX on two different channels running the whole time alongside all
//     of the above, demuxed by token exactly like
//     TestSolicitousE2EConcurrentWHOXAndHold already proves in isolation,
//     now under the same shared load.
func TestReplyRoutingStressLagsAndConcurrentPolls(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "me", Username: "u", Realname: "r"}
	s := New(netCfg, nil, nil, nil, nil)

	reqSeen := make(chan string, 32)
	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runRoutingStressServer(conn, time.Now().Add(15*time.Second), reqSeen)
	}()
	newTestUplink(t, s, netCfg, host, port)
	waitUntil(t, 5*time.Second, s.Registered)

	d1 := &fakeDL{id: "d1", caps: map[string]bool{}}
	d2 := &fakeDL{id: "d2", caps: map[string]bool{}}
	d3 := &fakeDL{id: "d3", caps: map[string]bool{}}
	d4 := &fakeDL{id: "d4", caps: map[string]bool{}}
	d5 := &fakeDL{id: "d5", caps: map[string]bool{}}
	for _, d := range []*fakeDL{d1, d2, d3, d4, d5} {
		if err := s.Attach(d); err != nil {
			t.Fatal(err)
		}
		d.clearSent()
	}

	// Round 1: five concurrent requests. d1 and d3 both WHOIS alice — the
	// second is coalesced onto the first uplink write.
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d1, irc.Message{Command: "WHOIS", Params: []string{"alice"}})
	}()
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d2, irc.Message{Command: "WHOIS", Params: []string{"bob"}})
	}()
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d3, irc.Message{Command: "WHOIS", Params: []string{"alice"}})
	}()
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d4, irc.Message{Command: "WHO", Params: []string{"#chan1", "%tuhn,10"}})
	}()
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d5, irc.Message{Command: "WHO", Params: []string{"#chan2", "%tuhn,20"}})
	}()
	wg.Wait()

	waitUntil(t, 5*time.Second, func() bool {
		return countCmd(d1, "318") >= 1 && countCmd(d2, "318") >= 1 && countCmd(d3, "318") >= 1 &&
			countCmd(d4, "315") >= 1 && countCmd(d5, "315") >= 1
	})

	assertOnlyWhoisFor(t, d1, "alice")
	assertOnlyWhoisFor(t, d2, "bob")
	assertOnlyWhoisFor(t, d3, "alice")
	if countCmd(d1, "311") != 1 || countCmd(d3, "311") != 1 {
		t.Fatalf("same-target WHOIS burst leaked across clients: d1=%d d3=%d 311s",
			countCmd(d1, "311"), countCmd(d3, "311"))
	}
	a1 := firstCmd(d1, "311")
	a3 := firstCmd(d3, "311")
	if a1 == nil || a3 == nil {
		t.Fatalf("missing alice 311: d1=%v d3=%v", d1.snapshot(), d3.snapshot())
	}
	if !strings.Contains(a1.Trailing(), "Queued First") {
		t.Fatalf("d1 got wrong burst: %+v", a1.Params)
	}
	if !strings.Contains(a3.Trailing(), "Queued First") {
		t.Fatalf("d3 must share d1's coalesced burst: %+v", a3.Params)
	}

	m4 := firstCmd(d4, "354")
	m5 := firstCmd(d5, "354")
	if m4 == nil || m5 == nil {
		t.Fatalf("missing WHOX 354: d4=%v d5=%v", d4.snapshot(), d5.snapshot())
	}
	if m4.Param(1) != "10" || m4.Param(2) != "#chan1" {
		t.Fatalf("d4 WHOX crossed: %+v", m4.Params)
	}
	if m5.Param(1) != "20" || m5.Param(2) != "#chan2" {
		t.Fatalf("d5 WHOX crossed: %+v", m5.Params)
	}
	if countCmd(d4, "354") != 1 || countCmd(d5, "354") != 1 {
		t.Fatalf("WHOX demux duplicated: d4=%d d5=%d", countCmd(d4, "354"), countCmd(d5, "354"))
	}

	d1.clearSent()
	d4.clearSent()

	// Round 2: repeat polls from two clients that already finished a
	// request in round 1 (one WHOIS, one WHO) — same same-nick-queue
	// ordering discipline as round 1, now proving round 1's queue entries
	// didn't leave anything behind for this round to trip over.
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d1, irc.Message{Command: "WHOIS", Params: []string{"erin"}})
	}()
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(d4, irc.Message{Command: "WHOIS", Params: []string{"erin"}})
	}()
	wg.Wait()

	waitUntil(t, 5*time.Second, func() bool {
		return countCmd(d1, "318") >= 1 && countCmd(d4, "318") >= 1
	})
	assertOnlyWhoisFor(t, d1, "erin")
	assertOnlyWhoisFor(t, d4, "erin")
	if countCmd(d1, "311") != 1 || countCmd(d4, "311") != 1 {
		t.Fatalf("round-2 same-target WHOIS burst leaked: d1=%d d4=%d 311s", countCmd(d1, "311"), countCmd(d4, "311"))
	}
	e1 := firstCmd(d1, "311")
	e4 := firstCmd(d4, "311")
	if e1 == nil || e4 == nil {
		t.Fatalf("missing erin 311: d1=%v d4=%v", d1.snapshot(), d4.snapshot())
	}
	if !strings.Contains(e1.Trailing(), "Erin First") {
		t.Fatalf("round 2: d1 got wrong burst: %+v", e1.Params)
	}
	if !strings.Contains(e4.Trailing(), "Erin First") {
		t.Fatalf("round 2: d4 must share d1's coalesced burst: %+v", e4.Params)
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
	}
}

// waitForReq drains reqSeen until it sees a "WHOIS <nick>" request line —
// tolerating both wire forms (irc.Message.Encode always colons the last
// param, so a freshly-built "WHOIS alice" is sent as "WHOIS :alice"; this
// doesn't care which). Used to make same-target queue ordering a checked
// fact instead of a race: the caller only releases a second same-nick
// request once the server has genuinely already seen the first one.
func waitForReq(t *testing.T, reqSeen <-chan string, nick string) {
	t.Helper()
	want1 := "WHOIS " + nick
	want2 := "WHOIS :" + nick
	for {
		select {
		case line := <-reqSeen:
			if line == want1 || line == want2 {
				return
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("never saw %s", want1)
		}
	}
}

// lag sleeps a random short duration before every reply line the stress
// server writes, standing in for real network jitter on a loopback
// connection that would otherwise deliver everything instantly and in
// exactly the order it was written.
func lag() { time.Sleep(time.Duration(5+rand.Intn(20)) * time.Millisecond) }

func runRoutingStressServer(server net.Conn, deadline time.Time, reqSeen chan<- string) error {
	br := newLineBuf(server)
	read := func() (string, error) {
		_ = server.SetReadDeadline(deadline)
		return br.readLine()
	}
	write := func(s string) error {
		lag()
		_, err := io.WriteString(server, s+"\r\n")
		return err
	}
	for _, want := range []string{"CAP LS", "NICK", "USER"} {
		line, err := read()
		if err != nil || !strings.Contains(line, want) {
			return fmt.Errorf("%s: %q %v", want, line, err)
		}
	}
	if err := write("CAP * LS :"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	for _, l := range []string{
		// WHOX in ISUPPORT is what makes forwardSolicitous treat WHO as
		// independently-tokened rather than serialized one-at-a-time (see
		// preferWHOX in downstream.go) — without it, the two concurrent
		// WHO requests below would hold-queue against each other exactly
		// like the LIST pair in TestSolicitousE2EConcurrentWHOXAndHold,
		// deadlocking against this script's own "collect everything
		// before replying to anything" ordering.
		":server 005 me CHANMODES=b,k,l,imnpst PREFIX=(ov)@+ CASEMAPPING=rfc1459 WHOX :are supported by this server",
		":server 001 me :Welcome",
		":server 376 me :End of /MOTD command.",
	} {
		if err := write(l); err != nil {
			return err
		}
	}

	readReq := func() (string, error) {
		_ = server.SetReadDeadline(deadline)
		line, err := br.readLine()
		if err != nil {
			return "", err
		}
		select {
		case reqSeen <- line:
		default:
		}
		return line, nil
	}

	// Round 1: collect the four on-wire requests. A second WHOIS alice is
	// coalesced by the tracker and never reaches the uplink.
	var aliceCount, bobSeen int
	var who1Tok, who2Tok string
	for aliceCount < 1 || bobSeen == 0 || who1Tok == "" || who2Tok == "" {
		line, err := readReq()
		if err != nil {
			return fmt.Errorf("round 1 read: %w", err)
		}
		switch {
		case line == "WHOIS :alice" || line == "WHOIS alice":
			aliceCount++
		case line == "WHOIS :bob" || line == "WHOIS bob":
			bobSeen++
		case strings.HasPrefix(line, "WHO #chan1"):
			who1Tok = whoToken(line)
		case strings.HasPrefix(line, "WHO #chan2"):
			who2Tok = whoToken(line)
		}
	}

	whoisBurst := func(nick, realname string) []string {
		return []string{
			":server 311 me " + nick + " user host * :" + realname,
			":server 312 me " + nick + " irc.example :Some Server",
			":server 318 me " + nick + " :End of /WHOIS list.",
		}
	}
	aliceBurst := whoisBurst("alice", "Queued First")
	bobBurst := whoisBurst("bob", "Real Name")

	schedule := []string{
		bobBurst[0],
		":server 354 me " + who1Tok + " #chan1 n1",
		aliceBurst[0],
		":server 354 me " + who2Tok + " #chan2 n2",
		bobBurst[1],
		aliceBurst[1],
		":server 315 me #chan1 :End",
		aliceBurst[2],
		bobBurst[2],
		":server 315 me #chan2 :End",
	}
	for _, l := range schedule {
		if err := write(l); err != nil {
			return err
		}
	}

	// Round 2: one on-wire WHOIS erin; the second client coalesces.
	var erinCount int
	for erinCount < 1 {
		line, err := readReq()
		if err != nil {
			return fmt.Errorf("round 2 read: %w", err)
		}
		if line == "WHOIS :erin" || line == "WHOIS erin" {
			erinCount++
		}
	}
	for _, l := range whoisBurst("erin", "Erin First") {
		if err := write(l); err != nil {
			return err
		}
	}

	return nil
}
