package brain

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// nickCollisionThenRecoveryServer accepts one connection, forces the
// client through a real nick-collision ladder step (primary nick rejected
// once, so registration completes on the alt), then drives a real
// ISON-based recovery exchange: the primary nick reads as still online on
// the first ISON, then free on the second — proving recovery doesn't
// reclaim early — and confirms the reclaim itself by echoing a real NICK
// change line back, the same way a real ircd would.
func nickCollisionThenRecoveryServer(t *testing.T, ln net.Listener, isonSeen chan<- string, nickSeen chan<- string) {
	t.Helper()
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	send := func(line string) { _, _ = conn.Write([]byte(line + "\r\n")) }
	buf := make([]byte, 4096)
	var pending string
	readLine := func() (string, bool) {
		for {
			if i := indexCRLF(pending); i >= 0 {
				line := pending[:i]
				pending = pending[i+2:]
				return line, true
			}
			n, err := conn.Read(buf)
			if err != nil {
				return "", false
			}
			pending += string(buf[:n])
		}
	}

	nick := ""
	gotUser := false
	firstNickTried := false
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case hasPrefix(line, "CAP LS"):
			send(":fake.example CAP * LS :")
		case hasPrefix(line, "NICK "):
			candidate := line[len("NICK "):]
			if !firstNickTried {
				firstNickTried = true
				send(":fake.example 433 * " + candidate + " :Nickname is already in use.")
			} else {
				nick = candidate
			}
		case hasPrefix(line, "USER "):
			gotUser = true
		}
		if nick != "" && gotUser {
			break
		}
	}
	send(":fake.example 001 " + nick + " :Welcome")
	send(":fake.example 002 " + nick + " :Your host is fake.example")
	send(":fake.example 003 " + nick + " :This server was created today")
	send(":fake.example 004 " + nick + " fake.example test-1.0 a a")
	send(":fake.example 005 " + nick + " NICKLEN=30 :are supported by this server")
	send(":fake.example 376 " + nick + " :End of MOTD")

	freedOnNextISON := false
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case hasPrefix(line, "ISON "):
			isonSeen <- line
			if !freedOnNextISON {
				send(":fake.example 303 " + nick + " :wanted")
				freedOnNextISON = true
			} else {
				send(":fake.example 303 " + nick + " :")
			}
		case hasPrefix(line, "NICK "):
			newNick := line[len("NICK "):]
			nickSeen <- newNick
			send(":" + nick + "!u@h NICK :" + newNick)
			nick = newNick
		}
	}
}

// TestDriverNickRecoveryReclaimsFreedNick is the live proof for the whole
// feature: registration collides on the primary nick and falls back to
// the alt (a real ladder step, not synthesized), recovery then ISONs the
// primary periodically, does NOT reclaim while it reads as online, and
// does reclaim — a real NICK write reaching the wire — the moment it
// reads as free. The server's own NICK echo is what proves
// handleSelfNickChange's tracking is correct: if Driver mistracked its
// own current nick, the second ISON's target selection would be wrong.
func TestDriverNickRecoveryReclaimsFreedNick(t *testing.T) {
	mgr := keeper.NewManager(8192, 4096, nil)
	sockPath := testSockPath(t)

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	waitForSocket(t, sockPath)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	isonSeen := make(chan string, 8)
	nickSeen := make(chan string, 8)
	go nickCollisionThenRecoveryServer(t, ln, isonSeen, nickSeen)
	host, portStr := hostPortSplit(t, ln.Addr().String())

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	// The interval needs real separation from the "confirm no early
	// reclaim" wait below — too short and tick #2 fires while that wait
	// is still running, mid-test, sending a second ISON this test hasn't
	// accounted for yet and confusing which round-trip produced which
	// result.
	driver := NewDriver(client, WithNickRecoveryInterval(600*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "wanted", AltNick: "wanted_", NickRecovery: true})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: portStr}, 0); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	select {
	case res := <-driver.Results():
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
		if res.State.Nick != "wanted_" {
			t.Fatalf("registered nick=%q, want %q (the ladder fallback)", res.State.Nick, "wanted_")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	// First ISON: primary still reads as online. No reclaim yet.
	select {
	case <-isonSeen:
	case <-time.After(3 * time.Second):
		t.Fatalf("no ISON within timeout")
	}
	select {
	case nick := <-nickSeen:
		t.Fatalf("reclaimed %q before the primary nick ever read as free", nick)
	case <-time.After(300 * time.Millisecond):
	}

	// Second ISON: primary now reads as free. Must reclaim.
	select {
	case <-isonSeen:
	case <-time.After(5 * time.Second):
		t.Fatalf("no second ISON within timeout")
	}
	select {
	case nick := <-nickSeen:
		if nick != "wanted" {
			t.Fatalf("reclaimed %q, want %q", nick, "wanted")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("primary nick was never reclaimed after reading as free")
	}

	// Recovery must have stopped: no further ISON traffic once on the
	// primary nick, spanning at least one more full tick interval so a
	// bug that kept ticking would actually have a chance to show up.
	select {
	case line := <-isonSeen:
		t.Fatalf("got another ISON (%q) after reclaiming the primary nick — recovery should have stopped", line)
	case <-time.After(900 * time.Millisecond):
	}
}

// nickCollisionThenHoldServer is the collision-then-alt handshake from
// nickCollisionThenRecoveryServer, but ISON always reports the primary
// nick as still online and every post-registration NICK is echoed. Used
// to prove a client-driven NICK (StopNickRecovery) is not undone by
// recovery reclaiming the alt the moment the uplink echo arrives.
func nickCollisionThenHoldServer(t *testing.T, ln net.Listener, isonSeen chan<- string, nickSeen chan<- string) {
	t.Helper()
	conn, err := ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	send := func(line string) { _, _ = conn.Write([]byte(line + "\r\n")) }
	buf := make([]byte, 4096)
	var pending string
	readLine := func() (string, bool) {
		for {
			if i := indexCRLF(pending); i >= 0 {
				line := pending[:i]
				pending = pending[i+2:]
				return line, true
			}
			n, err := conn.Read(buf)
			if err != nil {
				return "", false
			}
			pending += string(buf[:n])
		}
	}

	nick := ""
	gotUser := false
	firstNickTried := false
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case hasPrefix(line, "CAP LS"):
			send(":fake.example CAP * LS :")
		case hasPrefix(line, "NICK "):
			candidate := nickArg(line)
			if !firstNickTried {
				firstNickTried = true
				send(":fake.example 433 * " + candidate + " :Nickname is already in use.")
			} else {
				nick = candidate
			}
		case hasPrefix(line, "USER "):
			gotUser = true
		}
		if nick != "" && gotUser {
			break
		}
	}
	send(":fake.example 001 " + nick + " :Welcome")
	send(":fake.example 002 " + nick + " :Your host is fake.example")
	send(":fake.example 003 " + nick + " :This server was created today")
	send(":fake.example 004 " + nick + " fake.example test-1.0 a a")
	send(":fake.example 005 " + nick + " NICKLEN=30 :are supported by this server")
	send(":fake.example 376 " + nick + " :End of MOTD")

	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case hasPrefix(line, "ISON "):
			isonSeen <- line
			send(":fake.example 303 " + nick + " :wanted")
		case hasPrefix(line, "NICK "):
			newNick := nickArg(line)
			nickSeen <- newNick
			send(":" + nick + "!u@h NICK :" + newNick)
			nick = newNick
		}
	}
}

func nickArg(line string) string {
	arg := line[len("NICK "):]
	if len(arg) > 0 && arg[0] == ':' {
		return arg[1:]
	}
	return arg
}

// TestDriverClientNICKHoldsRecoveryFromReclaiming is the live proof for
// the StopNickRecovery hold: registration lands on the alt, recovery
// starts (ISON shows primary still online), a client NICK moves us to a
// third nick, and the uplink echo of that NICK must not restart recovery
// so it reclaims the alt. Without the hold, handleSelfNickChange sees
// the new nick ≠ primary and startNickRecoveryIfNeeded fires a fresh
// loop whose first tick ISONs and NICK-reclaims wanted_.
func TestDriverClientNICKHoldsRecoveryFromReclaiming(t *testing.T) {
	mgr := keeper.NewManager(8192, 4096, nil)
	sockPath := testSockPath(t)

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	waitForSocket(t, sockPath)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	isonSeen := make(chan string, 8)
	nickSeen := make(chan string, 8)
	go nickCollisionThenHoldServer(t, ln, isonSeen, nickSeen)
	host, portStr := hostPortSplit(t, ln.Addr().String())

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithNickRecoveryInterval(400*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "wanted", AltNick: "wanted_", NickRecovery: true})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: portStr}, 0); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	select {
	case res := <-driver.Results():
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
		if res.State.Nick != "wanted_" {
			t.Fatalf("registered nick=%q, want %q (the ladder fallback)", res.State.Nick, "wanted_")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	select {
	case <-isonSeen:
	case <-time.After(3 * time.Second):
		t.Fatalf("no ISON within timeout (recovery never started)")
	}

	driver.StopNickRecovery(netID)
	if err := driver.WriteRaw(netID, "NICK other"); err != nil {
		t.Fatalf("WriteRaw NICK other: %v", err)
	}
	select {
	case nick := <-nickSeen:
		if nick != "other" {
			t.Fatalf("uplink NICK=%q, want %q (the client-driven change)", nick, "other")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("client NICK never reached the uplink")
	}

	// Spanning more than one recovery interval: a restarted loop would
	// tick immediately, ISON the still-online primary, and NICK back to
	// the alt. The hold must prevent that.
	select {
	case nick := <-nickSeen:
		t.Fatalf("recovery reclaimed %q after client NICK other — StopNickRecovery should have held", nick)
	case <-time.After(900 * time.Millisecond):
	}
}

func hostPortSplit(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %q: %v", portStr, err)
	}
	return host, port
}
