package brain

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// quietThenReadServer registers one client, then records every subsequent
// line it sends (used to observe keepalive PING).
func quietThenReadServer(t *testing.T, ln net.Listener, lines chan<- string) {
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
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case hasPrefix(line, "CAP LS"):
			send(":fake.example CAP * LS :")
		case hasPrefix(line, "NICK "):
			nick = line[len("NICK "):]
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
		select {
		case lines <- line:
		default:
		}
		switch {
		case hasPrefix(line, "PING "):
			token := strings.TrimPrefix(line, "PING ")
			token = strings.TrimPrefix(token, ":")
			send("PONG :" + token)
		case hasPrefix(line, "ISON "):
			targets := strings.TrimPrefix(line, "ISON ")
			send(":fake.example 303 " + nick + " :" + targets)
		}
	}
}

func TestDriverKeepaliveSendsPINGWhenIdle(t *testing.T) {
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
	got := make(chan string, 32)
	go quietThenReadServer(t, ln, got)
	host, port := hostPortSplit(t, ln.Addr().String())

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithKeepalive(80*time.Millisecond, time.Second))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "me", NickRecovery: false})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
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
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case line := <-got:
			if strings.HasPrefix(line, "PING ") {
				if line != "PING :gobnc" {
					t.Fatalf("keepalive PING = %q, want PING :gobnc", line)
				}
				return
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("no keepalive PING :gobnc within timeout")
}

// silentAfterRegServer registers the client, then goes mute: it keeps the
// socket open and records every subsequent line but never answers the
// keepalive PING, so the idle+grace budget is guaranteed to expire.
func silentAfterRegServer(t *testing.T, ln net.Listener, lines chan<- string) {
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
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case hasPrefix(line, "CAP LS"):
			send(":fake.example CAP * LS :")
		case hasPrefix(line, "NICK "):
			nick = line[len("NICK "):]
		case hasPrefix(line, "USER "):
			gotUser = true
		}
		if nick != "" && gotUser {
			break
		}
	}
	send(":fake.example 001 " + nick + " :Welcome")
	send(":fake.example 376 " + nick + " :End of MOTD")

	for {
		line, ok := readLine()
		if !ok {
			return
		}
		select {
		case lines <- line:
		default:
		}
	}
}

// TestDriverKeepaliveTimeoutPublishesDisconnected is the missing-signal
// bug: the keepalive timeout closes the uplink with a deliberate
// CloseRequest, and the keeper publishes no EventDisconnected for a
// deliberate close — so nothing ever reached internal/server's demux, and
// Session.HandleDisconnect (which ERRORs and closes attached downlinks)
// never ran. The Driver must synthesize the event itself.
func TestDriverKeepaliveTimeoutPublishesDisconnected(t *testing.T) {
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
	got := make(chan string, 32)
	go silentAfterRegServer(t, ln, got)
	host, port := hostPortSplit(t, ln.Addr().String())

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithKeepalive(80*time.Millisecond, 100*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "me", NickRecovery: false})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
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
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	// The server now stays silent and won't PONG. Expect the keepalive
	// timeout, and — the point of this test — a NetworkEvent{Disconnected}
	// on the Driver's event stream, which is what internal/server's demux
	// routes to Session.HandleDisconnect.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-driver.NetworkEvents():
			if !ok {
				t.Fatal("NetworkEvents closed before Disconnected event")
			}
			if ev.Kind != keeper.EventDisconnected {
				continue
			}
			if ev.Network != netID {
				t.Fatalf("Disconnected event for network %d, want %d", ev.Network, netID)
			}
			if !strings.Contains(ev.Error, "keepalive") {
				t.Fatalf("Disconnected event Error=%q, want it to name the keepalive timeout", ev.Error)
			}
			return
		case <-deadline:
			t.Fatal("keepalive timeout produced no NetworkEvent{Disconnected} — Session.HandleDisconnect would never run and downlinks would stay attached")
		}
	}
}

// isonReclaimKeepsOnlineServer is the nick-collision + ISON-still-taken
// fixture: registration completes on the alt, every ISON reports the
// primary still online (so reclaim never fires), and inbound PINGs are
// answered so a missing PONG is distinguishable from a missing PING.
func isonReclaimKeepsOnlineServer(t *testing.T, ln net.Listener, seen chan<- string) {
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

	for {
		line, ok := readLine()
		if !ok {
			return
		}
		select {
		case seen <- line:
		default:
		}
		switch {
		case hasPrefix(line, "ISON "):
			send(":fake.example 303 " + nick + " :wanted")
		case hasPrefix(line, "PING "):
			token := strings.TrimPrefix(line, "PING ")
			token = strings.TrimPrefix(token, ":")
			send("PONG :" + token)
		}
	}
}

// TestDriverKeepalivePINGsDuringISONReclaim is the reported failure:
// off the primary nick, recovery ISONs every tick, and those 303 replies
// used to count as RX liveness — so the keepalive PING never fired, the
// debug log went quiet of PINGs, and the ircd dropped the socket. The
// 303 must not suppress the PING.
func TestDriverKeepalivePINGsDuringISONReclaim(t *testing.T) {
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
	seen := make(chan string, 64)
	go isonReclaimKeepsOnlineServer(t, ln, seen)
	host, port := hostPortSplit(t, ln.Addr().String())

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client,
		WithNickRecoveryInterval(40*time.Millisecond),
		WithKeepalive(80*time.Millisecond, time.Second),
	)
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "wanted", AltNick: "wanted_", NickRecovery: true})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
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
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	var gotISON, gotPING bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !(gotISON && gotPING) {
		select {
		case line := <-seen:
			if strings.HasPrefix(line, "ISON ") {
				gotISON = true
			}
			if line == "PING :gobnc" {
				gotPING = true
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !gotISON {
		t.Fatal("recovery never sent ISON")
	}
	if !gotPING {
		t.Fatal("keepalive PING was suppressed by ISON 303 replies — reclaim must not count as uplink liveness")
	}
}

// TestDriverKeepalivePINGsAfterNickConfigChange is the network-mod shape
// of the same bug: already on a nick, primary is retargeted at a taken
// name, recovery starts ISONing, keepalive must still PING.
func TestDriverKeepalivePINGsAfterNickConfigChange(t *testing.T) {
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
	seen := make(chan string, 64)
	go quietThenReadServer(t, ln, seen)
	host, port := hostPortSplit(t, ln.Addr().String())

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client,
		WithNickRecoveryInterval(40*time.Millisecond),
		WithKeepalive(80*time.Millisecond, time.Second),
	)
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "me", NickRecovery: true})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
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
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	// Drain registration-window traffic, then retarget the primary nick
	// at a name we don't hold — recovery starts, same as network mod
	// --nick=taken.
	drainUntil := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(drainUntil) {
		select {
		case <-seen:
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	driver.UpdateNetworkConfig(netID, NetworkConfig{PrimaryNick: "taken", NickRecovery: true})

	var gotISON, gotPING bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !(gotISON && gotPING) {
		select {
		case line := <-seen:
			if strings.HasPrefix(line, "ISON ") {
				gotISON = true
			}
			if line == "PING :gobnc" {
				gotPING = true
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !gotISON {
		t.Fatal("recovery never sent ISON after nick config change")
	}
	if !gotPING {
		t.Fatal("keepalive PING stopped after network mod --nick=taken")
	}
}
