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

// Uplink-side draft/resume-0.5 through Driver (distinct from resume_test.go,
// which is about a *brain* resuming against a keeper-held socket — here the
// socket itself is what dies, and the bouncer resumes its *server-side*
// session on the redial).
//
// TestDriverResumesUplinkSessionOnRedial drives two real connections
// through one Driver against a scripted ircd:
//
//	conn 1: cap offered+ACK'd; Driver presents the token it was
//	        configured with (NetworkConfig.ResumeToken) → server rejects
//	        it (FAIL RESUME) → registration proceeds fresh → server issues
//	        "tok1" → auto-join happens → server hangs up.
//	conn 2: the automatic redial presents tok1 (learned live on conn 1,
//	        via ActionResumeToken — the config was never updated by the
//	        caller) → RESUME SUCCESS → registration completes with
//	        Resumed=true → NO auto-join.
func TestDriverResumesUplinkSessionOnRedial(t *testing.T) {
	client, _ := newAttachedLiveClientWithManager(t)

	srv := newFakeIRCServer(t)
	defer srv.close()
	conn1 := make(chan string, 64)
	conn2 := make(chan string, 64)
	go func() {
		srv.serveResume(t, 1, conn1)
		srv.serveResume(t, 2, conn2)
	}()
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithBackoff(20*time.Millisecond, 20*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", ResumeToken: "cfgtok"})
	driver.SetChannels(netID, []ChannelJoin{{Name: "#chan"}})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if dr := awaitDialResult(t, driver, 5*time.Second); !dr.OK || dr.Epoch != 1 {
		t.Fatalf("first DialResult=%+v", dr)
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	res1 := awaitResult(t, driver, 10*time.Second)
	if res1.State.Phase != registration.PhaseComplete || res1.State.Resumed {
		t.Fatalf("conn 1: phase=%v resumed=%v (err=%v), want complete/fresh", res1.State.Phase, res1.State.Resumed, res1.State.Err)
	}
	got1, _ := drainUntil(conn1, "376-sent", 5*time.Second)
	if !contains(got1, "RESUME cfgtok") {
		t.Fatalf("conn 1 never saw the configured token presented: %v", got1)
	}
	// Auto-join is a reaction to registration completing, so it follows
	// the burst rather than preceding it.
	if _, sawJoin := drainUntil(conn1, "JOIN #chan", 5*time.Second); !sawJoin {
		t.Fatalf("conn 1 (fresh registration) did not auto-join")
	}

	// The server hung up after conn 1; the auto-redial must present tok1.
	if dr := awaitDialResult(t, driver, 5*time.Second); !dr.OK || dr.Epoch != 2 {
		t.Fatalf("redial DialResult=%+v", dr)
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration (redial): %v", err)
	}
	res2 := awaitResult(t, driver, 10*time.Second)
	if res2.State.Phase != registration.PhaseComplete || !res2.State.Resumed {
		t.Fatalf("conn 2: phase=%v resumed=%v (err=%v), want complete/resumed", res2.State.Phase, res2.State.Resumed, res2.State.Err)
	}
	if res2.State.Nick != "gobncbrain" {
		t.Fatalf("conn 2: nick=%q, want the old session's nick from RESUME SUCCESS", res2.State.Nick)
	}
	got2, _ := drainUntil(conn2, "376-sent", 5*time.Second)
	if !contains(got2, "RESUME tok1") {
		t.Fatalf("redial did not present the token learned on conn 1: %v", got2)
	}
	// Anything the driver sends right after registering on a resumed
	// session arrives within this window; a JOIN here is the bug.
	time.Sleep(300 * time.Millisecond)
	for {
		select {
		case line := <-conn2:
			if strings.HasPrefix(line, "JOIN ") {
				t.Fatalf("auto-joined on a resumed session: %q", line)
			}
			continue
		default:
		}
		break
	}
	if cfg := driver.configFor(netID); cfg.ResumeToken != "tok2" {
		t.Fatalf("config token=%q after conn 2, want tok2 (the one conn 2 was issued)", cfg.ResumeToken)
	}
}

func (d *Driver) configFor(id keeper.NetworkID) NetworkConfig {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.configs[id]
}

func awaitResult(t *testing.T, driver *Driver, timeout time.Duration) Result {
	t.Helper()
	select {
	case res := <-driver.Results():
		return res
	case <-time.After(timeout):
		t.Fatalf("no registration Result within %s", timeout)
		return Result{}
	}
}

// drainUntil collects lines from ch until marker (a client line, or one
// the fake server injects into its own capture channel) or timeout,
// reporting whether the marker was actually reached.
func drainUntil(ch <-chan string, marker string, timeout time.Duration) ([]string, bool) {
	var out []string
	deadline := time.After(timeout)
	for {
		select {
		case l := <-ch:
			if l == marker {
				return out, true
			}
			out = append(out, l)
		case <-deadline:
			return out, false
		}
	}
}

func contains(lines []string, want string) bool {
	for _, l := range lines {
		if l == want {
			return true
		}
	}
	return false
}

// serveResume is one scripted draft/resume-0.5 connection (see the test's
// doc comment for the two scripts). Every client line is copied to out;
// "376-sent" is pushed to out once the welcome burst is complete. attempt
// 1 hangs up after the burst; attempt 2 stays open until the test ends.
func (s *fakeIRCServer) serveResume(t *testing.T, attempt int, out chan<- string) {
	t.Helper()
	conn, err := s.ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	send := func(line string) { _, _ = conn.Write([]byte(line + "\r\n")) }
	buf := make([]byte, 4096)
	var pending string
	readLine := func() (string, bool) {
		for {
			if i := indexCRLF(pending); i >= 0 {
				line := pending[:i]
				pending = pending[i+2:]
				out <- line
				return line, true
			}
			n, err := conn.Read(buf)
			if err != nil {
				return "", false
			}
			pending += string(buf[:n])
		}
	}
	burst := func(nick string) {
		send(":fake.example 001 " + nick + " :Welcome")
		send(":fake.example 002 " + nick + " :Your host is fake.example")
		send(":fake.example 003 " + nick + " :This server was created today")
		send(":fake.example 004 " + nick + " fake.example test-1.0 a a")
		send(":fake.example 005 " + nick + " NICKLEN=30 :are supported by this server")
		send(":fake.example 376 " + nick + " :End of MOTD")
		out <- "376-sent"
	}

	nick := "nick"
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case hasPrefix(line, "CAP LS"):
			send(":fake.example CAP * LS :message-tags " + registration.ResumeCap)
		case hasPrefix(line, "NICK "):
			nick = line[len("NICK "):]
			if attempt == 2 {
				// The old session still holds it — exactly what a real
				// resume attempt runs into.
				send(":fake.example 433 * " + nick + " :Nickname is already in use")
			}
		case hasPrefix(line, "CAP REQ"):
			send(":fake.example CAP * ACK :message-tags " + registration.ResumeCap)
			if attempt == 1 {
				send(":fake.example RESUME TOKEN tok1")
			} else {
				send(":fake.example RESUME TOKEN tok2")
			}
		case hasPrefix(line, "RESUME "):
			if attempt == 1 {
				send(":fake.example FAIL RESUME INVALID_TOKEN :Cannot resume connection, token is not valid")
			} else {
				send(":fake.example RESUME SUCCESS gobncbrain")
				burst("gobncbrain")
			}
		case line == "CAP END":
			burst(nick)
		case hasPrefix(line, "JOIN "):
			if attempt == 1 {
				return // hang up: the auto-redial is the point
			}
		}
	}
}

var _ net.Conn = (*net.TCPConn)(nil)
