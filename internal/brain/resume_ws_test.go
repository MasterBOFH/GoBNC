package brain

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
	"github.com/MasterBOFH/GoBNC/internal/wsconn"
	"github.com/coder/websocket"
)

// wsResumeIRCd is serveResume's WebSocket twin: a real IRCv3-WebSocket ircd
// (coder Accept, one message = one line) running the same two-connection
// draft/resume-0.5 script. It proves the WebSocket transport and the resume
// machinery compose — the actual combination needed for an ircu2 that only
// offers resume to WebSocket clients.
type wsResumeIRCd struct {
	ln       net.Listener
	mu       sync.Mutex
	nattempt int
	conn1    chan string
	conn2    chan string
}

func newWSResumeIRCd(t *testing.T) *wsResumeIRCd {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &wsResumeIRCd{ln: ln, conn1: make(chan string, 64), conn2: make(chan string, 64)}
	hs := &http.Server{Handler: http.HandlerFunc(s.handle)}
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })
	return s
}

func (s *wsResumeIRCd) addr() (string, int) {
	h, p, _ := net.SplitHostPort(s.ln.Addr().String())
	var port int
	_, _ = fmtSscanf(p, &port)
	return h, port
}

func (s *wsResumeIRCd) handle(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: wsconn.Subprotocols})
	if err != nil {
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	ctx := r.Context()

	s.mu.Lock()
	s.nattempt++
	attempt := s.nattempt
	s.mu.Unlock()
	out := s.conn1
	if attempt == 2 {
		out = s.conn2
	}

	send := func(line string) error { return c.Write(ctx, websocket.MessageBinary, []byte(line)) }
	readLine := func() (string, bool) {
		_, data, err := c.Read(ctx)
		if err != nil {
			return "", false
		}
		out <- string(data)
		return string(data), true
	}
	burst := func(nick string) {
		for _, l := range []string{
			":fake.example 001 " + nick + " :Welcome",
			":fake.example 002 " + nick + " :Your host is fake.example",
			":fake.example 003 " + nick + " :This server was created today",
			":fake.example 004 " + nick + " fake.example test-1.0 a a",
			":fake.example 005 " + nick + " NICKLEN=30 :are supported by this server",
			":fake.example 376 " + nick + " :End of MOTD",
		} {
			_ = send(l)
		}
		out <- "376-sent"
	}

	nick := "nick"
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case strings.HasPrefix(line, "CAP LS"):
			_ = send(":fake.example CAP * LS :message-tags " + registration.ResumeCap)
		case strings.HasPrefix(line, "NICK "):
			nick = line[len("NICK "):]
			if attempt == 2 {
				_ = send(":fake.example 433 * " + nick + " :Nickname is already in use")
			}
		case strings.HasPrefix(line, "CAP REQ"):
			_ = send(":fake.example CAP * ACK :message-tags " + registration.ResumeCap)
			if attempt == 1 {
				_ = send(":fake.example RESUME TOKEN tok1")
			} else {
				_ = send(":fake.example RESUME TOKEN tok2")
			}
		case strings.HasPrefix(line, "RESUME "):
			if attempt == 1 {
				_ = send(":fake.example FAIL RESUME INVALID_TOKEN :Cannot resume connection, token is not valid")
			} else {
				_ = send(":fake.example RESUME SUCCESS gobncbrain")
				burst("gobncbrain")
			}
		case line == "CAP END":
			burst(nick)
		case strings.HasPrefix(line, "JOIN "):
			if attempt == 1 {
				return // hang up so the auto-redial resumes
			}
		}
	}
}

func fmtSscanf(p string, out *int) (int, error) {
	n := 0
	for _, r := range p {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	*out = n
	return 1, nil
}

// TestDriverResumesUplinkSessionOverWebSocket is
// TestDriverResumesUplinkSessionOnRedial run over a WebSocket transport:
// the keeper dials with DialConfig.WebSocket, so the whole resume exchange
// (token issue, present-on-redial, RESUME SUCCESS, no auto-join) rides
// IRCv3 WebSocket framing rather than a raw stream.
func TestDriverResumesUplinkSessionOverWebSocket(t *testing.T) {
	client, _ := newAttachedLiveClientWithManager(t)
	srv := newWSResumeIRCd(t)
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithBackoff(20*time.Millisecond, 20*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", ResumeToken: "cfgtok"})
	driver.SetChannels(netID, []ChannelJoin{{Name: "#chan"}})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port, WebSocket: true}, 0); err != nil {
		t.Fatalf("Dial (websocket): %v", err)
	}
	if dr := awaitDialResult(t, driver, 5*time.Second); !dr.OK || dr.Epoch != 1 {
		t.Fatalf("first DialResult=%+v", dr)
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	res1 := awaitResult(t, driver, 10*time.Second)
	if res1.State.Phase != registration.PhaseComplete || res1.State.Resumed {
		t.Fatalf("conn 1 over WS: phase=%v resumed=%v (err=%v), want complete/fresh", res1.State.Phase, res1.State.Resumed, res1.State.Err)
	}
	got1, _ := drainUntil(srv.conn1, "376-sent", 5*time.Second)
	if !contains(got1, "RESUME cfgtok") {
		t.Fatalf("conn 1 (WS) never presented the configured token: %v", got1)
	}
	if _, sawJoin := drainUntil(srv.conn1, "JOIN #chan", 5*time.Second); !sawJoin {
		t.Fatal("conn 1 (WS, fresh registration) did not auto-join")
	}

	// Server hung up; the auto-redial must resume over a fresh WebSocket.
	if dr := awaitDialResult(t, driver, 5*time.Second); !dr.OK || dr.Epoch != 2 {
		t.Fatalf("redial DialResult=%+v", dr)
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration (redial): %v", err)
	}
	res2 := awaitResult(t, driver, 10*time.Second)
	if res2.State.Phase != registration.PhaseComplete || !res2.State.Resumed {
		t.Fatalf("conn 2 over WS: phase=%v resumed=%v (err=%v), want complete/resumed", res2.State.Phase, res2.State.Resumed, res2.State.Err)
	}
	if res2.State.Nick != "gobncbrain" {
		t.Fatalf("conn 2 (WS): nick=%q, want the resumed session's nick", res2.State.Nick)
	}
	got2, _ := drainUntil(srv.conn2, "376-sent", 5*time.Second)
	if !contains(got2, "RESUME tok1") {
		t.Fatalf("WS redial did not present the token learned on conn 1: %v", got2)
	}
	time.Sleep(300 * time.Millisecond)
	for {
		select {
		case line := <-srv.conn2:
			if strings.HasPrefix(line, "JOIN ") {
				t.Fatalf("auto-joined on a resumed WS session: %q", line)
			}
			continue
		default:
		}
		break
	}
}
