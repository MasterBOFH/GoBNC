package downlink

import (
	"bufio"
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/session"
)

// TestSendNeverLogsDebugSourceTraffic guards against the exact feedback
// loop found live: a /bnc debug raw/all subscriber receiving a relay
// message would, without this exclusion, see that very message logged as
// ordinary outgoing traffic, picked back up by the same subscription, and
// relayed again — forever, and exponentially (each relay embeds the
// previous one). See session.DebugSource's doc comment for the full story.
//
// Send only enqueues (see Client.out's doc comment) — writeLoop does the
// actual write, and logs immediately before it, in that goroutine. Each
// assertion below waits for its own message to actually cross the pipe
// before checking the log, so it's synchronized with, not racing, that
// async write.
func TestSendNeverLogsDebugSourceTraffic(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	r := bufio.NewReader(b)

	var logBuf bytes.Buffer
	logger, _, err := gobnclog.Setup(gobnclog.Options{Level: "debug", Console: &logBuf})
	if err != nil {
		t.Fatal(err)
	}

	cl := &Client{id: "c1", conn: a, caps: make(map[string]bool), log: logger}

	if err := cl.Send(irc.Message{Source: session.DebugSource, Command: "PRIVMSG", Params: []string{"me", "debug relay text"}}); err != nil {
		t.Fatal(err)
	}
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logBuf.String(), "debug relay text") {
		t.Fatalf("debug-sourced message was logged as outgoing traffic (feedback-loop regression): %s", logBuf.String())
	}

	if err := cl.Send(irc.Message{Source: "gobnc", Command: "NOTICE", Params: []string{"me", "ordinary text"}}); err != nil {
		t.Fatal(err)
	}
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logBuf.String(), "ordinary text") {
		t.Fatalf("ordinary outgoing traffic was not logged: %s", logBuf.String())
	}
}
