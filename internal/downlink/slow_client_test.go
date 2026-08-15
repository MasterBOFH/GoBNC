package downlink

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// TestSlowClientSendNeverBlocks proves the regression this fix closes: a
// downlink client whose read side never drains must never make Send block
// its caller (the shared demux, in production, servicing every network) —
// see Client.out/writeLoop's own doc comments.
func TestSlowClientSendNeverBlocks(t *testing.T) {
	old := DownlinkOutQueueSize
	DownlinkOutQueueSize = 8
	t.Cleanup(func() { DownlinkOutQueueSize = old })

	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	// b is deliberately never read from — simulates a stuck/slow client.

	cl := &Client{id: "c1", conn: a, caps: map[string]bool{}, log: slog.Default()}

	done := make(chan error, 1)
	go func() {
		var lastErr error
		for i := 0; i < DownlinkOutQueueSize*4; i++ {
			lastErr = cl.Send(irc.Message{Command: "PRIVMSG", Params: []string{"#weechat", "hi"}})
		}
		done <- lastErr
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an overflow error once the stuck client's queue filled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on a stuck client instead of returning promptly")
	}

	cl.wmu.Lock()
	closed := cl.closed
	cl.wmu.Unlock()
	if !closed {
		t.Fatal("expected the stuck client to be disconnected after its queue overflowed")
	}
}

// TestClientSendPreservesOrder confirms the per-client outbound queue
// doesn't reorder messages for a client whose read side keeps up.
func TestClientSendPreservesOrder(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	cl := &Client{id: "c1", conn: a, caps: map[string]bool{}, log: slog.Default()}

	const n = 50
	go func() {
		for i := 0; i < n; i++ {
			_ = cl.Send(irc.Message{Command: "PRIVMSG", Params: []string{"#chan", fmt.Sprintf("msg%d", i)}})
		}
	}()

	r := bufio.NewReader(b)
	for i := 0; i < n; i++ {
		_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		want := fmt.Sprintf("msg%d", i)
		if !strings.Contains(line, want) {
			t.Fatalf("out of order: got %q, want containing %q", line, want)
		}
	}
}
