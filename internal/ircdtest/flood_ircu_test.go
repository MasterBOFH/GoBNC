//go:build ircd

package ircd_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/uplink"
)

// ircu2 default recvq is 1024 bytes. Without pacing, dumping >1KiB quickly
// typically gets Excess Flood / kill. With burst under recvq, we should survive.
func TestIrcu2FloodRecvQ(t *testing.T) {
	if os.Getenv("GOBNC_IRCD") == "0" {
		t.Skip("GOBNC_IRCD=0")
	}
	const addr = "127.0.0.1:4443"
	if c, err := net.DialTimeout("tcp", addr, 3*time.Second); err != nil {
		t.Skipf("ircu2 not reachable at %s: %v", addr, err)
	} else {
		_ = c.Close()
	}

	nick := sanitizeNick(fmt.Sprintf("f%d", time.Now().Unix()%100000))
	h := &floodHandler{}
	u := uplink.New(uplink.Config{
		Network: store.Network{
			Name:       "ircu2",
			Host:       "127.0.0.1",
			Port:       4443,
			Nick:       nick,
			Username:   "gobnc",
			Realname:   "floodtest",
			TLS:        false,
			FloodBurst: 512, // under 1024 recvq
			FloodRate:  256, // bytes/sec
		},
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, a string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
	}, h)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- u.Run(ctx) }()

	deadline := time.Now().Add(30 * time.Second)
	for !h.registered() {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("timeout waiting for registration")
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	nick = u.Nick()

	// Enqueue well over recvq in one shot; pacing must keep us under 1024 on the wire.
	const n = 40
	payload := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" // 40 bytes body
	for i := 0; i < n; i++ {
		line := fmt.Sprintf("PRIVMSG %s :%s%d", nick, payload, i)
		if err := u.WriteRaw(line); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	// ~40 * (~15+len(nick)+40) ≈ 3KB+ queued; at 256 B/s needs ~12s after burst.
	time.Sleep(20 * time.Second)

	if h.disconnected() {
		t.Fatalf("uplink dropped during paced flood (likely recvq/Excess Flood): %v", h.discErr())
	}
	// Still able to send after drain.
	if err := u.WriteRaw("PRIVMSG " + nick + " :still-alive"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if h.disconnected() {
		t.Fatalf("dropped after flood drain: %v", h.discErr())
	}

	cancel()
	select {
	case <-errc:
	case <-time.After(5 * time.Second):
	}
}

type floodHandler struct {
	mu   sync.Mutex
	reg  bool
	disc bool
	err  error
}

func (h *floodHandler) OnRegistered(*uplink.Uplink) {
	h.mu.Lock()
	h.reg = true
	h.mu.Unlock()
}
func (h *floodHandler) OnMessage(*uplink.Uplink, irc.Message)          {}
func (h *floodHandler) OnDisconnect(_ *uplink.Uplink, err error)       {
	h.mu.Lock()
	h.disc = true
	h.err = err
	h.mu.Unlock()
}
func (h *floodHandler) OnRegistrationLine(*uplink.Uplink, irc.Message) {}
func (h *floodHandler) OnCapsChanged(*uplink.Uplink, []string, []string) {}
func (h *floodHandler) OnSASLOffer(*uplink.Uplink, bool)               {}
func (h *floodHandler) OnCapNAK(*uplink.Uplink, []string)              {}

func (h *floodHandler) registered() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reg
}
func (h *floodHandler) disconnected() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.disc
}
func (h *floodHandler) discErr() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}
