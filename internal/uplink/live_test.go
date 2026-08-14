//go:build ircd

// This is the 3b-i checkpoint proof: internal/uplink's real, unmodified
// register()/session()/Run() path still completes a full live registration
// against real ircds. Nothing in this package changed this round — the new
// keeper/brain infrastructure is entirely separate code (internal/keeper,
// internal/registration, internal/brain). This test exists only to make
// "the old read loop still works" a checked claim instead of an assumption
// resting on "nothing touched this package."
//
//	Run: (cd docker/ircd && docker compose up -d ergo) && \
//	  go test -tags ircd ./internal/uplink/... -run Live -v
package uplink

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

const (
	liveErgoAddr = "127.0.0.1"
	liveErgoPort = 6667

	liveRemoteAddr = "192.168.171.1"
	liveRemotePort = 6667
)

var liveUplinkNickCounter atomic.Uint64

func liveUplinkNick(prefix string) string {
	return fmt.Sprintf("%s%d%d", prefix, time.Now().Unix()%100000, liveUplinkNickCounter.Add(1))
}

// recordingHandler implements Handler, capturing just enough to prove
// registration actually completed — it isn't trying to exercise every
// callback, only the ones this checkpoint proof needs.
type recordingHandler struct {
	mu         sync.Mutex
	registered bool
	regCh      chan struct{}
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{regCh: make(chan struct{})}
}

func (h *recordingHandler) OnRegistered(u *Uplink) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.registered {
		h.registered = true
		close(h.regCh)
	}
}
func (h *recordingHandler) OnMessage(u *Uplink, msg irc.Message)             {}
func (h *recordingHandler) OnDisconnect(u *Uplink, err error)                {}
func (h *recordingHandler) OnRegistrationLine(u *Uplink, msg irc.Message)    {}
func (h *recordingHandler) OnCapsChanged(u *Uplink, added, removed []string) {}
func (h *recordingHandler) OnSASLOffer(u *Uplink, available bool)            {}
func (h *recordingHandler) OnCapNAK(u *Uplink, names []string)               {}

func runLiveUplinkRegistration(t *testing.T, host string, port int, nick string) {
	t.Helper()
	h := newRecordingHandler()
	u := New(Config{
		Network: store.Network{
			Host:         host,
			Port:         port,
			Nick:         nick,
			AltNick:      nick + "_",
			NickRecovery: true,
		},
	}, h)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- u.Run(ctx) }()

	select {
	case <-h.regCh:
		t.Logf("registered as %q against %s:%d", u.Nick(), host, port)
	case err := <-runErr:
		t.Fatalf("Run exited before registering: %v", err)
	case <-time.After(25 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	if !u.Registered() {
		t.Fatalf("Registered()=false despite OnRegistered firing")
	}
	cancel()
	<-runErr
}

// TestLiveUplinkRegistersAgainstErgo proves the old, unmodified path still
// completes a real registration against docker/ircd's ergo service.
func TestLiveUplinkRegistersAgainstErgo(t *testing.T) {
	runLiveUplinkRegistration(t, liveErgoAddr, liveErgoPort, liveUplinkNick("gbuplink"))
}

// TestLiveUplinkRegistersAgainstRemote proves the same against the real
// remote Undernet network used throughout this project's soak testing.
func TestLiveUplinkRegistersAgainstRemote(t *testing.T) {
	runLiveUplinkRegistration(t, liveRemoteAddr, liveRemotePort, liveUplinkNick("gbuplink"))
}
