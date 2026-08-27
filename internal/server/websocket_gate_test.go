package server

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/version"
)

// dialNetworkLocked must refuse a WebSocket network when the running keeper
// is older than the generation that can dial one — otherwise the old keeper
// would ignore DialConfig.WebSocket and silently open a plain connection.
func TestWebSocketNetworkRefusedOnOldKeeper(t *testing.T) {
	n := store.Network{Name: "ws", Host: "irc.example", Port: 443, TLS: true, Nick: "me", Enabled: true, WebSocket: true}
	sess := session.New(n, nil, nil, slog.Default(), nil)

	s := &Server{
		log:           slog.Default(),
		cfg:           config.Default(),
		sess:          map[string]*session.Session{n.Name: sess},
		sessByNetID:   map[keeper.NetworkID]*session.Session{sess.NetworkID(): sess},
		resumedAtBoot: map[keeper.NetworkID]bool{},
		keeperClient:  &keeper.AttachClient{KeeperVersion: version.WSMinKeeperVersion - 1},
	}

	err := s.dialNetworkLocked(n, sess)
	if err == nil {
		t.Fatal("dialNetworkLocked accepted a WebSocket network on a gen-2 keeper; want refusal")
	}
	if !strings.Contains(err.Error(), "WebSocket-capable keeper") {
		t.Fatalf("error = %v, want it to name the WebSocket-capable-keeper requirement", err)
	}
	// The half-registered session must be cleaned up, matching the other
	// dial-failure paths.
	if _, ok := s.sess[n.Name]; ok {
		t.Fatal("session left registered after a refused WebSocket dial")
	}
}
