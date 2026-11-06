package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/config"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A stopped network keeps the nick it last had: the alternate it registered with after a
// collision, then a nick it changed to later, rather than the configured one. Modules see it
// in networks.list and in the stopped state event.
func TestStoppedNetworkKeepsLastNick(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.TLSCert = filepath.Join(dir, "missing.crt")
	cfg.TLSKey = filepath.Join(dir, "missing.key")
	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	attachTestKeeper(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.runCtx = ctx
	s.cancel = cancel

	if _, err := s.Store().UpsertNetwork(ctx, store.Network{Name: "net1", Host: "127.0.0.1", Port: 1, Nick: "n", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	n, err := s.Store().NetworkByName(ctx, "net1")
	if err != nil {
		t.Fatal(err)
	}
	if info := s.networkInfo(n); info.State != wire.NetStopped || info.Nick != "n" {
		t.Fatalf("never run: %+v, want stopped with the configured nick", info)
	}
	if err := s.StartNetworkByName("net1"); err != nil {
		t.Fatal(err)
	}
	sess, err := s.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	// "n" was taken: the uplink registered as the alternate.
	sess.HandleLine([]byte(":srv.example 001 n_ :Welcome"), 1)
	sess.HandleLine([]byte(":srv.example 376 n_ :End"), 2)
	if nick, _ := s.Store().LastNick(ctx, n.ID); nick != "n_" {
		t.Fatalf("after registering, last nick = %q, want n_", nick)
	}
	sess.HandleLine([]byte(":n_!u@h NICK :n2"), 3)

	if err := s.StopNetwork("net1"); err != nil {
		t.Fatal(err)
	}
	if nick, _ := s.Store().LastNick(ctx, n.ID); nick != "n2" {
		t.Fatalf("after stopping, last nick = %q, want n2", nick)
	}
	if info := s.networkInfo(n); info.State != wire.NetStopped || info.Nick != "n2" {
		t.Fatalf("stopped: %+v, want stopped with nick n2", info)
	}
}
