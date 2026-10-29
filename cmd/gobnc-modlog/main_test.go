package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/module"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// brain is a minimal module.Backend over one Session fed lines directly (the uplink's side).
type brain struct{ sess *session.Session }

func (b brain) BrainVersion() string         { return "test" }
func (b brain) Ready() bool                  { return true }
func (b brain) Networks() []wire.NetworkInfo { return []wire.NetworkInfo{{ID: 1, Name: "undernet"}} }
func (b brain) Session(id int64) (*session.Session, bool) {
	return b.sess, id == 1
}
func (b brain) Store() *store.Store           { return nil }
func (b brain) Config() config.Config         { return config.Config{} }
func (b brain) ConnectNetwork(int64) error    { return nil }
func (b brain) DisconnectNetwork(int64) error { return nil }
func (b brain) ReconnectNetwork(int64) error  { return nil }

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestModlogPrintsChannelMessageOnce(t *testing.T) {
	dir, err := os.MkdirTemp("", "gml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "m.sock")

	sess := session.New(store.Network{ID: 1, Name: "undernet", Nick: "me"}, nil, nil, nil, nil)
	for i, l := range []string{":srv.example 001 me :hi", ":srv.example 376 me :end", ":me!u@h JOIN #rete"} {
		sess.HandleLine([]byte(l), uint64(i+1))
	}
	srv := module.NewServer(config.ModulesConfig{Socket: sock, List: []config.ModuleConfig{
		{Name: "modlog", Features: []string{"networks", "events"}}}}, brain{sess}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listening := make(chan struct{})
	srv.OnListening(func() { close(listening) })
	go func() { _ = srv.Serve(ctx) }()
	<-listening

	out := &syncBuf{}
	go func() { _ = run(ctx, moduleapi.Options{Socket: sock, Module: "modlog", SkipLock: true}, out) }()
	deadline := time.Now().Add(5 * time.Second)
	for sess.Subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("modlog never subscribed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	sess.HandleLine([]byte(":peer!p@h PRIVMSG #rete :hello log"), 10)
	sess.HandleLine([]byte(":peer!p@h JOIN #rete"), 11) // not a message: filtered by kinds
	for !strings.Contains(out.String(), "hello log") {
		if time.Now().After(deadline) {
			t.Fatalf("output = %q", out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	got := strings.TrimSpace(out.String())
	if strings.Count(got, "\n") != 0 || !strings.Contains(got, " undernet #rete ") || !strings.Contains(got, "PRIVMSG #rete :hello log") {
		t.Fatalf("output = %q, want one line for the PRIVMSG", got)
	}
}
