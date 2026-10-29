package server

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// readPeerLines collects n line events for buffer "peer" from mod, returning their texts by id.
func readPeerLines(t *testing.T, mod *modTestClient, n int) map[int64]string {
	t.Helper()
	got := map[int64]string{}
	deadline := time.Now().Add(10 * time.Second)
	for len(got) < n {
		f, err := mod.read(time.Until(deadline))
		if err != nil {
			t.Fatalf("waiting for %d lines, have %d: %v", n, len(got), err)
		}
		if f.Kind != wire.KindEvent || f.Event.Event != wire.EventLine {
			continue
		}
		var ev wire.LineEvent
		_ = json.Unmarshal(f.Event.Data, &ev)
		for _, b := range ev.Buffers {
			if b.Key == "peer" && b.ID != 0 {
				if _, dup := got[b.ID]; dup {
					t.Fatalf("line id %d delivered twice", b.ID)
				}
				got[b.ID] = ev.Raw
			}
		}
	}
	return got
}

// TestModuleSurvivesBrainReload: a module subscribed to the old brain loses its connection at
// the reload, reconnects to the new brain (same module socket), is told not_ready until
// LiveReady, and after re-subscribing gets every line it missed from history exactly once.
func TestModuleSurvivesBrainReload(t *testing.T) {
	rk := startResumeTestKeeper(t)
	dir, err := os.MkdirTemp("", "gbr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	dbPath, sock := filepath.Join(dir, "t.db"), filepath.Join(dir, "m.sock")
	modules := config.ModulesConfig{Socket: sock, List: []config.ModuleConfig{{Name: "m",
		Features: []string{"networks", "events", "history", "clients", "send"}}}}
	features := modules.List[0].Features

	s1 := newResumeTestServer(t, dbPath)
	s1.cfg.Modules = modules
	s1.startModules(s1.runCtx)
	fake := newDemuxFakeIRC(t)
	h, p := fake.addr(t)
	closeAfter := make(chan struct{})
	defer close(closeAfter)
	go fake.serveOne(t, "fake.example", closeAfter)
	if _, err := s1.Store().UpsertNetwork(s1.runCtx, store.Network{Name: "net1", Host: h, Port: p, Nick: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	n, err := s1.Store().NetworkByName(s1.runCtx, "net1")
	if err != nil {
		t.Fatal(err)
	}
	rk.attach(t, s1, []store.Network{n})
	sess1, _ := s1.Session("net1")
	waitForRegistered(t, "net1", sess1.Registered)
	ircConn := fake.lastConn(t)
	say := func(i int) {
		if _, err := fmt.Fprintf(ircConn, ":peer!p@h PRIVMSG alice :line-%d\r\n", i); err != nil {
			t.Fatal(err)
		}
	}

	mod := dialTestModule(t, sock)
	if werr := mod.call(wire.MethodHello, wire.HelloParams{API: 1, Module: "m", Features: features}, nil); werr != nil {
		t.Fatal(werr)
	}
	if werr := mod.call(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: n.ID}, nil); werr != nil {
		t.Fatal(werr)
	}
	for i := 1; i <= 5; i++ {
		say(i)
	}
	seen := readPeerLines(t, mod, 5)
	var lastID int64
	for id := range seen {
		lastID = max(lastID, id)
	}

	// Reload: the replacement brain is up (its module socket replaces the old one) before the
	// old brain lets go of the keeper and exits.
	s2 := newResumeTestServer(t, dbPath)
	s2.cfg.Modules = modules
	s2.startModules(s2.runCtx)
	time.Sleep(50 * time.Millisecond)
	s1.detachFromKeeper()
	s1.cancel()
	for { // the old brain closes the module connection
		_, err := mod.read(5 * time.Second)
		if err == nil {
			continue
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatal("the old brain never closed the module connection")
		}
		break
	}
	time.Sleep(50 * time.Millisecond) // the old module server's cleanup has run
	for i := 6; i <= 10; i++ {
		say(i) // while no brain is attached
	}

	mod2 := dialTestModule(t, sock)
	var hello wire.HelloResult
	if werr := mod2.call(wire.MethodHello, wire.HelloParams{API: 1, Module: "m", Features: features}, &hello); werr != nil {
		t.Fatal(werr)
	}
	if hello.Ready {
		t.Fatal("new brain said ready before LiveReady")
	}
	if werr := mod2.call(wire.MethodSendLine, wire.SendLineParams{Network: n.ID, Client: "ws1", Line: "PRIVMSG peer :too early"}, nil); werr == nil || werr.Code != wire.CodeNotReady {
		t.Fatalf("send.line before ready: %v, want not_ready", werr)
	}

	rk.attach(t, s2, []store.Network{n})
	mod2.waitEvent(wire.EventReady, 5*time.Second, nil)
	sess2, _ := s2.Session("net1")
	waitForRegistered(t, "net1 (resumed)", sess2.Registered)
	if werr := mod2.call(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: n.ID}, nil); werr != nil {
		t.Fatal(werr)
	}
	deadline := time.Now().Add(10 * time.Second)
	var page wire.HistoryMessagesResult
	for {
		if werr := mod2.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: n.ID, Buffer: "peer", AfterID: &lastID, Limit: 100}, &page); werr != nil {
			t.Fatal(werr)
		}
		if len(page.Messages) >= 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("history after id %d has %d lines, want the 5 sent during the reload", lastID, len(page.Messages))
		}
		time.Sleep(50 * time.Millisecond)
	}
	all := map[string]int{}
	for _, raw := range seen {
		all[raw[strings.LastIndex(raw, "line-"):]]++
	}
	for _, m := range page.Messages {
		if _, dup := seen[m.ID]; dup {
			t.Fatalf("history after_id=%d returned already-seen id %d", lastID, m.ID)
		}
		all[m.Text]++
	}
	for i := 1; i <= 10; i++ {
		if c := all[fmt.Sprintf("line-%d", i)]; c != 1 {
			t.Fatalf("line-%d seen %d times (live %v, history %+v)", i, c, seen, page.Messages)
		}
	}
}
