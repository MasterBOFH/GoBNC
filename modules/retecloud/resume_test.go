package retecloud

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func TestBrainReloadKeepsWebSocketAndFillsTheGap(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c")
	c := e.syncedClient("ios-1", "undernet", "#c")
	mark := c.count()
	for i := 1; i <= 3; i++ {
		e.feed("undernet", fmt.Sprintf(":peer!p@h PRIVMSG #c :n-%d", i))
	}
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.HasSuffix(f["data"].(map[string]any)["raw"].(string), "n-3")
	})

	e.stopSrv() // the brain reloads
	time.Sleep(100 * time.Millisecond)
	for i := 4; i <= 8; i++ {
		e.feed("undernet", fmt.Sprintf(":peer!p@h PRIVMSG #c :n-%d", i)) // stored by the new brain's resume
	}
	rid := c.request("send", map[string]any{"command": "PRIVMSG #c :while away"}, e.sid("undernet"))
	if f := c.waitFor(mark, 5*time.Second, reply(rid, "send_success", "send_error")); f["error"] != "Bouncer is reloading" {
		t.Fatalf("send during the reload: %v", f)
	}
	e.startBrain()
	c.waitFor(mark, 10*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.HasSuffix(f["data"].(map[string]any)["raw"].(string), "n-8")
	})
	e.feed("undernet", ":peer!p@h PRIVMSG #c :n-9")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.HasSuffix(f["data"].(map[string]any)["raw"].(string), "n-9")
	})

	var order []string
	for _, f := range c.since(mark) {
		if f["type"] == "irc_line" {
			raw := f["data"].(map[string]any)["raw"].(string)
			if i := strings.LastIndex(raw, " :n-"); i >= 0 {
				order = append(order, raw[i+2:])
			}
		}
	}
	want := []string{"n-1", "n-2", "n-3", "n-4", "n-5", "n-6", "n-7", "n-8", "n-9"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("lines = %v, want %v once each, in order", order, want)
	}
}

func TestRestartedNetworkIsFollowedAgain(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c")
	c := e.syncedClient("ios-1", "undernet", "#c")

	// The brain stops and starts the network: a new Session in its place.
	id := e.nets["undernet"]
	n, _ := e.be.db.NetworkByName(context.Background(), "undernet")
	fresh := session.New(n, e.be.db, history.New(e.be.db), nil, nil)
	e.be.mu.Lock()
	e.be.sessions[id] = fresh
	e.be.mu.Unlock()
	e.feed("undernet", ":srv.example 001 me :Welcome", ":srv.example 376 me :End", ":me!u@h JOIN #c")
	mark := c.count()
	e.srv.NetworkChanged(wire.NetworkEvent{Network: id, Name: "undernet", State: wire.NetRegistered, Change: wire.ChangeAdded})
	time.Sleep(300 * time.Millisecond)
	e.feed("undernet", ":peer!p@h PRIVMSG #c :after restart")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.HasSuffix(f["data"].(map[string]any)["raw"].(string), "after restart")
	})
}
