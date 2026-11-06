package retecloud

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A network the module learns of without ever seeing it run (no snapshot, so no ISUPPORT)
// classifies names by GoBNC's default channel prefixes, the ones the brain's stored view of a
// stopped network uses: none of them can start a nick, so no query is taken for a channel.
func TestNewNetworkClassifiesByDefaultChanTypes(t *testing.T) {
	n := newNetwork(1)
	for _, name := range []string{"#c", "&local", "+modeless", "!safe"} {
		if got := n.bufferKind(name); got != "channel" {
			t.Errorf("bufferKind(%q) = %q, want channel", name, got)
		}
	}
	if got := n.bufferKind("friend"); got != "query" {
		t.Errorf("bufferKind(friend) = %q, want query", got)
	}
	if got := n.bufferKind(wire.ServerBuffer); got != "server" {
		t.Errorf("bufferKind(Server) = %q, want server", got)
	}
	if n.joined == nil {
		t.Error("joined is nil")
	}
}

// A lite bootstrap of a stopped network the module never saw running labels a channel with
// unread lines a channel, not a query.
func TestLiteSyncNeverRunNetworkLabelsChannels(t *testing.T) {
	e := newTestEnv(t)
	ctx := context.Background()
	id, err := e.be.db.UpsertNetwork(ctx, store.Network{Name: "quiet", Host: "h", Port: 1, Nick: "me", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.be.db.AddChannel(ctx, id, "#c", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.be.db.InsertMessage(ctx, store.Message{NetworkID: id, Target: "#c", Time: time.Now().Add(-time.Minute).UTC(),
		Command: "PRIVMSG", Source: "peer!p@h", Raw: ":peer!p@h PRIVMSG #c :hi", Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	e.nets["quiet"] = id
	// What internal/server sends when a network is added without being started.
	e.srv.NetworkChanged(wire.NetworkEvent{Network: id, Name: "quiet", State: wire.NetStopped, Nick: "me", Change: wire.ChangeAdded})

	c := e.dial(false)
	c.authenticate("ios-1")
	_, frames := c.liteSync(e.sid("quiet"), "ios-1", nil)
	d := successData(t, frames)
	if d["connected"] != false {
		t.Fatalf("bootstrap = %v, want disconnected", d)
	}
	if ch := liteBuffers(t, d)["#c"]; ch == nil || ch["kind"] != "channel" {
		t.Fatalf("#c = %v, want kind channel", ch)
	}
}

// A stopped network's highlights in a lite bootstrap name the nick it last had (the alternate
// after a collision, as the brain records it), not its configured one.
func TestLiteSyncStoppedNetworkHighlightsLastNick(t *testing.T) {
	e := newTestEnv(t, "undernet")
	ctx := context.Background()
	nid := e.nets["undernet"]
	if err := e.be.db.AddChannel(ctx, nid, "#c", ""); err != nil {
		t.Fatal(err)
	}
	e.feed("undernet", ":me!u@h JOIN #c")
	sid := e.sid("undernet")
	c := e.dial(false)
	c.authenticate("ios-1")
	e.knownDevice("ios-1", "undernet") // not a first sync: the backdated lines below stay unread
	c.liteSync(sid, "ios-1", nil)

	if err := e.be.db.SetLastNick(ctx, nid, "me_"); err != nil { // configured nick "me"
		t.Fatal(err)
	}
	t0 := time.Now().Add(-time.Hour).UTC()
	for i, text := range []string{"me_: ping", "hello all"} {
		if _, err := e.be.db.InsertMessage(ctx, store.Message{NetworkID: nid, Target: "#c",
			Time: t0.Add(time.Duration(i) * time.Second), Command: "PRIVMSG", Source: "peer!p@h", Raw: "x", Text: text}); err != nil {
			t.Fatal(err)
		}
	}
	mark := c.count()
	e.stopNetwork("undernet")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		d, _ := f["data"].(map[string]any)
		return f["type"] == "session_state" && d["state"] == "disconnected"
	})
	_, frames := c.liteSync(sid, "ios-1", nil)
	ch := liteBuffers(t, successData(t, frames))["#c"]
	if ch == nil || ch["unread"] != float64(2) || ch["highlights"] != float64(1) {
		t.Fatalf("#c = %v, want 2 unread, 1 highlight (the line naming me_)", ch)
	}
}
