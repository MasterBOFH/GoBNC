package retecloud

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The lite Server buffer keeps its wire name "Server"; underneath, its notices and $mask lines
// are stored under the servers' names, its read cursor is the Server buffer's own ("*", not a
// nick Server's "server"), and get_messages reads every server's lines together.
func TestLiteServerBufferReadsTheServerTargets(t *testing.T) {
	if k := newNetwork(1).key(wire.ServerBuffer); k != wire.ServerBufferKey {
		t.Fatalf("module key(Server) = %q, want *", k)
	}
	e := newTestEnv(t, "undernet")
	ctx := context.Background()
	c := e.syncedClient("ios-1", "undernet")
	mark := c.count()
	e.feed("undernet", ":irc.example.net NOTICE me :*** Notice -- snotice")
	d := ircLineOf(t, c, mark, "snotice")
	if d["buffer"] != wire.ServerBuffer {
		t.Fatalf("irc_line = %v, want buffer Server", d)
	}
	id := int64(d["id"].(float64))
	m, err := e.be.db.MessageByID(ctx, e.nets["undernet"], id)
	if err != nil || m == nil || m.Target != "irc.example.net" {
		t.Fatalf("stored row = %+v (err %v), want target irc.example.net", m, err)
	}
	mk := c.count()
	rid := c.request("read_cursor", map[string]any{"buffer": wire.ServerBuffer, "last_message_id": id}, e.sid("undernet"))
	c.waitFor(mk, 5*time.Second, reply(rid, "read_cursor_success"))
	if _, ok, _ := e.be.db.GetReadMarker(ctx, e.nets["undernet"], wire.ServerBufferKey); !ok {
		t.Fatal("Server read cursor not stored under *")
	}
	if _, ok, _ := e.be.db.GetReadMarker(ctx, e.nets["undernet"], "server"); ok {
		t.Fatal("Server read cursor stored under server")
	}

	mk = c.count()
	e.feed("undernet", ":oper!o@h NOTICE $*.example.net :global notice")
	d = ircLineOf(t, c, mk, "global notice")
	if d["buffer"] != wire.ServerBuffer || d["id"] == nil {
		t.Fatalf("irc_line = %v, want buffer Server with an id", d)
	}
	if m, err := e.be.db.MessageByID(ctx, e.nets["undernet"], int64(d["id"].(float64))); err != nil || m == nil || !irc.IsServerName(m.Target) {
		t.Fatalf("stored $mask row = %+v (err %v), want a server's name", m, err)
	}
	mk = c.count()
	e.feed("undernet", ":services.example.net NOTICE me :services notice")
	ircLineOf(t, c, mk, "services notice")
	mk = c.count()
	rid = c.request("get_messages", map[string]any{"buffer": wire.ServerBuffer}, e.sid("undernet"))
	msgs := c.waitFor(mk, 5*time.Second, reply(rid, "get_messages_success"))["data"].(map[string]any)["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("Server history = %v, want the three Server lines", msgs)
	}
}
