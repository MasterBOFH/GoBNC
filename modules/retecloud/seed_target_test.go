package retecloud

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// A3005: a row an earlier seed opened under the wrong key (target our own nick "me", displayed
// "Melksedek", as on the owner's bouncer) is listed by display. Closing it by that name closes
// it for good: a later bootstrap, on a new connection of the device, no longer lists it.
func TestCloseMisKeyedSeedRowStaysClosedAcrossBootstrap(t *testing.T) {
	e := newTestEnv(t, "undernet")
	ctx := context.Background()
	net := e.nets["undernet"]
	if _, err := e.be.db.InsertMessage(ctx, store.Message{NetworkID: net, Target: "me", Time: time.Now().UTC(),
		Command: "PRIVMSG", Source: "Melksedek!m@h", Raw: ":Melksedek!m@h PRIVMSG me :old line", Text: "old line"}); err != nil {
		t.Fatal(err)
	}
	e.knownDevice("ios-1", "undernet")
	if err := e.be.db.SetBufferOpen(ctx, "ios-1", net, "me", "Melksedek", true, 0); err != nil {
		t.Fatal(err)
	}

	c := e.dial(false)
	c.authenticate("ios-1")
	_, frames := c.liteSync(e.sid("undernet"), "ios-1", nil)
	if bootstrapBuffer(successData(t, frames), "Melksedek") == nil {
		t.Fatal("the mis-keyed row is not listed: the fixture does not reproduce the bug")
	}
	rid := c.request("close_buffer", map[string]any{"buffer": "Melksedek"}, e.sid("undernet"))
	c.waitFor(0, 5*time.Second, reply(rid, "close_buffer_success"))

	again := e.dial(false)
	again.authenticate("ios-1")
	_, frames = again.liteSync(e.sid("undernet"), "ios-1", nil)
	if b := bootstrapBuffer(successData(t, frames), "Melksedek"); b != nil {
		t.Fatalf("closed buffer listed again after reconnect: %v", b)
	}
}
