package retecloud

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Rete A3131: the uplink dropped and came back while the iPhone was away, and its resync of the
// Server buffer held nothing: the burst was consumed by registration, and neither the drop nor
// the reconnect left a row. Now the Server buffer's lite page holds the uplink's ERROR, a State
// row for the drop (with its reason), one for the new connection, the new burst's numerics and
// one for the registration, in that order; the live state frames name the same instants as the
// rows; none of it counts as unread; and IRC clients never see those rows as a target.
func TestServerPageHoldsTheUplinkDropAndReconnect(t *testing.T) {
	e := newTestEnv(t, "undernet")
	ctx := context.Background()
	nid := e.nets["undernet"]
	live := e.syncedClient("ios-live", "undernet")

	// The drop: the server's ERROR, then the connection goes; a failed attempt after it stores
	// no second drop.
	mark := live.count()
	e.feed("undernet", "ERROR :Closing Link: me (Ping timeout)")
	e.session("undernet").HandleDisconnect(errors.New("server ERROR: Closing Link: me (Ping timeout)"))
	down := live.waitFor(mark, 5*time.Second, isState(false, false))
	e.session("undernet").HandleDisconnect(errors.New("dial tcp: connection refused"))

	// The reconnect: a new connection's burst, then registration.
	mark = live.count()
	e.feed("undernet",
		":irc2.example 001 me :Welcome back",
		":irc2.example 005 me CHANTYPES=# PREFIX=(ov)@+ CASEMAPPING=rfc1459 :ok",
		":irc2.example 251 me :There are 3 users",
		":irc2.example 375 me :- irc2.example Message of the Day -",
		":irc2.example 372 me :- hello",
		":irc2.example 376 me :End of /MOTD command.")
	connecting := live.waitFor(mark, 5*time.Second, isState(true, false))
	registered := live.waitFor(mark, 5*time.Second, isState(true, true))

	// A device that was away resyncs the Server buffer.
	away := e.dial(false)
	away.authenticate("ios-away")
	mk := away.count()
	away.liteSync(e.sid("undernet"), "ios-away", map[string]any{
		"subscribe": []any{map[string]any{"buffer": wire.ServerBuffer, "history": 50, "after_id": 0}}})
	page := away.waitFor(mk, 5*time.Second, isType("subscribe_buffer_success"))["data"].(map[string]any)
	var got []string
	stateTimes := map[string]string{}
	for _, m := range page["messages"].([]any) {
		row := m.(map[string]any)
		ed := row["event_data"].(map[string]any)
		switch row["event"] {
		case "State":
			s := fmt.Sprintf("State connected=%v registered=%v", ed["connected"], ed["registered"])
			if msg, ok := ed["message"].(string); ok {
				s += " message=" + msg
			}
			got = append(got, s)
			stateTimes[fmt.Sprintf("%v/%v", ed["connected"], ed["registered"])] = ed["timestamp"].(string)
		case "Raw":
			f := strings.Fields(strings.SplitN(ed["raw"].(string), " ", 2)[1])
			if f[0] == "ERROR" {
				got = append(got, "ERROR")
			} else {
				got = append(got, f[1]) // the numeric
			}
		default:
			t.Fatalf("row of event %v: %v", row["event"], row)
		}
	}
	want := []string{
		// The first registration (the fixture's), then the drop and the reconnect.
		"001", "005", "376", "State connected=true registered=true",
		"ERROR", "State connected=false registered=false message=Closing Link: me (Ping timeout)",
		"State connected=true registered=false",
		"001", "005", "251", "375", "372", "376", "State connected=true registered=true",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Server page =\n  %v\nwant\n  %v", got, want)
	}
	for key, frame := range map[string]map[string]any{"false/false": down, "true/false": connecting, "true/true": registered} {
		if at := frame["data"].(map[string]any)["timestamp"]; at != stateTimes[key] {
			t.Fatalf("live state %s at %v, its row at %v", key, at, stateTimes[key])
		}
	}

	// None of it is unread: the Server buffer is not listed for the away device.
	for _, b := range away.waitFor(0, 5*time.Second, isType("sync_session_success"))["data"].(map[string]any)["buffers"].([]any) {
		if b.(map[string]any)["name"] == wire.ServerBuffer {
			t.Fatalf("Server listed with unread: %v", b)
		}
	}

	// No IRC target: not listed for playback or CHATHISTORY TARGETS.
	targets, err := e.be.db.DistinctTargets(ctx, nid)
	if err != nil {
		t.Fatal(err)
	}
	for _, tg := range targets {
		if tg == wire.ServerBufferKey {
			t.Fatalf("DistinctTargets lists %q: %v", tg, targets)
		}
	}
	acts, err := e.be.db.TargetsBetween(ctx, nid, time.Now().Add(-time.Hour), time.Now().Add(time.Hour), nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range acts {
		if a.Target == wire.ServerBufferKey {
			t.Fatalf("TargetsBetween lists %q", a.Target)
		}
	}
}

func isState(connected, registered bool) func(map[string]any) bool {
	return func(f map[string]any) bool {
		if f["type"] != "state" {
			return false
		}
		d := f["data"].(map[string]any)
		return d["connected"] == connected && d["registered"] == registered
	}
}
