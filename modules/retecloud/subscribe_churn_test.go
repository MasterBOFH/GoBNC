package retecloud

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// applyMembership applies one live line to a member set the way the lite spec asks a client to
// (idempotently): JOIN adds, PART/KICK remove, QUIT removes, NICK renames.
func applyMembership(members map[string]bool, raw, channel string) {
	msg, err := irc.Parse(raw)
	if err != nil {
		return
	}
	nick := strings.ToLower(msg.Nick())
	switch msg.Command {
	case "JOIN":
		if strings.EqualFold(msg.Param(0), channel) {
			members[nick] = true
		}
	case "PART":
		if strings.EqualFold(msg.Param(0), channel) {
			delete(members, nick)
		}
	case "KICK":
		if strings.EqualFold(msg.Param(0), channel) {
			delete(members, strings.ToLower(msg.Param(1)))
		}
	case "QUIT":
		delete(members, nick)
	case "NICK":
		if members[nick] {
			delete(members, nick)
			members[strings.ToLower(msg.Param(0))] = true
		}
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// A membership line the brain processes after a subscribe's snapshot but before the subscribe
// reads history is either reflected in the snapshot's state or delivered live afterwards: the
// lite ordering guarantee (no gaps). Before the fix, history (read later) held the line, the
// live copy was then suppressed as "already in history", and state still listed the leaver.
func TestSubscribeChurnBetweenSnapshotAndHistoryNoGap(t *testing.T) {
	cases := []struct {
		name  string
		churn string
		want  []string
	}{
		{"part", ":peer!p@h PART #c :bye", []string{"me", "stay"}},
		{"kick", ":stay!s@h KICK #c peer :out", []string{"me", "stay"}},
		{"quit", ":peer!p@h QUIT :gone", []string{"me", "stay"}},
		{"nick", ":peer!p@h NICK peer2", []string{"me", "peer2", "stay"}},
		{"join", ":fresh!f@h JOIN #c", []string{"fresh", "me", "peer", "stay"}},
	}
	for _, tc := range cases {
		for _, mode := range []string{"standalone", "inline", "first_unread"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				var mu sync.Mutex
				var fire func()
				e := newTestEnvWith(t, func(m *Module) {
					m.afterSubscribeSnapshot = func(int64) {
						mu.Lock()
						f := fire
						fire = nil
						mu.Unlock()
						if f != nil {
							f()
						}
					}
				}, "undernet")
				e.feed("undernet", ":me!u@h JOIN #c", ":srv.example 353 me = #c :@me stay peer",
					":srv.example 366 me #c :End")
				c := e.dial(false)
				c.authenticate("ios-1")
				mu.Lock()
				fire = func() { e.feed("undernet", tc.churn) }
				mu.Unlock()

				mark := c.count()
				var rid string
				switch mode {
				case "inline":
					rid, _ = c.liteSync(e.sid("undernet"), "ios-1", map[string]any{"subscribe": []any{
						map[string]any{"buffer": "#c", "history": 50, "members": "full"}}})
				case "standalone":
					c.liteSync(e.sid("undernet"), "ios-1", nil)
					rid = c.request("subscribe_buffer", map[string]any{"buffer": "#c", "history": 50, "members": "full"}, e.sid("undernet"))
				case "first_unread": // the page around the first unread line, read up to "read"
					e.feed("undernet", ":stay!s@h PRIVMSG #c :read", ":stay!s@h PRIVMSG #c :unread")
					c.liteSync(e.sid("undernet"), "ios-1", nil)
					read, err := e.be.db.MaxMessageID(context.Background(), e.nets["undernet"])
					if err != nil {
						t.Fatal(err)
					}
					read-- // "read", not "unread"
					m := c.count()
					cr := c.request("read_cursor", map[string]any{"buffer": "#c", "last_message_id": read}, e.sid("undernet"))
					c.waitFor(m, 5*time.Second, reply(cr, "read_cursor_success"))
					rid = c.request("subscribe_buffer", map[string]any{"buffer": "#c", "history": 50, "members": "full",
						"anchor": "first_unread"}, e.sid("undernet"))
				}
				sub := c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error"))
				if sub["type"] != "subscribe_buffer_success" {
					t.Fatalf("subscribe: %v", sub)
				}
				if _, anchored := sub["data"].(map[string]any)["anchor_id"]; anchored != (mode == "first_unread") {
					t.Fatalf("anchor_id present = %v in mode %s: %v", anchored, mode, sub["data"])
				}
				mu.Lock()
				fired := fire == nil
				mu.Unlock()
				if !fired {
					t.Fatal("the churn line was never injected")
				}
				e.feed("undernet", ":stay!s@h PRIVMSG #c :sentinel")
				c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
					d, _ := f["data"].(map[string]any)
					raw, _ := d["raw"].(string)
					return f["type"] == "irc_line" && strings.HasSuffix(raw, ":sentinel")
				})

				members := map[string]bool{}
				state := sub["data"].(map[string]any)["state"].(map[string]any)
				for _, u := range state["users"].([]any) {
					members[strings.ToLower(u.(map[string]any)["nick"].(string))] = true
				}
				after := false
				for _, f := range c.since(mark) {
					if f["type"] == "subscribe_buffer_success" && f["request_id"] == rid {
						after = true
						continue
					}
					if after && f["type"] == "irc_line" {
						applyMembership(members, f["data"].(map[string]any)["raw"].(string), "#c")
					}
				}
				if got := sortedKeys(members); strings.Join(got, ",") != strings.Join(tc.want, ",") {
					t.Fatalf("state + live lines = %v, want %v (state %v)", got, tc.want, state["users"])
				}
			})
		}
	}
}
