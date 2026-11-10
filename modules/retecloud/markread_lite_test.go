package retecloud

import (
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
)

// Rete A3054: an IRC client on the same bouncer (a desktop client with IRCv3 draft/read-marker)
// marks a channel read with MARKREAD. A lite client (the iPhone) must hear it: read_cursor with
// the stored id the timestamp covers, and the channel's badge cleared — an activity frame with no
// unread — since the phone follows the channel without being subscribed to it.

type markReadDownlink struct{}

func (markReadDownlink) ID() session.ClientID   { return "irc-desktop" }
func (markReadDownlink) Caps() map[string]bool  { return map[string]bool{readMarkerCapName: true} }
func (markReadDownlink) HasCap(n string) bool   { return n == readMarkerCapName }
func (markReadDownlink) ClearCap(string)        {}
func (markReadDownlink) HasSeenCap(string) bool { return true }
func (markReadDownlink) MarkSeenCap(string)     {}
func (markReadDownlink) Send(irc.Message) error { return nil }
func (markReadDownlink) Close() error           { return nil }
func (markReadDownlink) RemoteAddr() string     { return "127.0.0.1" }

const readMarkerCapName = "draft/read-marker"

func TestIRCClientMarkReadReachesLite(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c", ":peer!p@h PRIVMSG #c :read on the desktop")
	e.knownDevice("ios-a", "undernet") // not a first sync: #c stays unread until someone reads it
	c := e.dial(false)
	c.authenticate("ios-a")
	_, frames := c.liteSync(e.sid("undernet"), "ios-a", nil)
	if b := bootstrapBuffer(successData(t, frames), "#c"); b == nil || b["unread"] != float64(1) {
		t.Fatalf("#c on the phone before = %v, want 1 unread", b)
	}
	last := lastStoredID(t, e, "undernet", "#c")
	mark := c.count()

	sess := e.session("undernet")
	dl := markReadDownlink{}
	if err := sess.Attach(dl); err != nil {
		t.Fatal(err)
	}
	msg, err := irc.Parse("MARKREAD #c timestamp=" + time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.HandleClientMessage(dl, msg); err != nil {
		t.Fatal(err)
	}

	rc := c.waitFor(mark, 5*time.Second, isType("read_cursor"))
	if d := rc["data"].(map[string]any); d["buffer"] != "#c" || d["last_message_id"] != float64(last) {
		t.Fatalf("read_cursor = %v, want #c at %d", rc, last)
	}
	act := c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		if f["type"] != "activity" {
			return false
		}
		for _, b := range f["data"].(map[string]any)["buffers"].([]any) {
			if b.(map[string]any)["buffer"] == "#c" {
				return true
			}
		}
		return false
	})
	for _, b := range act["data"].(map[string]any)["buffers"].([]any) {
		if entry := b.(map[string]any); entry["buffer"] == "#c" && (entry["unread"] != float64(0) || entry["highlights"] != float64(0)) {
			t.Fatalf("#c's activity after the desktop read it = %v, want no unread and no highlights", entry)
		}
	}

	// The next sync agrees: #c is read on the phone.
	_, frames = c.liteSync(e.sid("undernet"), "ios-a", nil)
	if b := bootstrapBuffer(successData(t, frames), "#c"); b == nil || b["unread"] != nil || b["read_id"] != float64(last) {
		t.Fatalf("#c on the next sync = %v, want no unread and read_id %d", b, last)
	}
}
