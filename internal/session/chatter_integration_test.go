//go:build integration

package session_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

type memDL struct {
	id   session.ClientID
	caps map[string]bool
	seen map[string]bool
	mu   sync.Mutex
	sent []irc.Message
}

func (d *memDL) ID() session.ClientID { return d.id }
func (d *memDL) Caps() map[string]bool {
	return d.caps
}
func (d *memDL) HasCap(n string) bool { return d.caps[n] }
func (d *memDL) ClearCap(n string)    { delete(d.caps, n) }
func (d *memDL) HasSeenCap(n string) bool {
	return d.seen[n]
}
func (d *memDL) MarkSeenCap(n string) {
	if d.seen == nil {
		d.seen = make(map[string]bool)
	}
	d.seen[n] = true
}
func (d *memDL) Send(m irc.Message) error {
	d.mu.Lock()
	d.sent = append(d.sent, m)
	d.mu.Unlock()
	return nil
}
func (d *memDL) Close() error { return nil }
func (d *memDL) snapshot() []irc.Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]irc.Message, len(d.sent))
	copy(out, d.sent)
	return out
}
func (d *memDL) clear() {
	d.mu.Lock()
	d.sent = nil
	d.mu.Unlock()
}
func (d *memDL) countCmd(cmd string) int {
	n := 0
	for _, m := range d.snapshot() {
		if m.Command == cmd {
			n++
		}
	}
	return n
}

func TestHighChatterMultiClientPlayback(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "bouncernick", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	netw, _ := db.NetworkByName(ctx, "n")
	hist := history.New(db)
	sess := session.New(netw, db, hist, nil)

	const nClients = 8
	const nMsgs = 500
	clients := make([]*memDL, nClients)
	for i := 0; i < nClients; i++ {
		clients[i] = &memDL{
			id: session.ClientID(fmt.Sprintf("c%d", i)),
			caps: map[string]bool{
				"message-tags": true,
				"server-time":  true,
				"batch":        true,
				"chathistory":  true,
			},
		}
		sess.SetRegisteredForTest(true)
		if err := sess.Attach(clients[i]); err != nil {
			t.Fatal(err)
		}
		clients[i].clear()
	}

	// Simulate JOIN state so history target is known
	sess.OnMessage(nil, irc.Message{Source: "bouncernick!u@h", Command: "JOIN", Params: []string{"#busy"}})

	var wg sync.WaitGroup
	var injected atomic.Int64
	start := time.Now().UTC()
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < nMsgs/4; i++ {
				n := injected.Add(1)
				ts := start.Add(time.Duration(n) * time.Millisecond)
				msg := irc.Message{
					Tags: map[string]string{
						"time":  ts.Format("2006-01-02T15:04:05.000Z"),
						"msgid": fmt.Sprintf("m%d", n),
					},
					Source:  fmt.Sprintf("user%d!u@h", worker),
					Command: "PRIVMSG",
					Params:  []string{"#busy", fmt.Sprintf("chatter %d from w%d", n, worker)},
				}
				sess.OnMessage(nil, msg)
			}
		}(w)
	}
	wg.Wait()

	// Live clients should have received fan-out
	for i, c := range clients {
		got := c.countCmd("PRIVMSG")
		if got < nMsgs {
			t.Fatalf("client %d got %d privmsgs want >= %d", i, got, nMsgs)
		}
	}

	// New client connects and requests playback
	late := &memDL{
		id: "late",
		caps: map[string]bool{
			"message-tags": true, "server-time": true, "batch": true, "chathistory": true,
		},
	}
	if err := sess.Attach(late); err != nil {
		t.Fatal(err)
	}
	late.clear()
	err = sess.HandleClientMessage(late, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "#busy", "*", "100"},
	})
	if err != nil {
		// uplink nil is ok for CHATHISTORY path
		if late.countCmd("PRIVMSG") == 0 && late.countCmd("BATCH") == 0 {
			t.Fatalf("playback failed: %v sent=%+v", err, late.snapshot())
		}
	}
	priv := late.countCmd("PRIVMSG")
	if priv != 100 {
		t.Fatalf("playback privmsgs=%d want 100 (batch frames=%d)", priv, late.countCmd("BATCH"))
	}
	// Ensure chronological (batch contents)
	var times []string
	for _, m := range late.snapshot() {
		if m.Command == "PRIVMSG" {
			times = append(times, m.Tags["time"])
		}
	}
	for i := 1; i < len(times); i++ {
		if times[i] < times[i-1] {
			t.Fatalf("playback not sorted at %d: %s < %s", i, times[i], times[i-1])
		}
	}

	// BEFORE window
	late.clear()
	mid := times[len(times)/2]
	_ = sess.HandleClientMessage(late, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"BEFORE", "#busy", mid, "20"},
	})
	if late.countCmd("PRIVMSG") == 0 {
		t.Fatal("BEFORE returned nothing")
	}
}
