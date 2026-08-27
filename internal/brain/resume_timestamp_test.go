package brain

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// resetStateLocked seeds registration.State.ResumeTimestamp from the config
// (the boot/restart value), but prefers the timestamp learned live on the
// connection just ending — a redial should report how fresh our view
// actually was.
func TestResetStateSeedsResumeTimestamp(t *testing.T) {
	d := NewDriver(nil)
	const id keeper.NetworkID = 1
	cfg := NetworkConfig{PrimaryNick: "n", ResumeTimestamp: "2020-01-01T00:00:00.000Z"}

	d.mu.Lock()
	d.resetStateLocked(id, cfg)
	d.mu.Unlock()
	if got := d.states[id].ResumeTimestamp; got != "2020-01-01T00:00:00.000Z" {
		t.Fatalf("no live value: seeded %q, want the config timestamp", got)
	}

	d.mu.Lock()
	d.lastServerTime[id] = "2026-08-27T18:17:03.000Z"
	d.resetStateLocked(id, cfg)
	d.mu.Unlock()
	if got := d.states[id].ResumeTimestamp; got != "2026-08-27T18:17:03.000Z" {
		t.Fatalf("live value not preferred: seeded %q, want the live @time", got)
	}
}

// handleLine records the @time tag of each parsed line for the RESUME
// timestamp on a later redial.
func TestHandleLineCapturesServerTime(t *testing.T) {
	d := NewDriver(nil)
	const id keeper.NetworkID = 1
	d.RegisterNetwork(id, NetworkConfig{PrimaryNick: "n"})

	d.handleLine(keeper.LineMsg{
		Network: id,
		Raw:     []byte("@time=2026-08-27T18:17:03.000Z :srv PRIVMSG #c :hi"),
	})
	d.mu.Lock()
	got := d.lastServerTime[id]
	d.mu.Unlock()
	if got != "2026-08-27T18:17:03.000Z" {
		t.Fatalf("lastServerTime=%q, want the @time tag", got)
	}

	// A line without @time leaves the last value unchanged.
	d.handleLine(keeper.LineMsg{Network: id, Raw: []byte(":srv PRIVMSG #c :no tag")})
	d.mu.Lock()
	got = d.lastServerTime[id]
	d.mu.Unlock()
	if got != "2026-08-27T18:17:03.000Z" {
		t.Fatalf("untagged line clobbered lastServerTime: %q", got)
	}
}
