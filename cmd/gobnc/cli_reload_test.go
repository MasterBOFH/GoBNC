package main

import (
	"errors"
	"testing"
)

// TestReloadShouldFallBack is the regression test for a real bug this
// change would otherwise have introduced: before runReloadHandoff existed,
// rt.Reload() only ever failed with "keeper incompatible" or "server not
// running", so cmdReload's old blanket "anything other than must-be-upgraded
// falls back to stop+spawn" logic was safe. Once Reload() gained real
// failure modes of its own (spawn failed, handoff timeout, one already in
// progress) that blanket rule would force-kill a healthy, still-serving
// old brain over a reload it had already safely declined on its own —
// exactly the opposite of runReloadHandoff's whole point. Only "no RELOAD
// support" and "not running" are legitimate reasons to fall back.
func TestReloadShouldFallBack(t *testing.T) {
	cases := []struct {
		name string
		err  string
		want bool
	}{
		{"old brain, unknown command", "unknown command", true},
		{"daemon not running", "daemon not running (no control socket at /tmp/x.sock)", true},
		{"spawn failed", "spawn failed: fork/exec /usr/bin/gobnc: no such file or directory", false},
		{"handoff timeout", "replacement did not confirm readiness within 15s", false},
		{"already in progress", "a reload is already in progress", false},
		{"child exited early", "replacement exited before confirming readiness: exit status 1", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := reloadShouldFallBack(errors.New(c.err))
			if got != c.want {
				t.Fatalf("reloadShouldFallBack(%q) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
