package retecloud

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Rete A869: every per-session request for a session the account cannot reach answers
// "No access to session" with code no_access and the request's session_id; the client stops
// reconnecting to the session on the code, not the text.
func TestNoAccessIsCoded(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.syncedClient("ios-1", "undernet")
	cases := []struct {
		typ  string
		data map[string]any
	}{
		{"sync_session", map[string]any{"client_id": "ios-1"}},
		{"unsync_session", map[string]any{"client_id": "ios-1"}},
		{"get_messages", map[string]any{"buffer": "#c"}},
		{"send", map[string]any{"command": "PRIVMSG #c :x"}},
		{"connect_session", map[string]any{}},
		{"disconnect_session", map[string]any{}},
	}
	for _, tc := range cases {
		mark := c.count()
		rid := c.request(tc.typ, tc.data, "net-999")
		f := c.waitFor(mark, 5*time.Second, reply(rid, tc.typ+"_success", tc.typ+"_error"))
		if f["type"] != tc.typ+"_error" || f["error"] != "No access to session" ||
			f["code"] != "no_access" || f["session_id"] != "net-999" {
			t.Errorf("%s for an unknown session: %v", tc.typ, f)
		}
	}
}

// The text goes out only through noAccessFrame or a coded error: a bare errorFrame with it
// would send it without its code.
func TestNoAccessTextOnlyCoded(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	bare := regexp.MustCompile(`errorFrame\([^\n]*"No access to session"`)
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for i, line := range strings.Split(string(src), "\n") {
			if bare.MatchString(line) {
				t.Errorf("%s:%d sends the no-access text without its code: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
	if checked == 0 {
		t.Fatal("no source files read")
	}
}
