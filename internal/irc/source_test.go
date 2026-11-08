package irc

import "testing"

func TestServerSource(t *testing.T) {
	for src, want := range map[string]bool{
		"":                   true,
		"irc.example.net":    true,
		"services.":          true,
		"nick!user@host":     false,
		"nick@host":          false,
		"Server":             false, // a bare nick prefix
		"Server!s@h.example": false,
	} {
		if got := ServerSource(src); got != want {
			t.Errorf("ServerSource(%q) = %v, want %v", src, got, want)
		}
	}
}

func TestIsServerName(t *testing.T) {
	for target, want := range map[string]bool{
		"irc.example.net":         true,
		"127.0.0.1":               true,
		"":                        false,
		"server":                  false, // a nick
		"bob":                     false,
		"#rete.dev":               false, // a channel
		"&local.chan":             false,
		"$*.example.net":          false, // a mask
		"x@channels.undernet.org": false, // user@server addressing
		"n!u@h.example":           false,
	} {
		if got := IsServerName(target); got != want {
			t.Errorf("IsServerName(%q) = %v, want %v", target, got, want)
		}
	}
}
