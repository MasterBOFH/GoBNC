package wire

import (
	"strings"
	"testing"
)

// The Server buffer is keyed "*", which no nick or channel can be; every other buffer is
// keyed by the network's fold. On the module API a name that folds like "Server" names the
// Server buffer (the lite protocol's buffer names are compared under CASEMAPPING).
func TestBufferKey(t *testing.T) {
	fold := strings.ToLower
	for _, tc := range []struct{ name, want string }{
		{ServerBuffer, ServerBufferKey},
		{"server", ServerBufferKey},
		{"SERVER", ServerBufferKey},
		{"#Chan", "#chan"},
		{"Bob", "bob"},
		{"Servers", "servers"},
	} {
		if got := BufferKey(fold, tc.name); got != tc.want {
			t.Errorf("BufferKey(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
	if ServerBufferKey != "*" {
		t.Fatalf("ServerBufferKey = %q, want *", ServerBufferKey)
	}
}
