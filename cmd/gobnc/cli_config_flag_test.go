package main

import (
	"reflect"
	"testing"
)

// TestSplitConfigFlag proves runCLI strips "-config <path>" out of the
// argument list before dispatching to a subcommand. Before this, the scan
// only read the path and left the pair in place, so every subcommand saw
// it as its own argument: auth set-password refused with a usage error
// (it insists on no arguments), and auth add-fingerprint would have stored
// "-config <path>" as the label. On a packaged install gobnc.json lives in
// /etc/gobnc, so without the flag set-password silently created a fresh
// gobnc.db in the current directory and wrote the password there.
func TestSplitConfigFlag(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantPath string
		wantRest []string
	}{
		{"absent", []string{"auth", "set-password"}, "gobnc.json", []string{"auth", "set-password"}},
		{"trailing", []string{"auth", "set-password", "-config", "/etc/gobnc/gobnc.json"}, "/etc/gobnc/gobnc.json", []string{"auth", "set-password"}},
		{"leading", []string{"-config", "x.json", "network", "list"}, "x.json", []string{"network", "list"}},
		{"middle", []string{"auth", "-config", "x.json", "add-fingerprint", "ab", "laptop"}, "x.json", []string{"auth", "add-fingerprint", "ab", "laptop"}},
		{"dangling flag kept", []string{"status", "-config"}, "gobnc.json", []string{"status", "-config"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path, rest := splitConfigFlag(c.args)
			if path != c.wantPath {
				t.Errorf("path = %q, want %q", path, c.wantPath)
			}
			if !reflect.DeepEqual(rest, c.wantRest) {
				t.Errorf("rest = %q, want %q", rest, c.wantRest)
			}
		})
	}
}
