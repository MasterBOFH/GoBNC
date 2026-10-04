package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestAddFingerprintNormalizes: `auth add-fingerprint` is how a user pastes
// a fingerprint from openssl or a browser, which print "F1:F2:E3:…". The
// downlink matches against lowercase bare hex, so the CLI has to store that
// form, and has to refuse a value that isn't a SHA-256 at all rather than
// store a fingerprint that can never match. delete-fingerprint then takes
// the same colon form back.
func TestAddFingerprintNormalizes(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()

	colons := "AA:BB:CC:DD:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:FF"
	bare := "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aaff"
	if err := cmdAuth(ctx, st, []string{"add-fingerprint", colons, "laptop"}); err != nil {
		t.Fatal(err)
	}
	list, err := st.ListFingerprints(ctx)
	if err != nil || len(list) != 1 || list[0].Fingerprint != bare || list[0].Label != "laptop" {
		t.Fatalf("stored %+v, %v; want %q labelled laptop", list, err, bare)
	}

	if err := cmdAuth(ctx, st, []string{"add-fingerprint", "AA:BB:CC:DD", "sha1-ish"}); err == nil {
		t.Fatal("short fingerprint accepted")
	}
	if list, _ := st.ListFingerprints(ctx); len(list) != 1 {
		t.Fatalf("rejected fingerprint was stored: %+v", list)
	}

	if err := cmdAuth(ctx, st, []string{"delete-fingerprint", colons}); err != nil {
		t.Fatal(err)
	}
	if list, _ := st.ListFingerprints(ctx); len(list) != 0 {
		t.Fatalf("not deleted: %+v", list)
	}
}
