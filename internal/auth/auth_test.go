package auth

import (
	"strings"
	"testing"
)

func TestHashVerify(t *testing.T) {
	h, err := HashPassword("secret")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword(h, "secret") {
		t.Fatal("verify failed")
	}
	if VerifyPassword(h, "wrong") {
		t.Fatal("should fail")
	}
}

func TestGeneratePassword(t *testing.T) {
	a, err := GeneratePassword(0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := GeneratePassword(0)
	if err != nil {
		t.Fatal(err)
	}
	if a == "" || a == b {
		t.Fatalf("expected distinct non-empty passwords, got %q %q", a, b)
	}
	if len(a) < 20 {
		t.Fatalf("too short: %q", a)
	}
	h, err := HashPassword(a)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword(h, a) {
		t.Fatal("generated password should verify")
	}
}

// TestNormalizeFingerprint: add-fingerprint stores whatever it's given, and
// the downlink compares against lowercase bare hex, so a fingerprint pasted
// in the "F1:F2:E3:…" form every cert tool prints would never match. It
// must come out as bare lowercase hex, and anything that isn't a SHA-256
// must be refused rather than stored.
func TestNormalizeFingerprint(t *testing.T) {
	bare := "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aaff"
	colons := "AA:BB:CC:DD:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:FF"
	ok := map[string]string{
		"bare lowercase":    bare,
		"bare uppercase":    strings.ToUpper(bare),
		"colons uppercase":  colons,
		"colons lowercase":  strings.ToLower(colons),
		"surrounding space": "  " + colons + "\n",
		"space separated":   strings.ReplaceAll(colons, ":", " "),
	}
	for name, in := range ok {
		got, err := NormalizeFingerprint(in)
		if err != nil || got != bare {
			t.Errorf("%s: NormalizeFingerprint(%q) = %q, %v; want %q", name, in, got, err, bare)
		}
	}
	bad := map[string]string{
		"sha1 length":     "AA:BB:CC:DD:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF",
		"one digit short": bare[:63],
		"not hex":         strings.Replace(bare, "a", "g", 1),
		"empty":           "",
		"sha256 prefix":   "sha256:" + bare,
	}
	for name, in := range bad {
		if got, err := NormalizeFingerprint(in); err == nil {
			t.Errorf("%s: NormalizeFingerprint(%q) = %q, want error", name, in, got)
		}
	}
}
