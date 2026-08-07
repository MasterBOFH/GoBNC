package auth

import "testing"

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
