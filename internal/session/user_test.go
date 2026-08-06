package session

import "testing"

func TestUserPrefixAndFlags(t *testing.T) {
	u := &User{Nick: "n"}
	if u.Prefix() != "n" {
		t.Fatal(u.Prefix())
	}
	u.UpdateFromPrefix("n!~u@h")
	if u.Prefix() != "n!~u@h" {
		t.Fatal(u.Prefix())
	}
	u.ApplyWHOFlags("G*@Bs")
	if !u.Away || !u.Oper || !u.Bot || !u.Secure {
		t.Fatalf("%+v", u)
	}
	u.ApplyWHOFlags("H")
	if u.Away {
		t.Fatal("expected here")
	}
	u.ApplyUModes("+iZ")
	if u.UModeString() != "+iZ" && u.UModeString() != "+Zi" {
		// sorted
		if got := u.UModeString(); got != "+Zi" && got != "+iZ" {
			t.Fatal(got)
		}
	}
	if !u.Secure {
		t.Fatal("Z should set secure")
	}
}
