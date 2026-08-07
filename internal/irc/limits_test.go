package irc

import "testing"

func TestLimitsConstants(t *testing.T) {
	if MaxClientLine != 4608 || MaxServerLine != 8703 {
		t.Fatalf("MaxClientLine=%d MaxServerLine=%d", MaxClientLine, MaxServerLine)
	}
}

func TestInputTooLong(t *testing.T) {
	m := InputTooLong("alice")
	if m.Command != ErrInputTooLong || m.Param(0) != "alice" || m.Trailing() != "Input line was too long" {
		t.Fatalf("%+v", m)
	}
	if InputTooLong("").Param(0) != "*" {
		t.Fatal("empty nick")
	}
}

func TestHasRawCRLF(t *testing.T) {
	ok := Message{Command: "PRIVMSG", Params: []string{"#c", "hi"}}
	if HasRawCRLF(ok) {
		t.Fatal("clean message")
	}
	bad := Message{Command: "PRIVMSG", Params: []string{"#c", "x\rPRIVMSG y :z"}}
	if !HasRawCRLF(bad) {
		t.Fatal("expected CR detection")
	}
	badTag := Message{Command: "TAGMSG", Params: []string{"#c"}, Tags: map[string]string{"+x": "a\nb"}}
	if !HasRawCRLF(badTag) {
		t.Fatal("expected tag LF detection")
	}
}
