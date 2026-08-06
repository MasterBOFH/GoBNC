package irc

import "testing"

func TestParseCHANMODESAndPREFIX(t *testing.T) {
	ms := &ModeSet{Kind: map[byte]ModeKind{}, Prefix: map[byte]byte{}, PrefixBySymbol: map[byte]byte{}}
	_ = ms.ParseCHANMODES("eIbq,k,flj,CFLMPQScgimnprstuz")
	_ = ms.ParsePREFIX("(ov)@+")
	if ms.Classify('b') != ModeList {
		t.Fatal("b")
	}
	if ms.Classify('k') != ModeArgAlways {
		t.Fatal("k")
	}
	if ms.Classify('l') != ModeArgSet {
		t.Fatal("l")
	}
	if ms.Classify('n') != ModeFlag {
		t.Fatal("n")
	}
	if ms.Classify('o') != ModePrefix || ms.Prefix['o'] != '@' {
		t.Fatal("o")
	}
}

func TestApplyModes(t *testing.T) {
	ms := DefaultModeSet()
	cm := NewChannelModes()
	changes := ms.ParseModeParams("+ntk", []string{"secret"})
	cm.Apply(ms, CaseRFC1459, changes)
	modes, args := cm.ModeString()
	if !containsMode(modes, 'n') || !containsMode(modes, 't') {
		t.Fatalf("modes=%s", modes)
	}
	if len(args) != 1 || args[0] != "secret" {
		t.Fatalf("args=%v", args)
	}
	changes = ms.ParseModeParams("+o-o", []string{"Alice", "Alice"})
	cm.Apply(ms, CaseRFC1459, changes)
	if len(cm.Nicks) != 0 {
		t.Fatalf("nicks=%v", cm.Nicks)
	}
	changes = ms.ParseModeParams("+o", []string{"Alice"})
	cm.Apply(ms, CaseRFC1459, changes)
	if !cm.Nicks[CaseRFC1459.Canonical("Alice")]['o'] {
		t.Fatal("expected +o Alice")
	}
}

func containsMode(modes string, m byte) bool {
	for i := 0; i < len(modes); i++ {
		if modes[i] == m {
			return true
		}
	}
	return false
}
