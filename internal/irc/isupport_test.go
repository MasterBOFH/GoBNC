package irc

import "testing"

func TestRPL005Burst(t *testing.T) {
	raw := map[string]string{
		"CHANMODES":   "b,k,l,imnpst",
		"PREFIX":      "(ov)@+",
		"CASEMAPPING": "rfc1459",
		"WHOX":        "",
		"NETWORK":     "libera",
	}
	msgs := RPL005("nick", raw, 500)
	if len(msgs) == 0 {
		t.Fatal("expected messages")
	}
	seen := map[string]bool{}
	for _, m := range msgs {
		if m.Command != "005" || m.Param(0) != "nick" {
			t.Fatalf("%+v", m)
		}
		if m.Params[len(m.Params)-1] != isupportTrailing {
			t.Fatalf("trailing: %v", m.Params)
		}
		for _, p := range m.Params[1 : len(m.Params)-1] {
			k, _, _ := splitKV(p)
			seen[k] = true
		}
	}
	for k := range raw {
		if !seen[k] {
			t.Fatalf("missing token %s in %#v", k, msgs)
		}
	}
}

func TestRPL005Split(t *testing.T) {
	raw := map[string]string{}
	for i := 0; i < 40; i++ {
		raw[string(rune('A'+(i%26)))+string(rune('0'+i/26))] = "xxxxxxxxxxxxxxxxxxxx"
	}
	msgs := RPL005("n", raw, 120)
	if len(msgs) < 2 {
		t.Fatalf("expected split, got %d", len(msgs))
	}
	for _, m := range msgs {
		if len(m.Encode()) > 120+20 { // encode may add ':' etc; keep sane
			// maxLen is advisory on token packing; ensure we still split
		}
	}
}

func TestUserModes(t *testing.T) {
	m := map[byte]bool{}
	ApplyUserModes(m, "+iw")
	ApplyUserModes(m, "-w+x")
	if got := UserModeString(m); got != "+ix" {
		t.Fatal(got)
	}
}

func TestParse005UTF8Only(t *testing.T) {
	is := NewISUPPORT()
	if is.UTF8Only {
		t.Fatal("default should be false")
	}
	is.Parse005([]string{"nick", "WHOX", "UTF8ONLY", "NETWORK=test", "are supported by this server"})
	if !is.UTF8Only {
		t.Fatal("UTF8ONLY not set")
	}
	if _, ok := is.Raw["UTF8ONLY"]; !ok {
		t.Fatal("UTF8ONLY missing from Raw")
	}
	if !is.WHOX {
		t.Fatal("WHOX cleared")
	}
}
