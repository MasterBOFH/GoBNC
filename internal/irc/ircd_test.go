package irc

import "testing"

func TestDetectIRCd(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"Your host is x running version InspIRCd-3", IRCdInspIRCd},
		{"running UnrealIRCd-6.1", IRCdUnreal},
		{"Unreal-6.0", IRCdUnreal},
		{"solanum-1.0", IRCdSolanum},
		{"charybdis-4", IRCdCharybdis},
		{"ircd-ratbox-3", IRCdRatbox},
		{"ircd-hybrid-8", IRCdHybrid},
		{"bahamut-2", IRCdBahamut},
		{"ngircd-26", IRCdNgIRCd},
		{"ergo-2.1", IRCdErgo},
		{"snircd-1.3", IRCdSnircd},
		{"u2.10.12.19", IRCdIrcu},
		{"u2.10.13.0", IRCdIrcu},
		{"Undernet u2.10.12.19", IRCdIrcu},
		{"Your host is ircd.example.irc.com, running version 2.11.2p3", IRCdIrc2},
		{"2.11.2p3", IRCdIrc2},
		{"something else", IRCdUnknown},
	}
	for _, tc := range cases {
		if got := DetectIRCd(tc.in); got != tc.want {
			t.Errorf("DetectIRCd(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestMAPCodesPerIRCd(t *testing.T) {
	ircuEnd := MAPEndCodes(IRCdIrcu)
	if !ircuEnd["017"] || ircuEnd["007"] {
		t.Fatalf("ircu ends: %v", ircuEnd)
	}
	unrealEnd := MAPEndCodes(IRCdUnreal)
	if !unrealEnd["007"] || unrealEnd["017"] {
		t.Fatalf("unreal ends: %v", unrealEnd)
	}
	ircuRepl := MAPReplyCodes(IRCdIrcu)
	if !ircuRepl["015"] || ircuRepl["006"] {
		t.Fatalf("ircu replies should use 015 not 006: %v", ircuRepl)
	}
}

func TestIsWHOISReplyPerIRCd(t *testing.T) {
	// Core always.
	for _, n := range []string{"311", "318", "401"} {
		if !IsWHOISReply(n, IRCdIrcu) || !IsWHOISReply(n, IRCdUnreal) {
			t.Fatalf("core %s", n)
		}
	}
	// 307 is WHOISREGNICK on Unreal, not on ircu.
	if IsWHOISReply("307", IRCdIrcu) {
		t.Fatal("307 should not be WHOIS on ircu")
	}
	if !IsWHOISReply("307", IRCdUnreal) {
		t.Fatal("307 should be WHOIS on unreal")
	}
	// 330 account is ircu / ratbox-family, not unreal-primary list (unreal uses other).
	if !IsWHOISReply("330", IRCdIrcu) {
		t.Fatal("330 whois account on ircu")
	}
	if !IsWHOISReply("671", IRCdIrcu) {
		t.Fatal("671 whoissecure on ircu (u2.10.13+)")
	}
	if IsWHOISReply("378", IRCdIrcu) {
		t.Fatal("378 whoishost is unreal-specific")
	}
	if !IsWHOISReply("378", IRCdUnreal) {
		t.Fatal("378 on unreal")
	}
}
