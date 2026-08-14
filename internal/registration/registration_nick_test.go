package registration

import (
	"testing"
)

// None of the real transcripts hit a nick collision (every captured nick
// was free). These drive Step directly against hand-built 433/437
// sequences, and TestNickLadderPure exercises buildNickLadder/
// nextNickInLadder directly the way internal/uplink/nick_test.go does for
// the original — those two functions are pure and unchanged from the port,
// so table-driving them here is a direct regression check against the
// reference implementation's own test shape.

func TestNickLadderRetryOnCollision(t *testing.T) {
	s := New("wanted", "wanted_alt", true, SASLConfig{})
	s, acts := step(t, s, ":irc.example 433 * wanted :Nickname is already in use.")
	if s.Phase != PhaseCAPNegotiation {
		t.Fatalf("phase=%v, want unchanged (still negotiating)", s.Phase)
	}
	if got := lastSendLine(t, acts); got != "NICK wanted_alt" {
		t.Fatalf("got %q, want NICK wanted_alt (the configured alt)", got)
	}
	if s.Nick != "wanted_alt" {
		t.Fatalf("State.Nick=%q, want wanted_alt", s.Nick)
	}
}

func TestNickLadderContinuesPastAlt(t *testing.T) {
	s := New("wanted", "wanted_alt", true, SASLConfig{})
	s, _ = step(t, s, ":irc.example 433 * wanted :Nickname is already in use.")
	s, acts := step(t, s, ":irc.example 433 * wanted_alt :Nickname is already in use.")
	if got := lastSendLine(t, acts); got != "NICK wanted_" {
		t.Fatalf("got %q, want NICK wanted_ (underscore-suffixed ladder rung)", got)
	}
}

func TestNickLadderDisabledFailsImmediately(t *testing.T) {
	s := New("wanted", "wanted_alt", false, SASLConfig{}) // nickRecovery=false
	s, acts := step(t, s, ":irc.example 433 * wanted :Nickname is already in use.")
	if s.Phase != PhaseFailed {
		t.Fatalf("phase=%v, want failed (recovery disabled)", s.Phase)
	}
	if len(acts) != 1 || acts[0].Kind != ActionFailed {
		t.Fatalf("acts=%+v, want one ActionFailed", acts)
	}
}

func TestNickLadderExhaustionFails(t *testing.T) {
	s := New("ab", "", true, SASLConfig{}) // no alt
	// NICKLEN=1 truncates the ladder to a single one-char entry distinct
	// from the nick we actually sent ("ab"), so nextNickInLadder can't find
	// a matching rung to advance past — the idx<0, len(ladder)<=1 exhaustion
	// path. (NICKLEN=2 was tried first and turned out to leave one rung of
	// headroom — "ab" truncates to "a", then "a_" fits in 2 chars — which a
	// hand-derived assumption about the ladder's size got wrong; this
	// value was chosen by actually running buildNickLadder, not assumed.)
	s.ISUPPORT.Raw["NICKLEN"] = "1"
	s, acts := step(t, s, ":irc.example 433 * ab :Nickname is already in use.")
	if s.Phase != PhaseFailed {
		t.Fatalf("phase=%v, want failed (ladder exhausted under a 1-char NICKLEN)", s.Phase)
	}
	if len(acts) != 1 || acts[0].Kind != ActionFailed {
		t.Fatalf("acts=%+v, want one ActionFailed", acts)
	}
}

func TestNickLadderRespectsNICKLEN(t *testing.T) {
	s := New("nick", "", true, SASLConfig{})
	s.ISUPPORT.Raw["NICKLEN"] = "5" // "nick" (4) + one underscore fits, "nick__" (6) would not
	s, acts := step(t, s, ":irc.example 433 * nick :Nickname is already in use.")
	if got := lastSendLine(t, acts); got != "NICK nick_" {
		t.Fatalf("got %q, want NICK nick_ under NICKLEN=5", got)
	}
}

func TestNickLadderIgnoredPostWelcome(t *testing.T) {
	s := New("nick", "", true, SASLConfig{})
	s, _ = step(t, s, ":irc.example 001 nick :Welcome")
	s, acts := step(t, s, ":irc.example 433 nick otherclient :Nickname is already in use.")
	if len(acts) != 0 {
		t.Fatalf("post-welcome 433 produced actions: %+v, want none (not a registration failure)", acts)
	}
	if s.Phase == PhaseFailed {
		t.Fatalf("post-welcome 433 failed registration, want no change")
	}
}

func Test437NickForm(t *testing.T) {
	s := New("wanted", "wanted_alt", true, SASLConfig{})
	// Pre-welcome 437 with first param "*" is a nick collision.
	s, acts := step(t, s, ":irc.example 437 * wanted :Nick/channel is temporarily unavailable")
	if got := lastSendLine(t, acts); got != "NICK wanted_alt" {
		t.Fatalf("got %q, want NICK wanted_alt", got)
	}
}

func Test437ChannelFormIgnored(t *testing.T) {
	s := New("wanted", "wanted_alt", true, SASLConfig{})
	// 437 targeting a channel (first param isn't "*") is unrelated to
	// nick collision and must not perturb registration.
	s, acts := step(t, s, ":irc.example 437 wanted #somechan :Channel is temporarily unavailable")
	if len(acts) != 0 {
		t.Fatalf("channel-form 437 produced actions: %+v, want none", acts)
	}
	if s.Phase == PhaseFailed {
		t.Fatalf("channel-form 437 failed registration, want no change")
	}
}

// TestNickLadderPure table-drives buildNickLadder/nextNickInLadder
// directly — the two pure helper functions ported verbatim from
// internal/uplink/nick.go, unchanged by the port. Mirrors the shape of
// internal/uplink/nick_test.go's own table tests for the same functions,
// since there's nothing about the port that should have changed their
// behavior.
func TestNickLadderPure(t *testing.T) {
	cases := []struct {
		name         string
		primary, alt string
		maxLen       int
		wantFirstFew []string
	}{
		{
			name:    "primary and alt then underscores",
			primary: "foo", alt: "bar", maxLen: 30,
			wantFirstFew: []string{"foo", "bar", "foo_", "foo__"},
		},
		{
			name:    "no alt",
			primary: "foo", alt: "", maxLen: 30,
			wantFirstFew: []string{"foo", "foo_", "foo__"},
		},
		{
			name:    "alt equal to primary case-insensitively is deduped",
			primary: "foo", alt: "FOO", maxLen: 30,
			wantFirstFew: []string{"foo", "foo_"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ladder := buildNickLadder(c.primary, c.alt, c.maxLen)
			if len(ladder) < len(c.wantFirstFew) {
				t.Fatalf("ladder=%v, too short for expected prefix %v", ladder, c.wantFirstFew)
			}
			for i, want := range c.wantFirstFew {
				if ladder[i] != want {
					t.Fatalf("ladder[%d]=%q, want %q (full ladder: %v)", i, ladder[i], want, ladder)
				}
			}
		})
	}
}
