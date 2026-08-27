package registration

import (
	"reflect"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// No reachable ircd still ships draft/resume-0.5 (Ergo removed it in
// 2.6.0), so these drive Step against the wire shapes Oragono ≤ 2.5.1
// actually produced, per the spec text on the master+resume branch. A
// real capture against oragono/oragono:v2.5.1 belongs in
// testdata/registration/ as a follow-up — prefer it over these the moment
// it exists (docs/keeper-design.md's synthetic-only-as-fallback rule).

func hasActionKind(acts []Action, k ActionKind) bool {
	for _, a := range acts {
		if a.Kind == k {
			return true
		}
	}
	return false
}

func findAction(acts []Action, k ActionKind) (Action, bool) {
	for _, a := range acts {
		if a.Kind == k {
			return a, true
		}
	}
	return Action{}, false
}

func TestResumeCapRequestedEvenWithoutToken(t *testing.T) {
	s := New("nick", "", false, SASLConfig{})
	_, acts := step(t, s, ":irc.example CAP nick LS :multi-prefix draft/resume-0.5")
	req := lastSendLine(t, acts)
	if !strings.HasPrefix(req, "CAP REQ") || !strings.Contains(req, ResumeCap) {
		t.Fatalf("got %q, want a CAP REQ including %s (negotiating it is how a token gets issued)", req, ResumeCap)
	}
}

func TestResumeTokenIssuedIsSurfacedNotSent(t *testing.T) {
	s := New("nick", "", false, SASLConfig{})
	s, _ = step(t, s, ":irc.example CAP nick LS :draft/resume-0.5")
	s, acts := step(t, s, ":irc.example CAP nick ACK :draft/resume-0.5")
	if got := lastSendLine(t, acts); got != "CAP END" {
		t.Fatalf("no token to present: got %q, want CAP END", got)
	}
	s, acts = step(t, s, ":irc.example RESUME TOKEN JKbAypzFiovffzuD8VEfcs6bOLrXsSenxsyZNt8")
	a, ok := findAction(acts, ActionResumeToken)
	if !ok || a.Token != "JKbAypzFiovffzuD8VEfcs6bOLrXsSenxsyZNt8" {
		t.Fatalf("acts=%+v, want ActionResumeToken carrying the issued token", acts)
	}
	if s.IssuedToken != a.Token {
		t.Fatalf("IssuedToken=%q, want %q", s.IssuedToken, a.Token)
	}
	if len(sendLines(acts)) != 0 {
		t.Fatalf("RESUME TOKEN must not make the client send anything, got %v", sendLines(acts))
	}
	if s.ResumeToken != "" {
		t.Fatalf("ResumeToken=%q: the issued token must not be presented on the connection that issued it", s.ResumeToken)
	}
}

// resumeToACK drives a token-holding state through LS/ACK (sasl offered
// and wanted, to prove resume wins over SASL) and returns the state in
// PhaseResuming plus the ACK's actions.
func resumeToACK(t *testing.T, sasl SASLConfig) (State, []Action) {
	t.Helper()
	s := New("nick", "", false, sasl)
	s.ResumeToken = "A8KgnZPYDaRiGMzZWLu2frVvtN7lbCxO3hTwGLO"
	s, acts := step(t, s, ":irc.example CAP nick LS :multi-prefix draft/resume-0.5 sasl=PLAIN")
	if req := lastSendLine(t, acts); !strings.Contains(req, ResumeCap) {
		t.Fatalf("CAP REQ %q lacks %s", req, ResumeCap)
	}
	s, acts = step(t, s, ":irc.example CAP nick ACK :multi-prefix draft/resume-0.5 sasl")
	return s, acts
}

func TestResumeHappyPath(t *testing.T) {
	sasl := SASLConfig{Wanted: true, User: "alice", Pass: "hunter2"}
	s, acts := resumeToACK(t, sasl)
	if s.Phase != PhaseResuming {
		t.Fatalf("phase=%v after ACK, want resuming", s.Phase)
	}
	if got := sendLines(acts); !reflect.DeepEqual(got, []string{"RESUME A8KgnZPYDaRiGMzZWLu2frVvtN7lbCxO3hTwGLO"}) {
		t.Fatalf("ACK sent %v, want exactly the RESUME line (no AUTHENTICATE, no CAP END)", got)
	}

	// The new connection's own token crosses our RESUME on the wire.
	s, acts = step(t, s, ":irc.example RESUME TOKEN newtoken")
	if !hasActionKind(acts, ActionResumeToken) || s.IssuedToken != "newtoken" {
		t.Fatalf("issued token not surfaced: acts=%+v state.IssuedToken=%q", acts, s.IssuedToken)
	}
	if s.ResumeToken != "A8KgnZPYDaRiGMzZWLu2frVvtN7lbCxO3hTwGLO" {
		t.Fatalf("issuing a new token clobbered the one being presented: %q", s.ResumeToken)
	}

	s, acts = step(t, s, ":irc.example RESUME SUCCESS dan")
	if len(acts) != 0 {
		t.Fatalf("RESUME SUCCESS must be silent (no CAP END, no SASL), got %+v", acts)
	}
	if s.Phase != PhaseAwaitingWelcome || !s.Resumed || s.Nick != "dan" {
		t.Fatalf("after SUCCESS: phase=%v resumed=%v nick=%q, want awaiting_welcome/true/dan", s.Phase, s.Resumed, s.Nick)
	}

	s, _ = step(t, s, ":irc.example 001 dan :Welcome back")
	s, acts = step(t, s, ":irc.example 376 dan :End of MOTD")
	if s.Phase != PhaseComplete {
		t.Fatalf("phase=%v, want complete", s.Phase)
	}
	reg, ok := findAction(acts, ActionRegistered)
	if !ok || !reg.Resumed {
		t.Fatalf("acts=%+v, want ActionRegistered with Resumed=true", acts)
	}
}

func TestResumeSendsTimestampWhenKnown(t *testing.T) {
	s := New("nick", "", false, SASLConfig{})
	s.ResumeToken = "tok"
	s.ResumeTimestamp = "2017-04-13T15:12:51.620Z"
	s, _ = step(t, s, ":irc.example CAP nick LS :draft/resume-0.5")
	_, acts := step(t, s, ":irc.example CAP nick ACK :draft/resume-0.5")
	if got := lastSendLine(t, acts); got != "RESUME tok 2017-04-13T15:12:51.620Z" {
		t.Fatalf("got %q, want RESUME with the timestamp as second param", got)
	}
}

func TestResumeFailFallsBackToSASL(t *testing.T) {
	sasl := SASLConfig{Wanted: true, User: "alice", Pass: "hunter2"}
	s, _ := resumeToACK(t, sasl)
	s, acts := step(t, s, ":irc.example FAIL RESUME INVALID_TOKEN :Cannot resume connection, token is not valid")
	if s.Phase != PhaseAuthenticating {
		t.Fatalf("phase=%v after FAIL RESUME with sasl ACK'd, want authenticating", s.Phase)
	}
	if got := lastSendLine(t, acts); got != "AUTHENTICATE PLAIN" {
		t.Fatalf("got %q, want the SASL exchange to start exactly as it would without resume", got)
	}
	if s.ResumeToken != "" {
		t.Fatalf("rejected token still held: %q", s.ResumeToken)
	}
	s, _ = step(t, s, "AUTHENTICATE +")
	s, acts = step(t, s, ":irc.example 903 nick :SASL authentication successful")
	if got := lastSendLine(t, acts); got != "CAP END" {
		t.Fatalf("got %q, want CAP END after SASL", got)
	}
	s, _ = step(t, s, ":irc.example 001 nick :Welcome")
	s, acts = step(t, s, ":irc.example 376 nick :End of MOTD")
	reg, ok := findAction(acts, ActionRegistered)
	if !ok || reg.Resumed || s.Resumed {
		t.Fatalf("a failed resume must register fresh: acts=%+v state.Resumed=%v", acts, s.Resumed)
	}
}

func TestResumeFailWithoutSASLSendsCapEnd(t *testing.T) {
	s, _ := resumeToACK(t, SASLConfig{})
	s, acts := step(t, s, ":irc.example FAIL RESUME CANNOT_RESUME :Cannot resume connection, session not found")
	if s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("phase=%v, want awaiting_welcome", s.Phase)
	}
	if got := sendLines(acts); !reflect.DeepEqual(got, []string{"CAP END"}) {
		t.Fatalf("got %v, want exactly CAP END", got)
	}
}

func TestResumeNotAttemptedWhenCapNotOffered(t *testing.T) {
	s := New("nick", "", false, SASLConfig{})
	s.ResumeToken = "tok"
	s, acts := step(t, s, ":irc.example CAP nick LS :multi-prefix server-time")
	if req := lastSendLine(t, acts); strings.Contains(req, ResumeCap) {
		t.Fatalf("requested an unoffered cap: %q", req)
	}
	s, acts = step(t, s, ":irc.example CAP nick ACK :server-time")
	if got := lastSendLine(t, acts); got != "CAP END" {
		t.Fatalf("got %q, want CAP END — no RESUME without the cap ACK'd", got)
	}
	if s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("phase=%v, want awaiting_welcome", s.Phase)
	}
}

func TestResumeSuccessIgnoredWhenNotResuming(t *testing.T) {
	s := New("nick", "", false, SASLConfig{})
	s, _ = step(t, s, ":irc.example CAP nick LS :draft/resume-0.5")
	s, _ = step(t, s, ":irc.example CAP nick ACK :draft/resume-0.5")
	before := s
	s, acts := step(t, s, ":irc.example RESUME SUCCESS somebody")
	if len(acts) != 0 || s.Resumed || s.Nick != before.Nick || s.Phase != before.Phase {
		t.Fatalf("a stray RESUME SUCCESS changed state: acts=%+v before=%+v after=%+v", acts, before, s)
	}
	// Likewise a FAIL for something else entirely is passthrough.
	s, acts = step(t, s, ":irc.example FAIL BRB CANNOT_BRB :nope")
	if len(acts) != 0 || s.Phase != before.Phase {
		t.Fatalf("unrelated FAIL changed state: acts=%+v phase=%v", acts, s.Phase)
	}
}

// A resume attempt guarantees a 433: the old session still holds the
// nick. With nick recovery off, the ladder has exactly one rung, so
// without deferral the very first 433 — which arrives before CAP ACK,
// since NICK goes out in Start — would fail registration before RESUME
// was ever sent, and the redial loop would never resume anything.
func TestResumeDefersNickExhaustionUntilOutcome(t *testing.T) {
	s := New("dan", "", false, SASLConfig{}) // nickRecovery=false: one rung
	s.ResumeToken = "tok"
	s, acts := step(t, s, ":irc.example CAP dan LS :draft/resume-0.5")
	_ = acts
	s, acts = step(t, s, ":irc.example 433 * dan :Nickname is already in use")
	if hasActionKind(acts, ActionFailed) || s.Phase == PhaseFailed {
		t.Fatalf("433 failed registration while a resume was still possible: acts=%+v phase=%v", acts, s.Phase)
	}
	s, acts = step(t, s, ":irc.example CAP dan ACK :draft/resume-0.5")
	if got := lastSendLine(t, acts); !strings.HasPrefix(got, "RESUME tok") {
		t.Fatalf("got %q, want RESUME despite the earlier 433", got)
	}
	s, _ = step(t, s, ":irc.example RESUME SUCCESS dan")
	if s.Nick != "dan" || s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("nick=%q phase=%v, want dan/awaiting_welcome", s.Nick, s.Phase)
	}
	s, _ = step(t, s, ":irc.example 001 dan :Welcome back")
	_, acts = step(t, s, ":irc.example 376 dan :End of MOTD")
	if !hasActionKind(acts, ActionRegistered) {
		t.Fatalf("acts=%+v, want ActionRegistered", acts)
	}
}

func TestResumeRuledOutSurfacesDeferredNickFailure(t *testing.T) {
	wantErr := "nick error: 433"
	cases := []struct {
		name  string
		lines []string // after the 433, ending with whatever rules resume out
	}{
		{"FAIL RESUME", []string{
			":irc.example CAP dan LS :draft/resume-0.5",
			":irc.example CAP dan ACK :draft/resume-0.5",
			":irc.example FAIL RESUME INVALID_TOKEN :Cannot resume connection, token is not valid",
		}},
		{"cap not offered", []string{
			":irc.example CAP dan LS :multi-prefix",
		}},
		{"cap NAK", []string{
			":irc.example CAP dan LS :draft/resume-0.5",
			":irc.example CAP dan NAK :draft/resume-0.5",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New("dan", "", false, SASLConfig{})
			s.ResumeToken = "tok"
			s, acts := step(t, s, ":irc.example 433 * dan :Nickname is already in use")
			if hasActionKind(acts, ActionFailed) {
				t.Fatalf("failed before resume was ruled out: %+v", acts)
			}
			var last []Action
			for _, l := range tc.lines[:len(tc.lines)-1] {
				s, last = step(t, s, l)
				if hasActionKind(last, ActionFailed) {
					t.Fatalf("failed early on %q: %+v", l, last)
				}
			}
			s, last = step(t, s, tc.lines[len(tc.lines)-1])
			f, ok := findAction(last, ActionFailed)
			if !ok || s.Phase != PhaseFailed {
				t.Fatalf("acts=%+v phase=%v, want ActionFailed once resume is ruled out", last, s.Phase)
			}
			if f.Err == nil || !strings.HasPrefix(f.Err.Error(), wantErr) {
				t.Fatalf("Err=%v, want the deferred %q so HandleDisconnect relays the numeric", f.Err, wantErr)
			}
			if containsSendLine(last, "CAP END") || containsSendLine(last, "AUTHENTICATE") {
				t.Fatalf("kept registering after failing: %v", sendLines(last))
			}
		})
	}
}

func TestResumeNickExhaustionStillFatalWithoutToken(t *testing.T) {
	s := New("dan", "", false, SASLConfig{}) // no ResumeToken
	s, acts := step(t, s, ":irc.example 433 * dan :Nickname is already in use")
	if !hasActionKind(acts, ActionFailed) || s.Phase != PhaseFailed {
		t.Fatalf("without a token the ladder exhaustion must fail as before: acts=%+v phase=%v", acts, s.Phase)
	}
}

// The property TestReplayIdenticalToLive proves for the real transcript
// corpus, checked for the synthetic resume sequences too: Step's control
// flow never looks at Input.Replay.
func TestResumeReplayIdenticalToLive(t *testing.T) {
	sequences := map[string][]string{
		"success": {
			":irc.example CAP dan LS :draft/resume-0.5 sasl=PLAIN",
			":irc.example 433 * dan :Nickname is already in use",
			":irc.example CAP dan ACK :draft/resume-0.5 sasl",
			":irc.example RESUME TOKEN newtoken",
			":irc.example RESUME SUCCESS dan",
			":irc.example 001 dan :Welcome back",
			":irc.example 376 dan :End of MOTD",
		},
		"fail": {
			":irc.example CAP dan LS :draft/resume-0.5 sasl=PLAIN",
			":irc.example CAP dan ACK :draft/resume-0.5 sasl",
			":irc.example RESUME TOKEN newtoken",
			":irc.example FAIL RESUME INVALID_TOKEN :nope",
			"AUTHENTICATE +",
			":irc.example 903 dan :SASL authentication successful",
			":irc.example 001 dan :Welcome",
			":irc.example 376 dan :End of MOTD",
		},
	}
	run := func(lines []string, replay bool) (State, []Action) {
		s := New("dan", "", true, SASLConfig{Wanted: true, User: "alice", Pass: "hunter2"})
		s.ResumeToken = "tok"
		var all []Action
		for _, l := range lines {
			msg, err := irc.Parse(l)
			if err != nil {
				t.Fatalf("parse %q: %v", l, err)
			}
			var acts []Action
			s, acts = Step(s, Input{Msg: msg, Replay: replay})
			for _, a := range acts {
				if a.Replay != replay {
					t.Fatalf("action %+v carries Replay=%v, input was %v", a, a.Replay, replay)
				}
				a.Replay = false
				all = append(all, a)
			}
		}
		return s, all
	}
	for name, lines := range sequences {
		t.Run(name, func(t *testing.T) {
			live, liveActs := run(lines, false)
			rep, repActs := run(lines, true)
			if !reflect.DeepEqual(live, rep) {
				t.Fatalf("state diverged:\nlive=%+v\nreplay=%+v", live, rep)
			}
			if !reflect.DeepEqual(liveActs, repActs) {
				t.Fatalf("actions diverged:\nlive=%+v\nreplay=%+v", liveActs, repActs)
			}
			if live.Phase != PhaseComplete {
				t.Fatalf("phase=%v, want complete", live.Phase)
			}
		})
	}
}
