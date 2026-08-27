package registration

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// transcriptDir holds real captured registration sequences — see the
// package doc and the capture tool used to produce them. Each file is one
// real ircd's actual registration traffic, not synthesized.
const transcriptDir = "../../testdata/registration"

// loadTranscript reads one fixture file, stripping the "# ..." header
// comment and each line's "<ms>ms " capture-time prefix, returning the raw
// IRC lines in the order the server sent them.
func loadTranscript(t *testing.T, name string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(transcriptDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var lines []string
	for _, raw := range strings.Split(string(data), "\n") {
		raw = strings.TrimRight(raw, "\r")
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		_, line, ok := strings.Cut(raw, "ms ")
		if !ok {
			t.Fatalf("%s: malformed fixture line: %q", name, raw)
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		t.Fatalf("%s: no lines loaded", name)
	}
	return lines
}

// runTranscript begins at Start — the opening CAP LS/NICK/USER a real
// client sends before the server has said anything — then drives Step over
// every captured line in order, returning the final state and the full,
// in-order action log including Start's own actions. replay is threaded
// through to both: on a replay run Start correctly contributes nothing
// (see Start's doc comment), so replayActions is exactly Step's output.
func runTranscript(t *testing.T, lines []string, nick string, replay bool) (State, []Action) {
	t.Helper()
	s := New(nick, "", false, SASLConfig{}) // none of the captured transcripts use SASL
	allActions := Start(nick, "", "", "", replay)
	for i, line := range lines {
		msg, err := irc.Parse(line)
		if err != nil {
			t.Fatalf("line %d: parse %q: %v", i, line, err)
		}
		var acts []Action
		s, acts = Step(s, Input{Msg: msg, Replay: replay})
		allActions = append(allActions, acts...)
	}
	return s, allActions
}

// wantOpeningActions is what Start(nick, "", "", "", false) always
// produces for these fixtures — none use PASS or a non-default USER line.
func wantOpeningActions(nick string) []Action {
	return []Action{
		{Kind: ActionSend, Line: "CAP LS 302"},
		{Kind: ActionSend, Line: "NICK " + nick},
		{Kind: ActionSend, Line: "USER gobnc 0 * :GoBNC"},
	}
}

// registrationFixtures is every real transcript this state machine is
// verified against. Every one of these reached 001 and completed
// registration (376 or 422) when captured; that's the property under test.
var registrationFixtures = []struct {
	file string
	nick string
}{
	{"ergo.txt", "gobnccap1"},
	{"remote.txt", "gobnccap2"},
	{"ircu2.txt", "gc25094"},
	{"unreal4.txt", "gc7252"},
	{"hybrid.txt", "gc3477"},
	{"inspircd.txt", "gc16972"},
	{"ngircd.txt", "gc17340"},
	{"bahamut.txt", "gcb5211"},
	{"ircd-irc2.txt", "gci8534"},
	{"ergo-nak.txt", "gcnak32565"},
}

func TestRegistrationCompletesAgainstRealTranscripts(t *testing.T) {
	for _, fx := range registrationFixtures {
		t.Run(fx.file, func(t *testing.T) {
			lines := loadTranscript(t, fx.file)
			final, actions := runTranscript(t, lines, fx.nick, false)

			want := wantOpeningActions(fx.nick)
			if len(actions) < len(want) {
				t.Fatalf("only %d actions total, opening lines missing: %+v", len(actions), actions)
			}
			if !reflect.DeepEqual(actions[:len(want)], want) {
				t.Fatalf("opening actions = %+v, want %+v", actions[:len(want)], want)
			}

			if final.Phase != PhaseComplete {
				t.Fatalf("final phase=%v, want complete (err=%v)", final.Phase, final.Err)
			}
			if !final.GotWelcome {
				t.Fatalf("GotWelcome=false at completion")
			}
			if final.ISUPPORT == nil || len(final.ISUPPORT.Raw) == 0 {
				t.Fatalf("ISUPPORT not populated: %+v", final.ISUPPORT)
			}

			var registered int
			var failed int
			for i, a := range actions {
				switch a.Kind {
				case ActionRegistered:
					registered++
					if i != len(actions)-1 {
						t.Fatalf("ActionRegistered at index %d, want last (len=%d)", i, len(actions))
					}
				case ActionFailed:
					failed++
				}
			}
			if registered != 1 {
				t.Fatalf("got %d ActionRegistered, want exactly 1", registered)
			}
			if failed != 0 {
				t.Fatalf("got %d ActionFailed on a transcript that registered successfully", failed)
			}
		})
	}
}

// TestReplayIdenticalToLive is the property build step 5 depends on: the
// same transcript, stepped once as "live" and once as "replay", must
// produce byte-for-byte identical final State — same ISUPPORT, same
// welcome tails, same phase — because Step's own logic never branches on
// Replay. The only difference allowed in the Step-driven portion of the
// action log is the Replay flag carried on each emitted Action.
//
// The opening CAP LS/NICK/USER triplet is a separate, stronger invariant,
// not just "same but flagged": a replay run must not emit those lines at
// all, because replay means folding a transcript into a connection that's
// already established — resending CAP LS/NICK/USER down that live socket
// is a real hazard (see Start's doc comment and docs/keeper-design.md),
// not merely a side effect a caller could choose to suppress. This test is
// what makes that invariant checkable by the corpus rather than by review.
func TestReplayIdenticalToLive(t *testing.T) {
	for _, fx := range registrationFixtures {
		t.Run(fx.file, func(t *testing.T) {
			lines := loadTranscript(t, fx.file)

			liveFinal, liveActions := runTranscript(t, lines, fx.nick, false)
			replayFinal, replayActions := runTranscript(t, lines, fx.nick, true)

			if !reflect.DeepEqual(liveFinal, replayFinal) {
				t.Fatalf("live and replay final states differ:\nlive:   %+v\nreplay: %+v", liveFinal, replayFinal)
			}

			openingLen := len(wantOpeningActions(fx.nick))
			if len(liveActions) < openingLen {
				t.Fatalf("live run has fewer actions than the opening triplet alone: %+v", liveActions)
			}
			liveOpening, liveStepActions := liveActions[:openingLen], liveActions[openingLen:]
			for _, a := range liveOpening {
				if a.Replay {
					t.Fatalf("live run's opening action has Replay=true: %+v", a)
				}
			}

			// The whole point: replay's action log has NO opening actions
			// at all, not opening actions flagged Replay=true.
			if len(liveStepActions) != len(replayActions) {
				t.Fatalf("live step-driven actions=%d, replay actions=%d (replay must contribute zero opening actions):\nlive step actions: %+v\nreplay actions:    %+v",
					len(liveStepActions), len(replayActions), liveStepActions, replayActions)
			}
			for i := range liveStepActions {
				live, rep := liveStepActions[i], replayActions[i]
				if live.Kind != rep.Kind || live.Line != rep.Line {
					t.Fatalf("action %d differs beyond Replay: live=%+v replay=%+v", i, live, rep)
				}
				if live.Replay {
					t.Fatalf("action %d: live run's action has Replay=true", i)
				}
				if !rep.Replay {
					t.Fatalf("action %d: replay run's action has Replay=false", i)
				}
			}
		})
	}
}

// TestStartIsNoOpDuringReplay pins Start's replay contract directly,
// independent of the transcript corpus: replay=true must return nil, not
// merely Replay-flagged actions, because a replay run is folding a
// transcript into an already-established connection — there is no fresh
// socket for CAP LS/NICK/USER to be the opening lines of.
func TestStartIsNoOpDuringReplay(t *testing.T) {
	acts := Start("nick", "swordfish", "user", "Real Name", true)
	if acts != nil {
		t.Fatalf("Start(replay=true) = %+v, want nil", acts)
	}
}

// TestMultiLineCAPLSAccumulates uses ergo's real capture, which genuinely
// sent CAP LS 302 across two lines (see testdata/registration/ergo.txt) —
// a real-world case, not a synthesised edge case. Confirms both lines'
// tokens are visible when CAP REQ is built and that the intermediate line
// (the one with the "*" continuation param) produces no action.
func TestMultiLineCAPLSAccumulates(t *testing.T) {
	lines := loadTranscript(t, "ergo.txt")
	var sawContinuation, sawReq bool
	s := New("gobnccap1", "", false, SASLConfig{})
	for _, line := range lines {
		msg, err := irc.Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		var acts []Action
		prevPhase := s.Phase
		s, acts = Step(s, Input{Msg: msg})
		if msg.Command == "CAP" && len(msg.Params) >= 3 && msg.Params[2] == "*" {
			sawContinuation = true
			if len(acts) != 0 {
				t.Fatalf("mid-multiline CAP LS produced actions: %+v", acts)
			}
			if s.Phase != prevPhase {
				t.Fatalf("mid-multiline CAP LS changed phase: %v -> %v", prevPhase, s.Phase)
			}
		}
		for _, a := range acts {
			if a.Kind == ActionSend && strings.HasPrefix(a.Line, "CAP REQ") {
				sawReq = true
				// Tokens from both LS lines must be represented: ergo's
				// first line offered account-notify (line 1), second line
				// offered echo-message (line 2) — a bug that only saw the
				// last line would be invisible without checking both.
				if !strings.Contains(a.Line, "account-notify") {
					t.Fatalf("CAP REQ missing a cap from the first LS line: %q", a.Line)
				}
				if !strings.Contains(a.Line, "echo-message") {
					t.Fatalf("CAP REQ missing a cap from the second LS line: %q", a.Line)
				}
			}
		}
	}
	if !sawContinuation {
		t.Fatalf("ergo.txt didn't actually contain a multi-line CAP LS — fixture assumption broken")
	}
	if !sawReq {
		t.Fatalf("no CAP REQ action was ever produced")
	}
}

// TestUnknownFieldsDoNotCrash / general robustness: a message Step has no
// case for must be a no-op, not a panic — every fixture already exercises
// many such lines (251-266, NOTICE, MOTD body), but this pins the property
// directly for an arbitrary unrecognized numeric.
func TestUnhandledCommandIsNoOp(t *testing.T) {
	s := New("nick", "", false, SASLConfig{})
	msg, err := irc.Parse(":server.example 999 nick :some numeric nothing understands")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, acts := Step(s, Input{Msg: msg})
	if !reflect.DeepEqual(got, s) {
		t.Fatalf("unhandled command changed state: %+v -> %+v", s, got)
	}
	if len(acts) != 0 {
		t.Fatalf("unhandled command produced actions: %+v", acts)
	}
}

func TestServerErrorFails(t *testing.T) {
	s := New("nick", "", false, SASLConfig{})
	msg, err := irc.Parse("ERROR :Closing link: (throttled)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, acts := Step(s, Input{Msg: msg, Replay: true})
	if got.Phase != PhaseFailed {
		t.Fatalf("phase=%v, want failed", got.Phase)
	}
	if got.Err == nil {
		t.Fatalf("Err is nil after ERROR")
	}
	if len(acts) != 1 || acts[0].Kind != ActionFailed || !acts[0].Replay {
		t.Fatalf("actions=%+v, want one ActionFailed with Replay=true", acts)
	}
}

func TestTerminalPhaseStepIsNoOp(t *testing.T) {
	s := New("nick", "", false, SASLConfig{})
	s.Phase = PhaseComplete
	msg, _ := irc.Parse(":server.example NOTICE nick :after registration")
	got, acts := Step(s, Input{Msg: msg})
	if !reflect.DeepEqual(got, s) {
		t.Fatalf("Step on a terminal phase changed state: %+v -> %+v", s, got)
	}
	if len(acts) != 0 {
		t.Fatalf("Step on a terminal phase produced actions: %+v", acts)
	}
}

// TestRealCAPNAKSendsCAPEnd uses a genuine CAP NAK from ergo
// (testdata/registration/ergo-nak.txt), captured by deliberately
// requesting a capability ("this-capability-does-not-exist-xyz") no real
// server offers — the NAK itself is a real ircd's real behavior, not
// synthesized; only the trigger (asking for a bogus cap) was engineered to
// obtain it. Confirms the state machine reacts to the NAK by sending CAP
// END, not by retrying or hanging, and that registration still completes.
func TestRealCAPNAKSendsCAPEnd(t *testing.T) {
	lines := loadTranscript(t, "ergo-nak.txt")
	s := New("gcnak32565", "", false, SASLConfig{})
	var sawNAK, sentCAPEndAfterNAK bool
	for _, line := range lines {
		msg, err := irc.Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		var acts []Action
		s, acts = Step(s, Input{Msg: msg})
		if msg.Command == "CAP" && len(msg.Params) >= 2 && strings.EqualFold(msg.Params[1], "NAK") {
			sawNAK = true
			if len(acts) != 1 || acts[0].Kind != ActionSend || acts[0].Line != "CAP END" {
				t.Fatalf("actions immediately after CAP NAK = %+v, want exactly one ActionSend(\"CAP END\")", acts)
			}
			sentCAPEndAfterNAK = true
		}
	}
	if !sawNAK {
		t.Fatalf("ergo-nak.txt didn't actually contain a CAP NAK — fixture assumption broken")
	}
	if !sentCAPEndAfterNAK {
		t.Fatalf("CAP END was not sent in direct response to the NAK")
	}
	if s.Phase != PhaseComplete {
		t.Fatalf("final phase=%v after a NAK'd cap, want complete — a NAK must not block registration", s.Phase)
	}
}

// TestRealNoCAPPathCompletesWithoutAnyCAPTraffic uses ircd-irc2's real
// capture, which never sent a single CAP line at all — old enough to
// predate IRCv3 CAP negotiation entirely (see testdata/registration/
// ircd-irc2.txt: zero occurrences of "CAP"). Confirms the state machine
// reaches completion via 001 alone forcing PhaseAwaitingWelcome, with the
// CAP LS/REQ/END machinery never engaging because it never receives a CAP
// message to react to — this is what a real ircd that ignores CAP entirely
// actually produces, not a hypothetical.
//
// The client still sends CAP LS 302 as its very first line regardless
// (that's Start, and it's how a real client discovers whether a server
// supports CAP at all — see internal/uplink's register method, which this
// mirrors) — what this test actually proves is that the *reactive* CAP
// machinery (REQ/END/NAK) never engages beyond that one opening line,
// because ircd-irc2 never replies to it.
func TestRealNoCAPPathCompletesWithoutAnyCAPTraffic(t *testing.T) {
	lines := loadTranscript(t, "ircd-irc2.txt")
	for _, line := range lines {
		if strings.Contains(line, "CAP") {
			t.Fatalf("ircd-irc2.txt contains a CAP line, fixture assumption broken: %q", line)
		}
	}
	final, actions := runTranscript(t, lines, "gci8534", false)
	if final.Phase != PhaseComplete {
		t.Fatalf("final phase=%v, want complete", final.Phase)
	}
	var capActions int
	for _, a := range actions {
		if a.Kind == ActionSend && strings.HasPrefix(a.Line, "CAP") {
			capActions++
		}
	}
	if capActions != 1 {
		t.Fatalf("got %d CAP actions, want exactly 1 (Start's opening CAP LS 302, never followed up since the server never replies): %+v", capActions, actions)
	}
}
