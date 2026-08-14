// Package registration is a pure state machine for IRC uplink registration:
// CAP negotiation through the end of MOTD, including SASL and the
// nick-collision ladder. It performs no I/O — no sockets, no timers, no
// writes. Step takes the current State and one parsed message, and returns
// the new State plus a list of Actions the caller should take. Side
// effects are data, not code: Step never sends a line, never marks a
// client registered, never runs a perform script — it only says "send this
// line" or "registration is complete," and the caller decides what that
// means and whether to act on it.
//
// This is deliberately a new package, not a reshaping of internal/uplink.
// The old, socket-coupled implementation in internal/uplink/uplink.go stays
// in place and working; this package is the same logic rebuilt so it can
// run against a sequenced line feed (live or replayed from a keeper
// transcript) instead of a blocking socket read. See
// registration_test.go's transcript-driven tests for the corpus this is
// verified against — real captures from ergo, a real Undernet connection,
// and six other real ircd implementations (testdata/registration/).
//
// SASL (sasl.go, ported from internal/uplink/sasl.go) and the
// nick-collision ladder (nick.go, ported from internal/uplink/nick.go) are
// both implemented. Neither is exercised by the real transcript corpus —
// none of the reachable real servers required SASL or produced a nick
// collision during capture — so both are covered by synthetic table tests
// instead (registration_sasl_test.go, registration_nick_test.go),
// hand-driven against the same numerics/AUTHENTICATE sequences
// internal/uplink's own SASL tests use as reference behavior. See docs/
// keeper-design.md for why "synthetic where a real capture isn't
// obtainable" is the accepted fallback, not a shortcut.
package registration

import (
	"fmt"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// DesiredCaps mirrors uplink.DesiredCaps — the capabilities this state
// machine requests when the server offers them. Duplicated rather than
// imported: internal/uplink is the old implementation this package is
// meant to eventually replace, so depending on it here would tie the new,
// pure code to the thing being ported away from.
var DesiredCaps = []string{
	"cap-notify",
	"message-tags",
	"server-time",
	"batch",
	"labeled-response",
	"account-tag",
	"account-notify",
	"extended-join",
	"echo-message",
	"away-notify",
	"chghost",
	"invite-notify",
	"sasl",
	"chathistory",
	"draft/chathistory",
}

// Phase is where the state machine currently is.
type Phase int

const (
	PhaseCAPNegotiation  Phase = iota
	PhaseAuthenticating        // CAP ACK'd sasl and we want it; mid AUTHENTICATE exchange
	PhaseAwaitingWelcome       // CAP END sent (or skipped), waiting for 001..376/422
	PhaseComplete
	PhaseFailed
)

func (p Phase) String() string {
	switch p {
	case PhaseCAPNegotiation:
		return "cap_negotiation"
	case PhaseAuthenticating:
		return "authenticating"
	case PhaseAwaitingWelcome:
		return "awaiting_welcome"
	case PhaseComplete:
		return "complete"
	case PhaseFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// State is the machine's full state at a point in time. Step takes a State
// by value and returns a new one — callers hold the current State between
// Step calls, there is nothing else to manage.
//
// ISUPPORT and scramConv are held as pointers whose own methods mutate them
// in place (Parse005, scram's ClientConversation.Step) rather than
// returning new values — the same trade-off in both cases: Step's own
// control flow depends only on Input and State, never on anything outside
// them, so the same input sequence always produces the same mutations in
// the same order regardless of whether it's live or replayed. That's what
// TestReplayIdenticalToLive actually checks (via reflect.DeepEqual, which
// follows pointers), not an assumption papered over by the pointer.
type State struct {
	Phase Phase

	Nick         string
	PrimaryNick  string // original desired nick, for the ladder to return to if it ever frees up
	AltNick      string
	nickRecovery bool

	SASL SASLConfig

	saslMech  string
	scramConv scramConversation

	// Offered accumulates CAP LS tokens (name -> value, value may be
	// empty) across however many LS lines a 302 negotiation spans — a
	// multi-line LS is not a special case, just more tokens added to the
	// same map before the terminal (non-"*") line triggers CAP REQ.
	Offered map[string]string
	Acked   map[string]bool

	RPL002, RPL003, RPL004 []string
	ISUPPORT               *irc.ISUPPORT

	GotWelcome bool
	Err        error // set when Phase == PhaseFailed
}

// New creates the starting State for a fresh registration attempt.
// primaryNick/altNick/nickRecovery mirror store.Network's Nick/AltNick/
// NickRecovery; sasl mirrors the SASL-relevant subset of store.Network —
// see SASLConfig's doc comment for why HasClientCert is resolved by the
// caller rather than by this package.
func New(primaryNick, altNick string, nickRecovery bool, sasl SASLConfig) State {
	return State{
		Phase:        PhaseCAPNegotiation,
		Nick:         primaryNick,
		PrimaryNick:  primaryNick,
		AltNick:      altNick,
		nickRecovery: nickRecovery,
		SASL:         sasl,
		Offered:      make(map[string]string),
		Acked:        make(map[string]bool),
		ISUPPORT:     irc.NewISUPPORT(),
	}
}

// Start returns the opening lines a client sends before the server has said
// anything at all: CAP LS 302, optionally PASS, then NICK and USER. Step
// never produces these itself — every Step transition is a reaction to a
// server-sent message, and nothing has arrived yet at connection start — so
// callers must call Start once, immediately after a fresh connection is
// established, before feeding it any server input. This mirrors
// internal/uplink/uplink.go's register method line-for-line; it doesn't
// take or return a State because none of these lines are conditioned on
// anything the server has said.
//
// replay must be true when this call is driving a replayed transcript into
// an already-established connection rather than a genuinely fresh one —
// Start is a live-only entry point (see docs/keeper-design.md's Part 3a
// section). Unlike every other action kind, Start's output isn't a
// reaction to an Input the caller already tagged with Replay, so that
// existing gating mechanism can't reach it on its own; replay=true is a
// hard no-op (nil, not just Replay-flagged actions) specifically so a
// caller can pass it unconditionally without needing to remember to skip
// calling Start at all during replay. Getting this wrong resends
// CAP LS/NICK/USER down a live socket on every replay — on this project,
// that means every code reload.
func Start(nick, pass, username, realname string, replay bool) []Action {
	if replay {
		return nil
	}
	if username == "" {
		username = "gobnc"
	}
	if realname == "" {
		realname = "GoBNC"
	}
	actions := []Action{{Kind: ActionSend, Line: "CAP LS 302"}}
	if pass != "" {
		actions = append(actions, Action{Kind: ActionSend, Line: "PASS " + pass})
	}
	actions = append(actions,
		Action{Kind: ActionSend, Line: "NICK " + nick},
		Action{Kind: ActionSend, Line: fmt.Sprintf("USER %s 0 * :%s", username, realname)},
	)
	return actions
}

// ActionKind identifies what an Action asks the caller to do.
type ActionKind int

const (
	// ActionSend: write Line to the uplink verbatim. The only kind of
	// action with a side effect that must happen regardless of Replay —
	// during replay there is no live uplink to write to, so callers must
	// suppress ActionSend during replay themselves (Step doesn't know
	// whether it's being driven by a real connection); every other action
	// kind carries Replay for the caller to gate on.
	ActionSend ActionKind = iota
	// ActionRegistered: registration completed successfully. This is the
	// action whose execution must be gated on Replay — auto-join,
	// on-connect perform, and "connected" notices to downstream clients
	// all key off this, and none of them should fire on a replayed
	// transcript (see the design brief's replay side-effect audit
	// requirement). Step itself does none of these; it only says
	// registration is done.
	ActionRegistered
	// ActionFailed: registration cannot complete (nick exhausted, server
	// ERROR, SASL required but failed). Err has the reason.
	ActionFailed
)

func (k ActionKind) String() string {
	switch k {
	case ActionSend:
		return "send"
	case ActionRegistered:
		return "registered"
	case ActionFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Action is one thing Step wants the caller to do. Exactly the fields
// relevant to Kind are meaningful; the rest are zero.
type Action struct {
	Kind   ActionKind
	Line   string // ActionSend
	Err    error  // ActionFailed
	Replay bool   // copied from the triggering Input; see ActionKind docs
}

// Input is one message driving a Step call.
type Input struct {
	Msg irc.Message
	// Replay marks this message as replayed transcript rather than a line
	// just received live. Step's own logic does not branch on Replay at
	// all — the same code path produces the same State transitions and
	// the same Actions either way, which is the property that makes replay
	// provably identical to live. Replay exists solely to be copied onto
	// emitted Actions so the caller can gate execution of the side effects
	// those actions describe.
	Replay bool
}

// Step advances the state machine by one message. Once Phase is
// PhaseComplete or PhaseFailed, Step is a no-op (returns s unchanged, no
// actions) — the caller is expected to stop stepping, but a stray extra
// call (e.g. a line arriving just after completion) can't corrupt state.
func Step(s State, in Input) (State, []Action) {
	if s.Phase == PhaseComplete || s.Phase == PhaseFailed {
		return s, nil
	}

	msg := in.Msg
	switch msg.Command {
	case "CAP":
		return stepCAP(s, in)

	case "AUTHENTICATE":
		return stepAuthenticate(s, in)

	case "900":
		return stepLoggedIn(s, in)
	case "903", "904", "905", "906", "907": // SASL outcomes
		return stepSASLOutcome(s, in)

	case "001":
		s.GotWelcome = true
		if len(msg.Params) > 0 {
			s.Nick = msg.Params[0]
		}
		if s.Phase == PhaseCAPNegotiation {
			s.Phase = PhaseAwaitingWelcome
		}
		return s, nil

	case "002":
		s.RPL002 = welcomeTail(msg.Params)
		return s, nil
	case "003":
		s.RPL003 = welcomeTail(msg.Params)
		return s, nil
	case "004":
		s.RPL004 = welcomeTail(msg.Params)
		return s, nil
	case "005":
		// ISUPPORT is irreducible: the server sends it once and never
		// again reliably on a live connection, and it can span multiple
		// 005 lines. Parse005 accumulates — see irc.ISUPPORT.
		s.ISUPPORT.Parse005(msg.Params)
		return s, nil

	case "375", "372":
		return s, nil

	case "376", "422": // end of MOTD / no MOTD
		if !s.GotWelcome {
			return s, nil // not actually registered yet; ignore
		}
		s.Phase = PhaseComplete
		return s, []Action{{Kind: ActionRegistered, Replay: in.Replay}}

	case "432", "433", "437": // erroneous/in-use/unavailable nick
		return stepNickError(s, in)

	case "ERROR":
		s.Phase = PhaseFailed
		s.Err = fmt.Errorf("server ERROR: %s", msg.Trailing())
		return s, []Action{{Kind: ActionFailed, Err: s.Err, Replay: in.Replay}}

	default:
		return s, nil
	}
}

func stepCAP(s State, in Input) (State, []Action) {
	msg := in.Msg
	if len(msg.Params) < 2 {
		return s, nil
	}
	sub := strings.ToUpper(msg.Params[1])
	trailing := msg.Trailing()

	switch sub {
	case "LS":
		for _, tok := range strings.Fields(trailing) {
			name, val, _ := strings.Cut(tok, "=")
			s.Offered[name] = val
		}
		// CAP 302 LS may span multiple lines; a "*" third param means more
		// are coming and this line is not the one to react to yet.
		if len(msg.Params) >= 3 && msg.Params[2] == "*" {
			return s, nil
		}
		var req []string
		for _, want := range DesiredCaps {
			if want == "sasl" && !s.SASL.Wanted {
				continue
			}
			if _, ok := s.Offered[want]; ok {
				req = append(req, want)
			}
		}
		if len(req) == 0 {
			s.Phase = PhaseAwaitingWelcome
			return s, []Action{{Kind: ActionSend, Line: "CAP END", Replay: in.Replay}}
		}
		return s, []Action{{Kind: ActionSend, Line: "CAP REQ :" + strings.Join(req, " "), Replay: in.Replay}}

	case "ACK":
		acked := false
		for _, raw := range strings.Fields(trailing) {
			name, _, _ := strings.Cut(strings.TrimPrefix(raw, "-"), "=")
			s.Acked[name] = true
			if name == "sasl" {
				acked = true
			}
		}
		if s.SASL.Wanted && acked {
			return startSASL(s, in)
		}
		s.Phase = PhaseAwaitingWelcome
		return s, []Action{{Kind: ActionSend, Line: "CAP END", Replay: in.Replay}}

	case "NAK":
		s.Phase = PhaseAwaitingWelcome
		return s, []Action{{Kind: ActionSend, Line: "CAP END", Replay: in.Replay}}

	default: // NEW, DEL, LIST — not registration-phase concerns
		return s, nil
	}
}

// welcomeTail returns params after the target nick (params[0]), or nil if
// there's nothing beyond it — mirrors uplink.storeWelcomeTail.
func welcomeTail(params []string) []string {
	if len(params) <= 1 {
		return nil
	}
	return append([]string(nil), params[1:]...)
}
