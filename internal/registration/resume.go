package registration

import (
	"fmt"
	"strings"
)

// ResumeCap is the IRCv3 draft capability this file implements the client
// side of: github.com/DanielOaks/ircv3-specifications, branch
// master+resume, extensions/resume.md. The bouncer is the *client* here —
// resuming its own uplink session with the ircd after the socket the
// keeper held died (a network drop, or a keeper restart, the one thing
// the keeper/brain split can't paper over). The last ircd to ship this
// was Oragono ≤ 2.5.1 (Ergo removed it in 2.6.0), so the wire shapes here
// follow that implementation where the spec text is loose.
//
// Sequence, all within registration:
//
//	S: CAP * LS :... draft/resume-0.5 ...
//	C: CAP REQ :... draft/resume-0.5 ...   (always, token or not)
//	S: CAP * ACK :... draft/resume-0.5 ...
//	S: RESUME TOKEN <new>                   (persist: ActionResumeToken)
//	C: RESUME <old> [timestamp]             (only if State.ResumeToken set)
//	S: RESUME SUCCESS <oldnick>             → 001… as usual, Resumed=true
//	   — or —
//	S: FAIL RESUME <code> :...              → SASL/CAP END as if no resume
//
// RESUME TOKEN and the client's RESUME can arrive in either order — the
// server issues the token on ACK and this machine sends RESUME on ACK, so
// they cross on the wire. IssuedToken is separate from ResumeToken for
// exactly that reason.
const ResumeCap = "draft/resume-0.5"

// resumePossible reports whether this connection might still resume: a
// token to present exists and nothing has ruled the attempt out yet.
// While true, a nick-ladder exhaustion is deferred rather than fatal (see
// stepNickError) — a resume attempt *guarantees* a 433, because the old
// session still holds the nick, and RESUME SUCCESS hands it back.
func resumePossible(s State) bool {
	return s.ResumeToken != "" && !s.resumeRuledOut
}

// sendResume is stepCAP's ACK branch when a resume is on: RESUME goes out
// and the machine waits in PhaseResuming. SASL is deliberately not
// started, per spec.
func sendResume(s State, in Input) (State, []Action) {
	line := "RESUME " + s.ResumeToken
	if s.ResumeTimestamp != "" {
		line += " " + s.ResumeTimestamp
	}
	s.Phase = PhaseResuming
	return s, []Action{{Kind: ActionSend, Line: line, Replay: in.Replay}}
}

// continueRegistration is what CAP ACK led to before resume existed, and
// what a failed resume falls back to: SASL if it was wanted and ACK'd,
// otherwise CAP END.
func continueRegistration(s State, in Input) (State, []Action) {
	if s.SASL.Wanted && s.Acked["sasl"] {
		return startSASL(s, in)
	}
	s.Phase = PhaseAwaitingWelcome
	return s, []Action{{Kind: ActionSend, Line: "CAP END", Replay: in.Replay}}
}

// ruleOutResume marks that no resume will happen on this connection. If a
// nick-ladder exhaustion was deferred on the strength of a possible
// resume, it becomes the registration failure it always was — the
// returned actions are then non-nil (an ActionFailed) and the caller must
// return them instead of continuing. A no-op if resume was never possible.
func ruleOutResume(s State, in Input) (State, []Action) {
	if s.resumeRuledOut {
		return s, nil
	}
	s.resumeRuledOut = true
	if s.pendingNickErr == nil {
		return s, nil
	}
	s.Phase = PhaseFailed
	s.Err = s.pendingNickErr
	s.pendingNickErr = nil
	return s, []Action{{Kind: ActionFailed, Err: s.Err, Replay: in.Replay}}
}

// stepResume handles the server→client RESUME message's two forms.
func stepResume(s State, in Input) (State, []Action) {
	msg := in.Msg
	switch strings.ToUpper(msg.Param(0)) {
	case "TOKEN":
		tok := msg.Param(1)
		if tok == "" {
			return s, nil
		}
		s.IssuedToken = tok
		return s, []Action{{Kind: ActionResumeToken, Token: tok, Replay: in.Replay}}

	case "SUCCESS":
		if s.Phase != PhaseResuming {
			return s, nil // not ours to act on; nothing was requested
		}
		// The old session's nick is ours again, whatever the ladder
		// settled on meanwhile. Registration completes server-side from
		// here: no CAP END, no SASL — the welcome numerics follow.
		if nick := msg.Param(1); nick != "" {
			s.Nick = nick
		}
		s.Resumed = true
		s.pendingNickErr = nil
		s.Phase = PhaseAwaitingWelcome
		return s, nil

	default:
		return s, nil
	}
}

// stepFail handles standard-replies FAIL during registration. Only FAIL
// RESUME while a RESUME is outstanding means anything to this machine:
// the token was rejected (INVALID_TOKEN, or any other code — the spec
// tells a client to give up and register normally for all of them), so
// registration continues exactly where the ACK would otherwise have
// taken it. Any other FAIL is passthrough.
func stepFail(s State, in Input) (State, []Action) {
	msg := in.Msg
	if !strings.EqualFold(msg.Param(0), "RESUME") || s.Phase != PhaseResuming {
		return s, nil
	}
	s.ResumeToken = "" // consumed; never re-presented on this connection
	var failed []Action
	if s, failed = ruleOutResume(s, in); failed != nil {
		return s, failed
	}
	return continueRegistration(s, in)
}

// resumeFailErr is the Err text a deferred nick exhaustion surfaces with —
// the same "nick error:" wording stepNickError uses, because
// Session.HandleDisconnect matches on it to relay the numeric downstream.
func resumeFailErr(cmd string, params []string) error {
	return fmt.Errorf("nick error: %s %v", cmd, params)
}
