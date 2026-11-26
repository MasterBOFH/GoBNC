package registration

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/xdg-go/scram"
)

// IRCv3 AUTHENTICATE base64 chunks are at most 400 bytes.
const authenticateChunkSize = 400

// SASLConfig is the SASL-relevant subset of store.Network, plus one field
// this package cannot resolve itself: HasClientCert. Deciding whether a
// client certificate is configured means resolving file paths and touching
// disk (see config.ResolveTLSClientCert) — I/O, which this package
// deliberately has none of. The caller resolves that once, outside Step,
// and passes the answer in as a bool.
type SASLConfig struct {
	Wanted   bool
	Required bool
	User     string
	Pass     string

	HasClientCert bool
}

// scramConversation is the subset of *scram.ClientConversation this
// package uses — narrowed to keep the xdg-go/scram dependency's surface
// area explicit and to make State's SCRAM field mockable in tests without
// a real conversation.
type scramConversation interface {
	Step(challenge string) (response string, err error)
	// Valid reports whether the server-final arrived and its server
	// signature verified — the server's proof that it knows the password.
	Valid() bool
}

// stepLoggedIn handles 900 (RPL_LOGGEDIN), noting the account is not
// currently tracked by this package (no caller needs it during
// registration itself — session-level code tracks the account post
// registration) but the numeric still needs to not fall through to the
// unhandled-command default, since a real SASL exchange always includes
// it and Step should recognize it as expected traffic.
func stepLoggedIn(s State, in Input) (State, []Action) {
	return s, nil
}

// stepSASLOutcome handles 903 (success), 904/905/906 (failure), 907
// (already authenticated) — mirrors uplink.handleSASLOutcome plus the
// pre-registration CAP END trigger from uplink.register's switch. A
// success numeric the exchange doesn't back up (see saslSuccessRefused)
// is a failure like 904: it fails registration when SASL is required and
// falls back to registering unauthenticated otherwise.
func stepSASLOutcome(s State, in Input) (State, []Action) {
	msg := in.Msg
	refused := saslSuccessRefused(s, msg.Command)
	failed := refused != nil || (msg.Command != "903" && msg.Command != "907")
	s.saslSucceeded = !failed
	s.saslMech = ""
	s.scramConv = nil

	if s.GotWelcome {
		// Post-registration SASL outcome (a StepPost re-auth) — nothing
		// left for registration to do about it either way. Required is
		// deliberately not consulted here: it decides whether the bouncer
		// may *register* unauthenticated, and a re-auth failing after
		// registration leaves the connection exactly where a services
		// outage (901) already left it, which nothing tears down either.
		return s, nil
	}
	if failed && s.SASL.Required {
		s.Phase = PhaseFailed
		s.Err = refused
		if s.Err == nil {
			s.Err = fmt.Errorf("SASL failed: %s %v", msg.Command, msg.Params)
		}
		return s, []Action{{Kind: ActionFailed, Err: s.Err, Replay: in.Replay}}
	}
	s.Phase = PhaseAwaitingWelcome
	return s, []Action{{Kind: ActionSend, Line: "CAP END", Replay: in.Replay}}
}

// saslSuccessRefused returns why a success numeric (903, or 907 "already
// authenticated") must not count as success for the exchange this State
// ran, or nil if it may. SCRAM authenticates both ways: the server proves
// it knows the password with the signature in its server-final, so a
// success numeric before that signature has verified is a server (or a
// man in the middle) claiming success without the proof, and accepting
// it would throw the mutual authentication away. A success numeric after
// GoBNC itself aborted the exchange (AUTHENTICATE *) is refused for any
// mechanism: there was no exchange left to succeed. PLAIN and EXTERNAL
// carry no server proof, so their success numerics are taken as sent.
func saslSuccessRefused(s State, cmd string) error {
	if cmd != "903" && cmd != "907" {
		return nil
	}
	if s.saslAborted {
		return fmt.Errorf("SASL failed: %s after the exchange was aborted", cmd)
	}
	if isSCRAM(s.saslAttempt) && !s.saslVerified {
		return fmt.Errorf("SASL %s failed: %s without a verified server signature", s.saslAttempt, cmd)
	}
	return nil
}

func isSCRAM(mech string) bool {
	return strings.HasPrefix(strings.ToUpper(mech), "SCRAM-")
}

// beginSASL records the start of a new exchange with mech, clearing what
// the previous one left behind.
func beginSASL(s State, mech string) State {
	s.saslMech = mech
	s.scramConv = nil
	s.saslAttempt = mech
	s.saslVerified = false
	s.saslAborted = false
	s.saslSucceeded = false
	return s
}

// startSASL begins AUTHENTICATE with a mechanism we can complete, called
// once CAP ACK's sasl and SASL.Wanted. Mirrors uplink.startSASL.
func startSASL(s State, in Input) (State, []Action) {
	mech, ok := pickSASLMech(s)
	if !ok {
		if s.SASL.Required {
			s.Phase = PhaseFailed
			s.Err = fmt.Errorf("SASL: no supported mechanism in %q", s.Offered["sasl"])
			return s, []Action{{Kind: ActionFailed, Err: s.Err, Replay: in.Replay}}
		}
		s.Phase = PhaseAwaitingWelcome
		return s, []Action{{Kind: ActionSend, Line: "CAP END", Replay: in.Replay}}
	}
	s.Phase = PhaseAuthenticating
	s = beginSASL(s, mech)
	return s, []Action{{Kind: ActionSend, Line: "AUTHENTICATE " + mech, Replay: in.Replay}}
}

// pickSASLMech prefers SCRAM-SHA-256, then PLAIN, then EXTERNAL. Password
// auth (SCRAM/PLAIN) only when both SASL user and password are set.
// EXTERNAL when SASL is on, password is empty, and a client cert is
// available. Mirrors uplink.pickSASLMech exactly, with HasClientCert
// substituted for the two file-resolving helpers it used (hasClientCert,
// clientCertPathsConfigured) — see SASLConfig's doc comment.
func pickSASLMech(s State) (string, bool) {
	if !s.SASL.Wanted {
		return "", false
	}
	passwordOK := s.SASL.User != "" && s.SASL.Pass != ""
	externalOK := s.SASL.Pass == "" && s.SASL.HasClientCert

	var preferred []string
	switch {
	case passwordOK:
		preferred = []string{"SCRAM-SHA-256", "PLAIN"}
	case externalOK:
		preferred = []string{"EXTERNAL"}
	default:
		return "", false
	}

	var mechs []string
	if raw := s.Offered["sasl"]; raw != "" {
		for _, m := range strings.Split(raw, ",") {
			if m = strings.TrimSpace(m); m != "" {
				mechs = append(mechs, m)
			}
		}
	}
	if len(mechs) == 0 {
		if passwordOK {
			return "PLAIN", true
		}
		return "EXTERNAL", true
	}
	for _, want := range preferred {
		for _, m := range mechs {
			if strings.EqualFold(m, want) {
				return want, true
			}
		}
	}
	return "", false
}

// stepAuthenticate continues an in-progress SASL exchange. Mirrors
// uplink.handleAuthenticate.
func stepAuthenticate(s State, in Input) (State, []Action) {
	msg := in.Msg
	param := msg.Param(0)
	if param == "" {
		return s, nil
	}
	if param == "*" {
		// The server ended the exchange; whatever was verified so far
		// doesn't make a later success numeric count.
		s.saslMech = ""
		s.scramConv = nil
		s.saslVerified = false
		return s, nil
	}
	mech := s.saslMech
	if mech == "" {
		return s, nil
	}

	switch strings.ToUpper(mech) {
	case "PLAIN":
		if param != "+" {
			return s, nil
		}
		if s.SASL.User == "" || s.SASL.Pass == "" {
			return abortSASL(s, in, fmt.Errorf("SASL PLAIN: missing username/password"))
		}
		payload := "\x00" + s.SASL.User + "\x00" + s.SASL.Pass
		return sendAuthenticate(s, in, base64.StdEncoding.EncodeToString([]byte(payload)))

	case "EXTERNAL":
		if param != "+" {
			return s, nil
		}
		if s.SASL.User == "" {
			return sendAuthenticate(s, in, "+")
		}
		return sendAuthenticate(s, in, base64.StdEncoding.EncodeToString([]byte(s.SASL.User)))

	case "SCRAM-SHA-256":
		return stepSCRAM(s, in, param)

	default:
		return abortSASL(s, in, fmt.Errorf("SASL: unexpected mechanism %q", mech))
	}
}

func stepSCRAM(s State, in Input, param string) (State, []Action) {
	if s.SASL.User == "" || s.SASL.Pass == "" {
		return abortSASL(s, in, fmt.Errorf("SASL SCRAM-SHA-256: missing username/password"))
	}

	var challenge string
	if param != "+" {
		raw, err := base64.StdEncoding.DecodeString(param)
		if err != nil {
			return abortSASL(s, in, fmt.Errorf("SASL SCRAM: bad challenge encoding: %w", err))
		}
		challenge = string(raw)
	}

	conv := s.scramConv
	if conv == nil {
		client, err := scram.SHA256.NewClient(s.SASL.User, s.SASL.Pass, "")
		if err != nil {
			return abortSASL(s, in, err)
		}
		conv = client.NewConversation()
		s.scramConv = conv
	}

	// conv.Step on the server-final checks its server signature and
	// fails on a mismatch, an e= error or a malformed message, each of
	// which aborts here.
	resp, err := conv.Step(challenge)
	if err != nil {
		return abortSASL(s, in, err)
	}
	if conv.Valid() {
		s.saslVerified = true
	}
	// IRCv3: if the mechanism ends with a non-empty server challenge (SCRAM
	// server-final), the client MUST still send an empty AUTHENTICATE +.
	if resp == "" {
		return sendAuthenticate(s, in, "+")
	}
	return sendAuthenticate(s, in, base64.StdEncoding.EncodeToString([]byte(resp)))
}

func abortSASL(s State, in Input, err error) (State, []Action) {
	s.saslMech = ""
	s.scramConv = nil
	s.saslVerified = false
	s.saslAborted = true
	actions := []Action{{Kind: ActionSend, Line: "AUTHENTICATE *", Replay: in.Replay}}
	if s.GotWelcome {
		// Post-registration (StepPost) — see stepSASLOutcome on why
		// Required doesn't apply once registered.
		return s, actions
	}
	if s.SASL.Required {
		s.Phase = PhaseFailed
		s.Err = err
		return s, append(actions, Action{Kind: ActionFailed, Err: err, Replay: in.Replay})
	}
	s.Phase = PhaseAwaitingWelcome
	return s, append(actions, Action{Kind: ActionSend, Line: "CAP END", Replay: in.Replay})
}

// sendAuthenticate chunks a base64 payload into <=400-byte AUTHENTICATE
// lines per IRCv3, terminating with a bare "AUTHENTICATE +" when the
// payload is empty or lands exactly on the chunk boundary. Mirrors
// uplink.writeAuthenticate, restructured to build a slice of Actions
// instead of writing each chunk as it goes.
func sendAuthenticate(s State, in Input, b64 string) (State, []Action) {
	if b64 == "" || b64 == "+" {
		return s, []Action{{Kind: ActionSend, Line: "AUTHENTICATE +", Replay: in.Replay}}
	}
	var actions []Action
	for {
		if len(b64) > authenticateChunkSize {
			actions = append(actions, Action{Kind: ActionSend, Line: "AUTHENTICATE " + b64[:authenticateChunkSize], Replay: in.Replay})
			b64 = b64[authenticateChunkSize:]
			continue
		}
		actions = append(actions, Action{Kind: ActionSend, Line: "AUTHENTICATE " + b64, Replay: in.Replay})
		if len(b64) == authenticateChunkSize {
			actions = append(actions, Action{Kind: ActionSend, Line: "AUTHENTICATE +", Replay: in.Replay})
		}
		return s, actions
	}
}

// StepPost is Step's post-registration counterpart for the one exchange
// that still needs the SASL machinery after PhaseComplete: bouncer-owned
// SASL that could not happen at connect time. If sasl wasn't offered when
// we registered (services down), or was and later went away (CAP DEL
// :sasl) and we've since been logged out, the ircd re-advertising it with
// CAP NEW makes Session CAP REQ it again; the resulting CAP ACK is the
// same trigger startSASL waits for during registration, and this runs the
// same AUTHENTICATE exchange. Session is the one deciding *whether* to
// re-REQ (it knows if we're still logged in, StepPost doesn't need to);
// StepPost only reacts to the ACK.
//
// Unlike Step, StepPost never changes Phase and never emits ActionFailed:
// the connection is registered and stays that way whatever the outcome —
// see stepSASLOutcome/abortSASL's GotWelcome branches. It is a no-op on a
// State that isn't PhaseComplete, so a caller can route every line here
// once Step has gone terminal without checking first.
//
// Known gap: saslMech/scramConv live only in this State, so a brain
// restart mid-exchange forgets it; the ircd's AUTHENTICATE timeout ends
// that attempt and the next CAP NEW starts a fresh one.
func StepPost(s State, in Input) (State, []Action) {
	if s.Phase != PhaseComplete {
		return s, nil
	}
	msg := in.Msg
	switch msg.Command {
	case "CAP":
		if len(msg.Params) < 2 {
			return s, nil
		}
		switch strings.ToUpper(msg.Params[1]) {
		case "NEW":
			// Refresh the offered mechanism list: sasl may not have been
			// in the CAP LS at all, or the list may have changed.
			for _, tok := range strings.Fields(msg.Trailing()) {
				name, val, _ := strings.Cut(tok, "=")
				s.Offered[name] = val
			}
		case "DEL":
			for _, tok := range strings.Fields(msg.Trailing()) {
				name, _, _ := strings.Cut(tok, "=")
				delete(s.Offered, name)
				delete(s.Acked, name)
			}
		case "ACK":
			acked := false
			for _, raw := range strings.Fields(msg.Trailing()) {
				name, val, _ := strings.Cut(strings.TrimPrefix(raw, "-"), "=")
				s.Acked[name] = true
				if name == "sasl" {
					acked = true
					if val != "" {
						s.Offered[name] = val
					}
				}
			}
			if !acked || !s.SASL.Wanted || s.saslMech != "" {
				return s, nil
			}
			mech, ok := pickSASLMech(s)
			if !ok {
				return s, nil
			}
			s = beginSASL(s, mech)
			return s, []Action{{Kind: ActionSend, Line: "AUTHENTICATE " + mech, Replay: in.Replay}}
		}
		return s, nil
	case "AUTHENTICATE":
		return stepAuthenticate(s, in)
	case "903", "904", "905", "906", "907":
		return stepSASLOutcome(s, in)
	default:
		return s, nil
	}
}
