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
// pre-registration CAP END trigger from uplink.register's switch.
func stepSASLOutcome(s State, in Input) (State, []Action) {
	msg := in.Msg
	s.saslMech = ""
	s.scramConv = nil

	if msg.Command != "903" && msg.Command != "907" && s.SASL.Required {
		s.Phase = PhaseFailed
		s.Err = fmt.Errorf("SASL failed: %s %v", msg.Command, msg.Params)
		return s, []Action{{Kind: ActionFailed, Err: s.Err, Replay: in.Replay}}
	}
	if s.GotWelcome {
		// Post-registration SASL outcome (e.g. a services-side re-auth) —
		// nothing left for the registration state machine to do about it.
		return s, nil
	}
	s.Phase = PhaseAwaitingWelcome
	return s, []Action{{Kind: ActionSend, Line: "CAP END", Replay: in.Replay}}
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
	s.saslMech = mech
	s.scramConv = nil
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
		s.saslMech = ""
		s.scramConv = nil
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

	resp, err := conv.Step(challenge)
	if err != nil {
		return abortSASL(s, in, err)
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
	actions := []Action{{Kind: ActionSend, Line: "AUTHENTICATE *", Replay: in.Replay}}
	if s.SASL.Required {
		s.Phase = PhaseFailed
		s.Err = err
		return s, append(actions, Action{Kind: ActionFailed, Err: err, Replay: in.Replay})
	}
	if !s.GotWelcome {
		s.Phase = PhaseAwaitingWelcome
		return s, append(actions, Action{Kind: ActionSend, Line: "CAP END", Replay: in.Replay})
	}
	return s, actions
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
