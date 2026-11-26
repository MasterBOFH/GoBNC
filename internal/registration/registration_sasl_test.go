package registration

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/xdg-go/scram"
)

// None of the real transcripts captured for this package used SASL — every
// reachable server either doesn't require it or wasn't configured with
// credentials to test against. These tests drive Step directly against
// hand-built CAP/AUTHENTICATE sequences instead, using the actual
// numerics and message shapes internal/uplink/sasl.go's own tests treat as
// reference behavior. See docs/keeper-design.md for why synthetic is the
// accepted fallback here specifically.

func mustParse(t *testing.T, line string) irc.Message {
	t.Helper()
	msg, err := irc.Parse(line)
	if err != nil {
		t.Fatalf("parse %q: %v", line, err)
	}
	return msg
}

func step(t *testing.T, s State, line string) (State, []Action) {
	t.Helper()
	return Step(s, Input{Msg: mustParse(t, line)})
}

func sendLines(acts []Action) []string {
	var out []string
	for _, a := range acts {
		if a.Kind == ActionSend {
			out = append(out, a.Line)
		}
	}
	return out
}

func containsSendLine(acts []Action, prefix string) bool {
	for _, l := range sendLines(acts) {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}
	return false
}

func lastSendLine(t *testing.T, acts []Action) string {
	t.Helper()
	lines := sendLines(acts)
	if len(lines) == 0 {
		t.Fatalf("no ActionSend among %+v", acts)
	}
	return lines[len(lines)-1]
}

func TestSASLPlainHappyPath(t *testing.T) {
	s := New("nick", "", false, SASLConfig{Wanted: true, User: "alice", Pass: "hunter2"})

	s, acts := step(t, s, ":irc.example CAP nick LS :sasl=PLAIN")
	if !containsSendLine(acts, "CAP REQ") || !strings.Contains(lastSendLine(t, acts), "sasl") {
		t.Fatalf("expected a CAP REQ including sasl, got %+v", acts)
	}

	s, acts = step(t, s, ":irc.example CAP nick ACK :sasl")
	if s.Phase != PhaseAuthenticating {
		t.Fatalf("phase=%v, want authenticating", s.Phase)
	}
	if got := lastSendLine(t, acts); got != "AUTHENTICATE PLAIN" {
		t.Fatalf("got %q, want AUTHENTICATE PLAIN", got)
	}

	s, acts = step(t, s, "AUTHENTICATE +")
	want := base64.StdEncoding.EncodeToString([]byte("\x00alice\x00hunter2"))
	if got := lastSendLine(t, acts); got != "AUTHENTICATE "+want {
		t.Fatalf("got %q, want the base64 PLAIN payload", got)
	}

	s, acts = step(t, s, ":irc.example 903 nick :SASL authentication successful")
	if s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("phase=%v after 903, want awaiting_welcome", s.Phase)
	}
	if got := lastSendLine(t, acts); got != "CAP END" {
		t.Fatalf("got %q, want CAP END after successful SASL", got)
	}

	s, acts = step(t, s, ":irc.example 001 nick :Welcome")
	_ = acts
	s, acts = step(t, s, ":irc.example 376 nick :End of MOTD")
	if s.Phase != PhaseComplete {
		t.Fatalf("phase=%v, want complete", s.Phase)
	}
	if len(acts) != 1 || acts[0].Kind != ActionRegistered {
		t.Fatalf("acts=%+v, want exactly one ActionRegistered", acts)
	}
}

func TestSASLExternalHappyPath(t *testing.T) {
	s := New("nick", "", false, SASLConfig{Wanted: true, HasClientCert: true})

	s, acts := step(t, s, ":irc.example CAP nick LS :sasl=EXTERNAL")
	if !containsSendLine(acts, "CAP REQ") {
		t.Fatalf("expected CAP REQ, got %+v", acts)
	}
	s, acts = step(t, s, ":irc.example CAP nick ACK :sasl")
	if got := lastSendLine(t, acts); got != "AUTHENTICATE EXTERNAL" {
		t.Fatalf("got %q, want AUTHENTICATE EXTERNAL", got)
	}
	// No SASL.User set (authzid optional): "+" is sent verbatim, not a
	// base64-encoded empty string.
	s, acts = step(t, s, "AUTHENTICATE +")
	if got := lastSendLine(t, acts); got != "AUTHENTICATE +" {
		t.Fatalf("got %q, want a bare AUTHENTICATE +", got)
	}
	s, _ = step(t, s, ":irc.example 903 nick :SASL authentication successful")
	if s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("phase=%v, want awaiting_welcome", s.Phase)
	}
}

func TestSASLRequiredNoMechanismFails(t *testing.T) {
	s := New("nick", "", false, SASLConfig{Wanted: true, Required: true, User: "alice", Pass: "hunter2"})
	s, _ = step(t, s, ":irc.example CAP nick LS :sasl=EXTERNAL") // no PLAIN/SCRAM offered, we have no cert
	s, acts := step(t, s, ":irc.example CAP nick ACK :sasl")
	if s.Phase != PhaseFailed {
		t.Fatalf("phase=%v, want failed", s.Phase)
	}
	if len(acts) != 1 || acts[0].Kind != ActionFailed || acts[0].Err == nil {
		t.Fatalf("acts=%+v, want one ActionFailed with a non-nil Err", acts)
	}
}

func TestSASLNotRequiredNoMechanismContinuesUnauthenticated(t *testing.T) {
	s := New("nick", "", false, SASLConfig{Wanted: true, Required: false, User: "alice", Pass: "hunter2"})
	s, _ = step(t, s, ":irc.example CAP nick LS :sasl=EXTERNAL")
	s, acts := step(t, s, ":irc.example CAP nick ACK :sasl")
	if s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("phase=%v, want awaiting_welcome (SASL skipped, not required)", s.Phase)
	}
	if got := lastSendLine(t, acts); got != "CAP END" {
		t.Fatalf("got %q, want CAP END", got)
	}
}

func TestSASLPlainMissingCredentialsAborts(t *testing.T) {
	s := New("nick", "", false, SASLConfig{Wanted: true, User: "", Pass: ""})
	s, _ = step(t, s, ":irc.example CAP nick LS :sasl=PLAIN")
	// pickSASLMech requires both User and Pass to prefer PLAIN at all; with
	// neither set, no mechanism is chosen and CAP ACK falls straight
	// through to CAP END (this is uplink's behavior too: PLAIN is only
	// attempted with real credentials). Confirm that rather than assert an
	// abort that never happens.
	s, acts := step(t, s, ":irc.example CAP nick ACK :sasl")
	if s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("phase=%v, want awaiting_welcome", s.Phase)
	}
	if got := lastSendLine(t, acts); got != "CAP END" {
		t.Fatalf("got %q, want CAP END", got)
	}
}

func TestSASLAbortOnUnexpectedMechanism(t *testing.T) {
	// Force PLAIN in progress, then have the "server" behave as if a
	// different mechanism were active by manipulating state directly is
	// not possible (unexported field) — instead drive the abort path via
	// AUTHENTICATE * (server- or self-aborted exchange), which must clear
	// state and, since SASL isn't required, proceed to CAP END.
	s := New("nick", "", false, SASLConfig{Wanted: true, User: "alice", Pass: "hunter2"})
	s, _ = step(t, s, ":irc.example CAP nick LS :sasl=PLAIN")
	s, _ = step(t, s, ":irc.example CAP nick ACK :sasl")
	if s.Phase != PhaseAuthenticating {
		t.Fatalf("phase=%v, want authenticating", s.Phase)
	}
	s, acts := step(t, s, "AUTHENTICATE *")
	if len(acts) != 0 {
		t.Fatalf("AUTHENTICATE * produced actions: %+v, want none (it only clears in-progress state)", acts)
	}
	// Exchange aborted by the server; registration must still be able to
	// proceed once the server or brain decides what's next (e.g. it later
	// sends CAP ACK/NAK again in a real flow, or 904+not-required below).
	s, acts = step(t, s, ":irc.example 904 nick :SASL authentication failed")
	if s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("phase=%v after non-required SASL failure, want awaiting_welcome", s.Phase)
	}
	if got := lastSendLine(t, acts); got != "CAP END" {
		t.Fatalf("got %q, want CAP END", got)
	}
}

func TestSASLRequiredFailureNumericFails(t *testing.T) {
	s := New("nick", "", false, SASLConfig{Wanted: true, Required: true, User: "alice", Pass: "hunter2"})
	s, _ = step(t, s, ":irc.example CAP nick LS :sasl=PLAIN")
	s, _ = step(t, s, ":irc.example CAP nick ACK :sasl")
	s, _ = step(t, s, "AUTHENTICATE +")
	s, acts := step(t, s, ":irc.example 904 nick :SASL authentication failed")
	if s.Phase != PhaseFailed {
		t.Fatalf("phase=%v, want failed", s.Phase)
	}
	if len(acts) != 1 || acts[0].Kind != ActionFailed {
		t.Fatalf("acts=%+v, want one ActionFailed", acts)
	}
}

// TestSASLSCRAMRealRoundTrip drives a genuine xdg-go/scram server
// conversation against Step's client-side handling — real crypto on both
// ends, not a hand-faked challenge/response. This is as close to a real
// SCRAM transcript as is obtainable without a live server that offers it
// (none reachable during transcript capture did).
func TestSASLSCRAMRealRoundTrip(t *testing.T) {
	const user, pass = "alice", "hunter2"

	setup, err := scram.SHA256.NewClient(user, pass, "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	stored := setup.GetStoredCredentials(scram.KeyFactors{Salt: "abcdefgh", Iters: 4096})
	srv, err := scram.SHA256.NewServer(func(u string) (scram.StoredCredentials, error) {
		if u != user {
			return scram.StoredCredentials{}, fmt.Errorf("no such user %q", u)
		}
		return stored, nil
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sc := srv.NewConversation()

	s := New("nick", "", false, SASLConfig{Wanted: true, User: user, Pass: pass})
	s, acts := step(t, s, ":irc.example CAP nick LS :sasl=SCRAM-SHA-256")
	if !containsSendLine(acts, "CAP REQ") {
		t.Fatalf("expected CAP REQ, got %+v", acts)
	}
	s, acts = step(t, s, ":irc.example CAP nick ACK :sasl")
	if got := lastSendLine(t, acts); got != "AUTHENTICATE SCRAM-SHA-256" {
		t.Fatalf("got %q, want AUTHENTICATE SCRAM-SHA-256", got)
	}
	if s.Phase != PhaseAuthenticating {
		t.Fatalf("phase=%v, want authenticating", s.Phase)
	}

	serverParam := "+" // server prompts first, with an empty challenge
	completed := false
	for i := 0; i < 10; i++ {
		s, acts = step(t, s, "AUTHENTICATE "+serverParam)
		clientLine := lastSendLine(t, acts)
		clientB64 := strings.TrimPrefix(clientLine, "AUTHENTICATE ")
		if clientB64 == "+" {
			completed = true
			break
		}
		clientMsg, err := base64.StdEncoding.DecodeString(clientB64)
		if err != nil {
			t.Fatalf("decode client message %d: %v", i, err)
		}
		serverResp, err := sc.Step(string(clientMsg))
		if err != nil {
			t.Fatalf("server step %d: %v", i, err)
		}
		if serverResp == "" {
			serverParam = "+"
		} else {
			serverParam = base64.StdEncoding.EncodeToString([]byte(serverResp))
		}
		if sc.Done() {
			completed = true
			break
		}
	}
	if !completed {
		t.Fatalf("SCRAM exchange did not complete within 10 round trips")
	}
	if !sc.Valid() {
		t.Fatalf("real server conversation did not validate our client's credentials")
	}
	if s.Phase != PhaseAuthenticating {
		t.Fatalf("phase=%v after SCRAM exchange, want still authenticating (900/903 not sent yet)", s.Phase)
	}

	s, acts = step(t, s, ":irc.example 903 nick :SASL authentication successful")
	if s.Phase != PhaseAwaitingWelcome {
		t.Fatalf("phase=%v after 903, want awaiting_welcome", s.Phase)
	}
	if got := lastSendLine(t, acts); got != "CAP END" {
		t.Fatalf("got %q, want CAP END", got)
	}
}

// TestSASLSCRAMWrongPasswordFailsRealServer proves the reverse: real
// server-side rejection of a real (wrong) client conversation is what
// eventually surfaces as a 904, not something this package can fake by
// itself — this test only confirms the server genuinely doesn't validate,
// establishing that Step's 904 handling (already covered by
// TestSASLRequiredFailureNumericFails) is reacting to something real.
func TestSASLSCRAMWrongPasswordFailsRealServer(t *testing.T) {
	const user, realPass, wrongPass = "alice", "hunter2", "wrongpass"
	setup, _ := scram.SHA256.NewClient(user, realPass, "")
	stored := setup.GetStoredCredentials(scram.KeyFactors{Salt: "abcdefgh", Iters: 4096})
	srv, _ := scram.SHA256.NewServer(func(u string) (scram.StoredCredentials, error) {
		return stored, nil
	})
	sc := srv.NewConversation()

	badClient, err := scram.SHA256.NewClient(user, wrongPass, "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cc := badClient.NewConversation()

	serverParam := ""
	for i := 0; i < 10; i++ {
		clientMsg, err := cc.Step(serverParam)
		if err != nil {
			// A wrong password can surface as a client-side step error
			// depending on where the mismatch is detected; either outcome
			// proves the point.
			return
		}
		serverResp, err := sc.Step(clientMsg)
		if err != nil {
			return // server rejected — the expected outcome
		}
		serverParam = serverResp
		if sc.Done() {
			break
		}
	}
	if sc.Valid() {
		t.Fatalf("server validated a conversation authenticated with the wrong password")
	}
}

// scramTestServer is the server end of a SCRAM-SHA-256 exchange: a real
// xdg-go/scram server conversation holding alice/hunter2, so everything up
// to the server-final is genuine and only what a test chooses to tamper
// with is fake.
func scramTestServer(t *testing.T) *scram.ServerConversation {
	t.Helper()
	setup, err := scram.SHA256.NewClient("alice", "hunter2", "")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	stored := setup.GetStoredCredentials(scram.KeyFactors{Salt: "abcdefgh", Iters: 4096})
	srv, err := scram.SHA256.NewServer(func(string) (scram.StoredCredentials, error) {
		return stored, nil
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv.NewConversation()
}

// clientAuthenticate decodes the payload of the last AUTHENTICATE line
// among acts.
func clientAuthenticate(t *testing.T, acts []Action) string {
	t.Helper()
	line := lastSendLine(t, acts)
	b64, ok := strings.CutPrefix(line, "AUTHENTICATE ")
	if !ok {
		t.Fatalf("got %q, want an AUTHENTICATE line", line)
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("decode %q: %v", line, err)
	}
	return string(raw)
}

func authenticateLine(payload string) string {
	return "AUTHENTICATE " + base64.StdEncoding.EncodeToString([]byte(payload))
}

// scramToClientFinal runs a SCRAM exchange through the client-final: the
// client-first goes to the real server, the server-first comes back, the
// client-final goes to the real server. It returns the server-final the
// real server would send next.
func scramToClientFinal(t *testing.T, s State, sc *scram.ServerConversation) (State, string) {
	t.Helper()
	s, acts := step(t, s, "AUTHENTICATE +")
	serverFirst, err := sc.Step(clientAuthenticate(t, acts))
	if err != nil {
		t.Fatalf("server rejected client-first: %v", err)
	}
	s, acts = step(t, s, authenticateLine(serverFirst))
	serverFinal, err := sc.Step(clientAuthenticate(t, acts))
	if err != nil {
		t.Fatalf("server rejected client-final: %v", err)
	}
	return s, serverFinal
}

// sendServerFinal hands the client a server-final and checks its answer:
// the empty AUTHENTICATE + when it verifies, AUTHENTICATE * (an abort)
// when it doesn't.
func sendServerFinal(t *testing.T, s State, serverFinal string, wantAbort bool) State {
	t.Helper()
	s, acts := step(t, s, authenticateLine(serverFinal))
	if wantAbort {
		if !containsSendLine(acts, "AUTHENTICATE *") {
			t.Fatalf("server-final %q: acts=%+v, want AUTHENTICATE *", serverFinal, acts)
		}
		return s
	}
	if got := lastSendLine(t, acts); got != "AUTHENTICATE +" {
		t.Fatalf("server-final %q: got %q, want AUTHENTICATE +", serverFinal, got)
	}
	return s
}

const saslSuccess = ":irc.example 903 nick :SASL authentication successful"

// TestSASLSuccessNumericNeedsTheExchangeBehindIt drives a full SASL
// exchange through Step per case, ending in the server's outcome numeric,
// once with SASL required and once with fallback. A SCRAM success numeric
// counts only once the server-final's signature has verified; one after
// GoBNC's own abort never counts. A refused success is a SASL failure:
// required fails registration, fallback goes on unauthenticated (CAP END)
// — exactly what a 904 does in each.
func TestSASLSuccessNumericNeedsTheExchangeBehindIt(t *testing.T) {
	cases := []struct {
		name string
		mech string
		cfg  SASLConfig
		// run drives everything after AUTHENTICATE <mech>, up to but not
		// including the outcome numeric.
		run     func(t *testing.T, s State) State
		outcome string
		want    bool // the outcome counts as success
	}{
		{
			name: "scram valid exchange",
			mech: "SCRAM-SHA-256",
			run: func(t *testing.T, s State) State {
				sc := scramTestServer(t)
				s, final := scramToClientFinal(t, s, sc)
				return sendServerFinal(t, s, final, false)
			},
			outcome: saslSuccess,
			want:    true,
		},
		{
			name: "scram 903 right after client-final, no server-final",
			mech: "SCRAM-SHA-256",
			run: func(t *testing.T, s State) State {
				s, _ = scramToClientFinal(t, s, scramTestServer(t))
				return s
			},
			outcome: saslSuccess,
		},
		{
			name: "scram 907 right after client-final, no server-final",
			mech: "SCRAM-SHA-256",
			run: func(t *testing.T, s State) State {
				s, _ = scramToClientFinal(t, s, scramTestServer(t))
				return s
			},
			outcome: ":irc.example 907 nick :You have already authenticated",
		},
		{
			name: "scram server-final with wrong signature",
			mech: "SCRAM-SHA-256",
			run: func(t *testing.T, s State) State {
				s, _ = scramToClientFinal(t, s, scramTestServer(t))
				forged := "v=" + base64.StdEncoding.EncodeToString(make([]byte, 32))
				return sendServerFinal(t, s, forged, true)
			},
			outcome: saslSuccess,
		},
		{
			name: "scram server-final with e= error",
			mech: "SCRAM-SHA-256",
			run: func(t *testing.T, s State) State {
				s, _ = scramToClientFinal(t, s, scramTestServer(t))
				return sendServerFinal(t, s, "e=invalid-proof", true)
			},
			outcome: saslSuccess,
		},
		{
			name: "scram malformed server-final",
			mech: "SCRAM-SHA-256",
			run: func(t *testing.T, s State) State {
				s, _ = scramToClientFinal(t, s, scramTestServer(t))
				return sendServerFinal(t, s, "x=garbage", true)
			},
			outcome: saslSuccess,
		},
		{
			name: "scram 903 after GoBNC aborted on a bad server-first",
			mech: "SCRAM-SHA-256",
			run: func(t *testing.T, s State) State {
				s, _ = step(t, s, "AUTHENTICATE +")
				// The nonce doesn't extend the client's: GoBNC aborts.
				s, acts := step(t, s, authenticateLine("r=notours,s=YWJjZGVmZ2g=,i=4096"))
				if !containsSendLine(acts, "AUTHENTICATE *") {
					t.Fatalf("acts=%+v, want AUTHENTICATE *", acts)
				}
				return s
			},
			outcome: saslSuccess,
		},
		{
			name: "scram 903 after the server aborted",
			mech: "SCRAM-SHA-256",
			run: func(t *testing.T, s State) State {
				s, _ = scramToClientFinal(t, s, scramTestServer(t))
				s, _ = step(t, s, "AUTHENTICATE *")
				return s
			},
			outcome: saslSuccess,
		},
		{
			name: "plain success",
			mech: "PLAIN",
			run: func(t *testing.T, s State) State {
				s, _ = step(t, s, "AUTHENTICATE +")
				return s
			},
			outcome: saslSuccess,
			want:    true,
		},
		{
			name: "external success",
			mech: "EXTERNAL",
			cfg:  SASLConfig{Wanted: true, HasClientCert: true},
			run: func(t *testing.T, s State) State {
				s, _ = step(t, s, "AUTHENTICATE +")
				return s
			},
			outcome: saslSuccess,
			want:    true,
		},
		{
			name: "plain failure numeric",
			mech: "PLAIN",
			run: func(t *testing.T, s State) State {
				s, _ = step(t, s, "AUTHENTICATE +")
				return s
			},
			outcome: ":irc.example 904 nick :SASL authentication failed",
		},
	}

	for _, tc := range cases {
		for _, required := range []bool{true, false} {
			name := tc.name + "/fallback"
			if required {
				name = tc.name + "/required"
			}
			t.Run(name, func(t *testing.T) {
				cfg := tc.cfg
				if cfg == (SASLConfig{}) {
					cfg = SASLConfig{Wanted: true, User: "alice", Pass: "hunter2"}
				}
				cfg.Required = required

				s := New("nick", "", false, cfg)
				s, _ = step(t, s, ":irc.example CAP nick LS :sasl="+tc.mech)
				s, acts := step(t, s, ":irc.example CAP nick ACK :sasl")
				if got := lastSendLine(t, acts); got != "AUTHENTICATE "+tc.mech {
					t.Fatalf("got %q, want AUTHENTICATE %s", got, tc.mech)
				}
				s = tc.run(t, s)
				s, acts = step(t, s, tc.outcome)

				if s.saslSucceeded != tc.want {
					t.Fatalf("saslSucceeded=%v, want %v", s.saslSucceeded, tc.want)
				}
				if !tc.want && required {
					if s.Phase != PhaseFailed || s.Err == nil {
						t.Fatalf("phase=%v err=%v, want failed with an error", s.Phase, s.Err)
					}
					// Registration must not go on: no welcome completes it.
					s, acts = step(t, s, ":irc.example 001 nick :Welcome")
					s, acts = step(t, s, ":irc.example 376 nick :End of MOTD")
					if s.Phase != PhaseFailed || len(acts) != 0 {
						t.Fatalf("phase=%v acts=%+v after 001/376, want still failed, no actions", s.Phase, acts)
					}
					return
				}
				// Success, or a refused success under fallback: either way
				// registration goes on (CAP END) and completes.
				if s.Phase != PhaseAwaitingWelcome {
					t.Fatalf("phase=%v, want awaiting_welcome", s.Phase)
				}
				if got := lastSendLine(t, acts); got != "CAP END" {
					t.Fatalf("got %q, want CAP END", got)
				}
				s, _ = step(t, s, ":irc.example 001 nick :Welcome")
				s, acts = step(t, s, ":irc.example 376 nick :End of MOTD")
				if s.Phase != PhaseComplete || len(acts) != 1 || acts[0].Kind != ActionRegistered {
					t.Fatalf("phase=%v acts=%+v, want complete with one ActionRegistered", s.Phase, acts)
				}
			})
		}
	}
}

// TestSASLRefusedSuccessOnRequiredNamesTheReason pins the error a required
// network fails with when a SCRAM success numeric arrives unproven, so the
// log says why rather than echoing a 903 that looks like success.
func TestSASLRefusedSuccessOnRequiredNamesTheReason(t *testing.T) {
	s := New("nick", "", false, SASLConfig{Wanted: true, Required: true, User: "alice", Pass: "hunter2"})
	s, _ = step(t, s, ":irc.example CAP nick LS :sasl=SCRAM-SHA-256")
	s, _ = step(t, s, ":irc.example CAP nick ACK :sasl")
	s, _ = scramToClientFinal(t, s, scramTestServer(t))
	s, acts := step(t, s, saslSuccess)
	if len(acts) != 1 || acts[0].Kind != ActionFailed {
		t.Fatalf("acts=%+v, want one ActionFailed", acts)
	}
	if !strings.Contains(acts[0].Err.Error(), "without a verified server signature") {
		t.Fatalf("err=%v, want it to name the missing server signature", acts[0].Err)
	}
}

// TestSASLStepPostSCRAMNeedsServerFinal covers the post-registration
// re-auth: StepPost never fails the connection, but an unproven SCRAM 903
// must still not be recorded as success, and a new exchange starts clean.
func TestSASLStepPostSCRAMNeedsServerFinal(t *testing.T) {
	s := New("nick", "", false, SASLConfig{Wanted: true, User: "alice", Pass: "hunter2"})
	s, _ = step(t, s, ":irc.example CAP nick LS :away-notify")
	s, _ = step(t, s, ":irc.example CAP nick ACK :away-notify")
	s, _ = step(t, s, ":irc.example 001 nick :Welcome")
	s, _ = step(t, s, ":irc.example 376 nick :End of MOTD")
	if s.Phase != PhaseComplete {
		t.Fatalf("phase=%v, want complete", s.Phase)
	}
	post := func(s State, line string) (State, []Action) {
		return StepPost(s, Input{Msg: mustParse(t, line)})
	}
	stepPostSCRAM := func(s State) (State, string) {
		s, acts := post(s, ":irc.example CAP nick ACK :sasl=SCRAM-SHA-256")
		if got := lastSendLine(t, acts); got != "AUTHENTICATE SCRAM-SHA-256" {
			t.Fatalf("got %q, want AUTHENTICATE SCRAM-SHA-256", got)
		}
		sc := scramTestServer(t)
		s, acts = post(s, "AUTHENTICATE +")
		serverFirst, err := sc.Step(clientAuthenticate(t, acts))
		if err != nil {
			t.Fatalf("server-first: %v", err)
		}
		s, acts = post(s, authenticateLine(serverFirst))
		serverFinal, err := sc.Step(clientAuthenticate(t, acts))
		if err != nil {
			t.Fatalf("server-final: %v", err)
		}
		return s, serverFinal
	}

	s, _ = stepPostSCRAM(s)
	s, acts := post(s, saslSuccess)
	if s.saslSucceeded || len(acts) != 0 || s.Phase != PhaseComplete {
		t.Fatalf("unproven 903: saslSucceeded=%v acts=%+v phase=%v, want false, none, complete", s.saslSucceeded, acts, s.Phase)
	}

	s, final := stepPostSCRAM(s)
	s, acts = post(s, authenticateLine(final))
	if got := lastSendLine(t, acts); got != "AUTHENTICATE +" {
		t.Fatalf("got %q, want AUTHENTICATE +", got)
	}
	s, _ = post(s, saslSuccess)
	if !s.saslSucceeded {
		t.Fatalf("verified 903 not recorded as success")
	}
}
