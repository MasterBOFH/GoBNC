package registration

import (
	"encoding/base64"
	"testing"
)

// stepPost mirrors step for StepPost.
func stepPost(t *testing.T, s State, line string) (State, []Action) {
	t.Helper()
	return StepPost(s, Input{Msg: mustParse(t, line)})
}

// registeredWithoutSASL runs a registration where the server never
// offered sasl (services down at connect time) to PhaseComplete.
func registeredWithoutSASL(t *testing.T, cfg SASLConfig) State {
	t.Helper()
	s := New("nick", "", false, cfg)
	s, _ = step(t, s, "CAP * LS :message-tags")
	s, _ = step(t, s, "CAP * ACK :message-tags")
	s, _ = step(t, s, ":server 001 nick :Welcome")
	s, _ = step(t, s, ":server 376 nick :End of MOTD")
	if s.Phase != PhaseComplete {
		t.Fatalf("phase = %v, want complete", s.Phase)
	}
	return s
}

// TestStepPostAuthsAfterCapNewSASL: sasl arriving via CAP NEW after an
// unauthenticated registration, then ACK'd (Session's re-REQ), runs the
// full PLAIN exchange with the mechs CAP NEW carried, and the outcome
// leaves the State registered and idle — a second ACK afterwards starts
// a fresh exchange (nothing stale left over).
func TestStepPostAuthsAfterCapNewSASL(t *testing.T) {
	s := registeredWithoutSASL(t, SASLConfig{Wanted: true, User: "acct", Pass: "secret"})

	s, acts := stepPost(t, s, "CAP * NEW :sasl=PLAIN,EXTERNAL")
	if len(acts) != 0 {
		t.Fatalf("CAP NEW alone must not act (Session decides whether to REQ): %+v", acts)
	}
	s, acts = stepPost(t, s, "CAP * ACK :sasl")
	if got := lastSendLine(t, acts); got != "AUTHENTICATE PLAIN" {
		t.Fatalf("after ACK: %q", got)
	}
	if s.Phase != PhaseComplete {
		t.Fatalf("phase must stay complete during re-auth, got %v", s.Phase)
	}
	s, acts = stepPost(t, s, "AUTHENTICATE +")
	want := "AUTHENTICATE " + base64.StdEncoding.EncodeToString([]byte("\x00acct\x00secret"))
	if got := lastSendLine(t, acts); got != want {
		t.Fatalf("payload: %q want %q", got, want)
	}
	s, acts = stepPost(t, s, ":server 900 nick nick!u@h acct :You are now logged in as acct")
	if len(acts) != 0 {
		t.Fatalf("900: %+v", acts)
	}
	s, acts = stepPost(t, s, ":server 903 nick :SASL authentication successful")
	if len(acts) != 0 || s.Phase != PhaseComplete || s.saslMech != "" {
		t.Fatalf("903: acts=%+v phase=%v mech=%q", acts, s.Phase, s.saslMech)
	}

	// Idle again: a later re-ACK (services flapped once more) restarts.
	_, acts = stepPost(t, s, "CAP * ACK :sasl")
	if got := lastSendLine(t, acts); got != "AUTHENTICATE PLAIN" {
		t.Fatalf("second exchange: %q", got)
	}
}

// TestStepPostFailureNeverFailsRegisteredConnection: Required governs
// whether registration may complete unauthenticated, not whether an
// already-registered connection survives a failed re-auth — a 904 (or an
// aborted exchange) post-registration must emit no ActionFailed and
// leave Phase complete, and must never send CAP END.
func TestStepPostFailureNeverFailsRegisteredConnection(t *testing.T) {
	for _, outcome := range []string{":server 904 nick :SASL authentication failed", "AUTHENTICATE bogus-for-abort"} {
		s := registeredWithoutSASL(t, SASLConfig{Wanted: true, Required: true, User: "acct", Pass: "secret"})
		s, _ = stepPost(t, s, "CAP * NEW :sasl=PLAIN")
		s, _ = stepPost(t, s, "CAP * ACK :sasl")
		var acts []Action
		if outcome[0] == 'A' {
			// Mechanism mismatch on the wire makes stepAuthenticate abort.
			s.saslMech = "UNKNOWN-MECH"
			s, acts = stepPost(t, s, "AUTHENTICATE +")
		} else {
			s, acts = stepPost(t, s, outcome)
		}
		for _, a := range acts {
			if a.Kind == ActionFailed {
				t.Fatalf("%s: ActionFailed post-registration: %+v", outcome, acts)
			}
		}
		if containsSendLine(acts, "CAP END") {
			t.Fatalf("%s: CAP END post-registration: %+v", outcome, acts)
		}
		if s.Phase != PhaseComplete || s.Err != nil || s.saslMech != "" {
			t.Fatalf("%s: phase=%v err=%v mech=%q", outcome, s.Phase, s.Err, s.saslMech)
		}
	}
}

// TestStepPostIgnoresUnwantedOrPassthrough: a post-registration sasl ACK
// with SASL not Wanted is a client's passthrough REQ — never the
// bouncer's business; and a State still registering is Step's, not
// StepPost's.
func TestStepPostIgnoresUnwantedOrPassthrough(t *testing.T) {
	s := registeredWithoutSASL(t, SASLConfig{Wanted: false})
	s, _ = stepPost(t, s, "CAP * NEW :sasl=PLAIN")
	if _, acts := stepPost(t, s, "CAP * ACK :sasl"); len(acts) != 0 {
		t.Fatalf("passthrough ACK must not start bouncer SASL: %+v", acts)
	}
	reg := New("nick", "", false, SASLConfig{Wanted: true, User: "a", Pass: "b"})
	reg.Offered["sasl"] = "PLAIN"
	if _, acts := stepPost(t, reg, "CAP * ACK :sasl"); len(acts) != 0 {
		t.Fatalf("StepPost must no-op while still registering: %+v", acts)
	}
}
