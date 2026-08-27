package registration

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// TestResumeAgainstRealIrcu2Transcript replays a transcript captured live
// from a real resume-capable ircu2 (empus/ircu2 feat/resume, u2.10.12.19)
// over a wss uplink — the server→client half of a successful RESUME — and
// proves our state machine, seeded with a token, drives it to a resumed
// registration. Captured by cmd/resumeprobe against tests/docker's
// ircd-tls-hub; tokens are masked in the fixture and irrelevant here (we
// present our own ResumeToken). This is the resume counterpart to
// TestRegistrationCompletesAgainstRealTranscripts.
func TestResumeAgainstRealIrcu2Transcript(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "testdata", "resume", "ircu2-wss-resume.txt"))
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	// Server→client lines only ("<< "); the client half ("<< " excluded)
	// is what our own Step/actions must reproduce.
	var serverLines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "<< ") {
			serverLines = append(serverLines, strings.TrimPrefix(line, "<< "))
		}
	}
	if len(serverLines) == 0 {
		t.Fatal("no server lines in fixture")
	}

	s := New("gobncprobe-bak", "", false, SASLConfig{})
	s.ResumeToken = "OUR-TOKEN"
	sentResume := false
	sawSuccess := false
	registeredResumed := false

	for _, line := range serverLines {
		msg, err := irc.Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		if msg.Command == "PING" {
			continue // the keeper answers PING; not a Step concern
		}
		var acts []Action
		s, acts = Step(s, Input{Msg: msg})
		for _, a := range acts {
			if a.Kind == ActionSend && strings.HasPrefix(a.Line, "RESUME ") {
				if a.Line != "RESUME OUR-TOKEN" {
					t.Fatalf("presented %q, want our own token", a.Line)
				}
				sentResume = true
			}
			if a.Kind == ActionRegistered {
				registeredResumed = a.Resumed
			}
		}
		if msg.Command == "RESUME" && strings.EqualFold(msg.Param(0), "SUCCESS") {
			sawSuccess = true
		}
	}

	if !sentResume {
		t.Error("state machine never presented RESUME with our token on CAP ACK")
	}
	if !sawSuccess {
		t.Error("fixture is missing RESUME SUCCESS (bad capture?)")
	}
	if s.Phase != PhaseComplete {
		t.Fatalf("final phase=%v, want complete", s.Phase)
	}
	if !s.Resumed || !registeredResumed {
		t.Fatalf("state.Resumed=%v ActionRegistered.Resumed=%v, want both true", s.Resumed, registeredResumed)
	}
	// RESUME SUCCESS handed us the old nick, replacing the -bak we dialled with.
	if s.Nick != "gobncprobe" {
		t.Fatalf("nick=%q, want the resumed session's nick gobncprobe", s.Nick)
	}
}
