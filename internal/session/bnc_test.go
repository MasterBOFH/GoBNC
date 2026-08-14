package session

import (
	"errors"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestBNCHelpAndRejects(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.SetAdmin(func(args []string) ([]string, error) {
		if len(args) == 0 || args[0] == "help" {
			return []string{"BNC commands:", "  help", "  reconnect [<name>]", "  disconnect [<name>]", "  network list"}, nil
		}
		switch args[0] {
		case "stop", "auth", "serve":
			return nil, errors.New(args[0] + " is not available via BNC; use the gobnc CLI")
		case "reconnect":
			return []string{"reconnect requested for n"}, nil
		case "disconnect":
			return []string{"disconnected n"}, nil
		case "network":
			if len(args) > 1 && args[1] == "list" {
				return []string{"n1\thost:6697"}, nil
			}
		}
		return nil, errors.New("unknown command")
	})

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"help"}}); err != nil {
		t.Fatal(err)
	}
	sent := d.snapshot()
	if len(sent) < 2 {
		t.Fatalf("want help notices, got %#v", sent)
	}
	for _, m := range sent {
		if m.Command != "NOTICE" || m.Source != ServerName || m.Param(0) != "me" {
			t.Fatalf("bad notice: %+v", m)
		}
	}
	if !strings.Contains(sent[0].Trailing(), "BNC commands:") {
		t.Fatalf("help text: %q", sent[0].Trailing())
	}

	d.clearSent()
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"stop"}}); err != nil {
		t.Fatal(err)
	}
	sent = d.snapshot()
	if len(sent) != 1 || !strings.Contains(sent[0].Trailing(), "not available via BNC") {
		t.Fatalf("stop reject: %#v", sent)
	}

	d.clearSent()
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"network", "list"}}); err != nil {
		t.Fatal(err)
	}
	sent = d.snapshot()
	if len(sent) != 1 || sent[0].Trailing() != "n1\thost:6697" {
		t.Fatalf("list: %#v", sent)
	}

	d.clearSent()
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"reconnect"}}); err != nil {
		t.Fatal(err)
	}
	sent = d.snapshot()
	if len(sent) != 1 || sent[0].Trailing() != "reconnect requested for n" {
		t.Fatalf("reconnect: %#v", sent)
	}

	d.clearSent()
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"disconnect"}}); err != nil {
		t.Fatal(err)
	}
	sent = d.snapshot()
	if len(sent) != 1 || sent[0].Trailing() != "disconnected n" {
		t.Fatalf("disconnect: %#v", sent)
	}
}

func TestBNCUnavailable(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC"}); err != nil {
		t.Fatal(err)
	}
	sent := d.snapshot()
	if len(sent) != 1 || sent[0].Trailing() != "BNC unavailable" {
		t.Fatalf("%#v", sent)
	}
}
