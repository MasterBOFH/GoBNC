package log

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewLevels(t *testing.T) {
	var buf bytes.Buffer
	l := New("debug", &buf)
	l.Debug("hello", "k", 1)
	if !bytes.Contains(buf.Bytes(), []byte("hello")) {
		t.Fatalf("%s", buf.String())
	}
}

func TestWith(t *testing.T) {
	var buf bytes.Buffer
	l, _, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	l = With(l, "net", "libera")
	l.Info("up")
	if !bytes.Contains(buf.Bytes(), []byte(`"net":"libera"`)) {
		t.Fatalf("missing attr: %s", buf.String())
	}
}

func TestSetupDebugTextAndJSONFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gobnc.json.log")
	var console bytes.Buffer
	l, closer, err := Setup(Options{Level: "debug", Console: &console, File: path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closer() }()

	l.Debug("probe", "n", 1)

	if bytes.Contains(console.Bytes(), []byte(`{"time"`)) {
		t.Fatalf("console should be pretty text, got %s", console.String())
	}
	if !bytes.Contains(console.Bytes(), []byte("probe")) {
		t.Fatalf("console missing msg: %s", console.String())
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("file not JSON: %s (%v)", data, err)
	}
	if m["msg"] != "probe" {
		t.Fatalf("%v", m)
	}
}

func TestPrettyIRCLine(t *testing.T) {
	var buf bytes.Buffer
	l, _, err := Setup(Options{Level: "debug", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	IRC(l, "uplink/ircu2", "<<", ":server 001 nick :Welcome")
	out := buf.String()
	if !strings.Contains(out, "<<") || !strings.Contains(out, "uplink/ircu2") || !strings.Contains(out, "001") {
		t.Fatalf("%q", out)
	}
}

func TestRedactIRC(t *testing.T) {
	if got := RedactIRC("PASS libera/s3cret"); got != "PASS libera/***" {
		t.Fatal(got)
	}
	if got := RedactIRC("AUTHENTICATE abcdef"); got != "AUTHENTICATE ***" {
		t.Fatal(got)
	}
	if got := RedactIRC("PRIVMSG #c :hi"); got != "PRIVMSG #c :hi" {
		t.Fatal(got)
	}
}
