package log

import (
	"bytes"
	"encoding/json"
	"io"
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

func TestSetupDebugConsoleKeepsFileLevel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gobnc.json.log")
	var console bytes.Buffer
	l, closer, err := Setup(Options{
		Level:     "debug",
		FileLevel: "info",
		Console:   &console,
		File:      path,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closer() }()

	l.Debug("noise", "n", 1)
	l.Info("keep", "n", 2)

	if !bytes.Contains(console.Bytes(), []byte("noise")) || !bytes.Contains(console.Bytes(), []byte("keep")) {
		t.Fatalf("console should show both: %s", console.String())
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("noise")) {
		t.Fatalf("file should omit debug: %s", data)
	}
	if !bytes.Contains(data, []byte("keep")) {
		t.Fatalf("file missing info: %s", data)
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
	if got := RedactIRC("AUTHENTICATE +"); got != "AUTHENTICATE +" {
		t.Fatal(got)
	}
	if got := RedactIRC("PRIVMSG #c :hi"); got != "PRIVMSG #c :hi" {
		t.Fatal(got)
	}

	got := RedactIRC("@time=2024-01-01T00:00:00.000Z PASS libera/s3cret")
	if !strings.Contains(got, "PASS libera/***") || strings.Contains(got, "s3cret") {
		t.Fatalf("tagged PASS: %q", got)
	}
	if !strings.HasPrefix(got, "@") {
		t.Fatalf("expected tags preserved: %q", got)
	}

	got = RedactIRC(":nick!u@h PASS net/secret")
	if got != ":nick!u@h PASS net/***" {
		t.Fatalf("prefixed PASS: %q", got)
	}

	got = RedactIRC("@label=x AUTHENTICATE Zm9v")
	if !strings.Contains(got, "AUTHENTICATE ***") || strings.Contains(got, "Zm9v") {
		t.Fatalf("tagged AUTHENTICATE: %q", got)
	}

	got = RedactIRC("@label=x AUTHENTICATE +")
	if !strings.Contains(got, "AUTHENTICATE +") {
		t.Fatalf("tagged AUTHENTICATE +: %q", got)
	}
}

func TestSetupLogFileModeOwnerOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gobnc.json.log")
	_, closer, err := Setup(Options{Level: "info", Console: io.Discard, File: path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closer() }()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("log mode=%04o want owner-only", perm)
	}
}

func TestSetupChmodsWorldReadableLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gobnc.json.log")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	_, closer, err := Setup(Options{Level: "info", Console: io.Discard, File: path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closer() }()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("log mode=%04o want owner-only", perm)
	}
}
