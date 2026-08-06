package irc

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestWirePreservesNumericBody(t *testing.T) {
	line := `:irc.example.com 366 me #c :End of /NAMES list.`
	msg, err := Parse(line)
	if err != nil {
		t.Fatal(err)
	}
	// Tag-only change must not reformat the numeric body.
	msg.Tags = map[string]string{"time": "2024-01-01T00:00:00.000Z"}
	got := msg.Wire()
	want := `@time=2024-01-01T00:00:00.000Z ` + line
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	// No tags → exact original.
	msg.Tags = nil
	if msg.Wire() != line {
		t.Fatalf("got %q", msg.Wire())
	}
}

func TestEncodeTrailingColon(t *testing.T) {
	// PRIVMSG/NOTICE: always colon the message text.
	msg := Message{Source: "nick!u@h", Command: "PRIVMSG", Params: []string{"#chan", "hei"}}
	if got := msg.Encode(); got != ":nick!u@h PRIVMSG #chan :hei" {
		t.Fatalf("PRIVMSG: %q", got)
	}
	msg = Message{Command: "NOTICE", Params: []string{"nick", "CAP"}}
	if got := msg.Encode(); got != "NOTICE nick :CAP" {
		t.Fatalf("NOTICE: %q", got)
	}

	// Numerics: no colon when the last param is a plain token.
	msg = Message{Source: "irc.example.com", Command: "366", Params: []string{"me", "#c", "End"}}
	if got := msg.Encode(); got != ":irc.example.com 366 me #c End" {
		t.Fatalf("numeric plain: %q", got)
	}
	msg = Message{Source: "irc.example.com", Command: "366", Params: []string{"me", "#c", "End of /NAMES list."}}
	if got := msg.Encode(); got != ":irc.example.com 366 me #c :End of /NAMES list." {
		t.Fatalf("numeric spaced: %q", got)
	}
}

func TestParseSimple(t *testing.T) {
	msg, err := Parse("PRIVMSG #chan :hello world")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Command != "PRIVMSG" || len(msg.Params) != 2 || msg.Params[1] != "hello world" {
		t.Fatalf("%+v", msg)
	}
}

func TestParsePrefixTags(t *testing.T) {
	line := `@time=2024-01-01T00:00:00.000Z;msgid=abc :nick!u@h PRIVMSG #c :hi`
	msg, err := Parse(line)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Source != "nick!u@h" || msg.Command != "PRIVMSG" {
		t.Fatalf("%+v", msg)
	}
	if msg.Tags["time"] != "2024-01-01T00:00:00.000Z" || msg.Tags["msgid"] != "abc" {
		t.Fatalf("tags=%v", msg.Tags)
	}
}

func TestTagEscapeRoundTrip(t *testing.T) {
	cases := []string{
		`hello`,
		`a;b`,
		`a b`,
		`a\b`,
		"a\rb",
		"a\nb",
		`\:;\s\\\r\n`,
	}
	for _, c := range cases {
		esc := EscapeTagValue(c)
		got, err := UnescapeTagValue(esc)
		if err != nil {
			t.Fatalf("%q: %v", c, err)
		}
		if got != c {
			t.Fatalf("roundtrip %q -> %q -> %q", c, esc, got)
		}
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	lines := []string{
		"PING :123",
		":irc.example.com 001 nick :Welcome",
		":nick!u@h JOIN #chan",
		"PRIVMSG #chan :hello world",
		`@time=2024-01-01T00:00:00.000Z :a!b@c PRIVMSG #d :e`,
		`@a=b\sc :x COMMAND p1 :trailing with spaces`,
	}
	for _, line := range lines {
		msg, err := Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		enc := msg.Encode()
		msg2, err := Parse(enc)
		if err != nil {
			t.Fatalf("reparse %q from %q: %v", enc, line, err)
		}
		if msg2.Command != msg.Command || msg2.Source != msg.Source {
			t.Fatalf("cmd/src mismatch %q -> %q", line, enc)
		}
		if len(msg2.Params) != len(msg.Params) {
			t.Fatalf("params %v vs %v", msg.Params, msg2.Params)
		}
		for i := range msg.Params {
			if msg.Params[i] != msg2.Params[i] {
				t.Fatalf("param %d %q vs %q", i, msg.Params[i], msg2.Params[i])
			}
		}
		for k, v := range msg.Tags {
			if msg2.Tags[k] != v {
				t.Fatalf("tag %s: %q vs %q", k, v, msg2.Tags[k])
			}
		}
	}
}

func TestGoldenBroadCommands(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", "testdata", "messages", "broad.txt")
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var n int
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		n++
		msg, err := Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		enc := msg.Encode()
		msg2, err := Parse(enc)
		if err != nil {
			t.Fatalf("reparse %q from %q: %v", enc, line, err)
		}
		if msg2.Command != msg.Command {
			t.Fatalf("command %q -> %q", msg.Command, msg2.Command)
		}
		if msg2.Trailing() != msg.Trailing() {
			t.Fatalf("trailing %q -> %q for %q", msg.Trailing(), msg2.Trailing(), line)
		}
		for k, v := range msg.Tags {
			if msg2.Tags[k] != v {
				t.Fatalf("tag %s on %q", k, line)
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if n < 30 {
		t.Fatalf("expected many fixtures, got %d", n)
	}
}

func TestGoldenMessages(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", "testdata", "messages", "roundtrip.txt")
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		msg, err := Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		enc := msg.Encode()
		msg2, err := Parse(enc)
		if err != nil {
			t.Fatalf("reparse %q: %v", enc, err)
		}
		if msg2.Command != msg.Command || msg2.Trailing() != msg.Trailing() {
			t.Fatalf("%q -> %q", line, enc)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
}

func FuzzParse(f *testing.F) {
	f.Add("PRIVMSG #c :hi")
	f.Add("@a=b :x!y@z NOTICE #c :z")
	f.Add(":server 005 nick CHANMODES=b,k,l,imnpst :are supported")
	f.Fuzz(func(t *testing.T, s string) {
		msg, err := Parse(s)
		if err != nil {
			return
		}
		enc := msg.Encode()
		_, _ = Parse(enc)
	})
}
