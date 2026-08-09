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

func TestParamsTextPASS(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{"PASS Undernet/secret", "Undernet/secret"},
		{"PASS :Undernet/secret", "Undernet/secret"},
		{"PASS Undernet/sec ret", "Undernet/sec ret"},
		{"PASS :Undernet/sec ret", "Undernet/sec ret"},
		{"PASS Undernet/12345", "Undernet/12345"},
		{"PASS :Undernet/12345", "Undernet/12345"},
		{"PASS Undernet 12345", "Undernet 12345"},
		{"@label=1 PASS net/pass", "net/pass"},
		{"@label=1 PASS :net/pass with spaces", "net/pass with spaces"},
		{"PASS", ""},
		{"PASS :", ""},
	}
	for _, tc := range cases {
		msg, err := Parse(tc.line)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.line, err)
		}
		if got := msg.ParamsText(); got != tc.want {
			t.Fatalf("ParamsText(%q)=%q want %q (params=%q)", tc.line, got, tc.want, msg.Params)
		}
	}
	// Constructed message without Raw joins params.
	msg := Message{Command: "PASS", Params: []string{"net/a", "b", "c"}}
	if got := msg.ParamsText(); got != "net/a b c" {
		t.Fatalf("constructed: %q", got)
	}
}

func TestEncodeTrailingColon(t *testing.T) {
	// Encode always trailing-encodes the last param (synthesized lines only).
	msg := Message{Source: "nick!u@h", Command: "PRIVMSG", Params: []string{"#chan", "hei"}}
	if got := msg.Encode(); got != ":nick!u@h PRIVMSG #chan :hei" {
		t.Fatalf("PRIVMSG: %q", got)
	}
	msg = Message{Command: "NOTICE", Params: []string{"nick", "CAP"}}
	if got := msg.Encode(); got != "NOTICE nick :CAP" {
		t.Fatalf("NOTICE: %q", got)
	}
	msg = Message{Command: "PING", Params: []string{"gobnc"}}
	if got := msg.Encode(); got != "PING :gobnc" {
		t.Fatalf("PING: %q", got)
	}
	msg = Message{Command: "PONG", Params: []string{"gobnc"}}
	if got := msg.Encode(); got != "PONG :gobnc" {
		t.Fatalf("PONG: %q", got)
	}
	msg = Message{Source: "gobnc", Command: "353", Params: []string{"MrIron", "=", "#iron-dev", "@MrIron"}}
	if got := msg.Encode(); got != ":gobnc 353 MrIron = #iron-dev :@MrIron" {
		t.Fatalf("353: %q", got)
	}
	msg = Message{Source: "gobnc", Command: "332", Params: []string{"me", "#c", "hi"}}
	if got := msg.Encode(); got != ":gobnc 332 me #c :hi" {
		t.Fatalf("332: %q", got)
	}
	msg = Message{Source: "irc.example.com", Command: "366", Params: []string{"me", "#c", "End"}}
	if got := msg.Encode(); got != ":irc.example.com 366 me #c :End" {
		t.Fatalf("numeric last param: %q", got)
	}
	msg = Message{Source: "irc.example.com", Command: "366", Params: []string{"me", "#c", "End of /NAMES list."}}
	if got := msg.Encode(); got != ":irc.example.com 366 me #c :End of /NAMES list." {
		t.Fatalf("numeric spaced: %q", got)
	}
}

func TestWireNeverReencodesUpstreamBody(t *testing.T) {
	// Unusual colonation must survive tag injection — no Encode round-trip.
	line := `:irc.example.com 353 me = #c @nick`
	msg, err := Parse(line)
	if err != nil {
		t.Fatal(err)
	}
	msg.Tags = map[string]string{"time": "2024-01-01T00:00:00.000Z"}
	got := msg.Wire()
	if got != `@time=2024-01-01T00:00:00.000Z `+line {
		t.Fatalf("rewrote body: %q", got)
	}
	msg.Tags = nil
	if msg.Wire() != line {
		t.Fatalf("stripped tags mangled body: %q", msg.Wire())
	}
}

func TestWirePreservesBodyWhenTagsChange(t *testing.T) {
	// Server sent a one-word QUIT reason with a colon; keep that body when injecting tags.
	line := ":nick!u@h QUIT :Quit"
	msg, err := Parse(line)
	if err != nil {
		t.Fatal(err)
	}
	msg.Tags = map[string]string{"msgid": "abc", "time": "2026-08-07T12:00:00.000Z"}
	got := msg.Wire()
	if !strings.HasSuffix(got, " QUIT :Quit") {
		t.Fatalf("body mangled: %q", got)
	}
	if !strings.Contains(got, "msgid=abc") {
		t.Fatalf("missing msgid: %q", got)
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
