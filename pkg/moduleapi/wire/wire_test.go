package wire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func readOne(t *testing.T, v any) Frame {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteFrame(&buf, v); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(buf.Bytes(), []byte("\n")) || bytes.Count(buf.Bytes(), []byte("\n")) != 1 {
		t.Fatalf("frame %q is not one newline-terminated line", buf.String())
	}
	f, err := ReadFrame(bufio.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRoundTripRequest(t *testing.T) {
	f := readOne(t, Request{ID: 7, Method: MethodEventsSubscribe, Params: json.RawMessage(`{"network":3}`)})
	if f.Kind != KindRequest || f.Request.ID != 7 || f.Request.Method != MethodEventsSubscribe ||
		string(f.Request.Params) != `{"network":3}` {
		t.Fatalf("got %+v / %+v", f, f.Request)
	}
}

func TestRoundTripResponse(t *testing.T) {
	f := readOne(t, Response{ID: 9, Result: json.RawMessage(`{"ok":true}`)})
	if f.Kind != KindResponse || f.Response.ID != 9 || string(f.Response.Result) != `{"ok":true}` || f.Response.Error != nil {
		t.Fatalf("got %+v / %+v", f, f.Response)
	}
	f = readOne(t, Response{ID: 10, Error: &Error{Code: CodeNotGranted, Message: "send"}})
	if f.Kind != KindResponse || f.Response.Error == nil || f.Response.Error.Code != CodeNotGranted {
		t.Fatalf("got %+v / %+v", f, f.Response)
	}
}

func TestRoundTripEvent(t *testing.T) {
	ev, err := NewEvent(EventReady, Empty{})
	if err != nil {
		t.Fatal(err)
	}
	f := readOne(t, ev)
	if f.Kind != KindEvent || f.Event.Event != EventReady || string(f.Event.Data) != `{}` {
		t.Fatalf("got %+v / %+v", f, f.Event)
	}
}

func TestReadFrameSizeLimit(t *testing.T) {
	// Exactly MaxFrameSize bytes is accepted.
	pad := strings.Repeat("x", MaxFrameSize-len(`{"event":"e","data":""}`))
	ok := `{"event":"e","data":"` + pad + `"}`
	if len(ok) != MaxFrameSize {
		t.Fatalf("test line is %d bytes", len(ok))
	}
	if _, err := ReadFrame(bufio.NewReader(strings.NewReader(ok + "\n"))); err != nil {
		t.Fatalf("1 MiB frame: %v", err)
	}
	tooLong := `{"event":"e","data":"x` + pad + `"}`
	if _, err := ReadFrame(bufio.NewReader(strings.NewReader(tooLong + "\n"))); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("1 MiB + 1 frame: err = %v, want ErrFrameTooLarge", err)
	}
	// Also without a terminating newline in sight.
	if _, err := ReadFrame(bufio.NewReader(strings.NewReader(tooLong + tooLong))); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("unterminated oversized frame: err = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameRejectsUnclassifiable(t *testing.T) {
	if _, err := ReadFrame(bufio.NewReader(strings.NewReader(`{"foo":1}` + "\n"))); err == nil {
		t.Fatal("frame with neither method, event nor id accepted")
	}
	if _, err := ReadFrame(bufio.NewReader(strings.NewReader("not json\n"))); err == nil {
		t.Fatal("non-JSON frame accepted")
	}
}

func golden(t *testing.T, v any, want string) {
	t.Helper()
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("JSON mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestSnapshotJSONNames(t *testing.T) {
	golden(t, Snapshot{
		Network: 1, State: NetRegistered, LastID: 42, Seq: 7,
		Self:     Self{Nick: "me", User: "u", Host: "h", Account: "acct", UModes: "+i", Away: false},
		ISupport: map[string]string{"CASEMAPPING": "rfc1459"},
		Caps:     []string{"server-time"},
		Channels: []Channel{{
			Name: "#Rete", Key: "#rete", Topic: Topic{Text: "hi", Setter: "op", Time: "2026-10-23T20:00:00Z"},
			Modes: "+nt", ChanKey: "", MembersComplete: true,
			Members: []Member{{Nick: "op", Prefixes: "@", User: "o", Host: "oh", Account: "", Away: true}},
		}},
	}, `{"network":1,"state":"registered","last_id":42,"seq":7,"self":{"nick":"me","user":"u","host":"h","account":"acct","umodes":"+i","away":false},`+
		`"isupport":{"CASEMAPPING":"rfc1459"},"caps":["server-time"],"channels":[{"name":"#Rete","key":"#rete",`+
		`"topic":{"text":"hi","setter":"op","time":"2026-10-23T20:00:00Z"},"modes":"+nt","chankey":"","members_complete":true,`+
		`"members":[{"nick":"op","prefixes":"@","user":"o","host":"oh","account":"","away":true}]}]}`)
}

func TestLineEventJSONNames(t *testing.T) {
	golden(t, LineEvent{
		Network: 1, Seq: 5, Kind: LineMessage, Raw: ":a!b@c PRIVMSG #x :hi", MsgID: "m1",
		Time: "2026-10-23T20:00:00.5Z", Source: "a!b@c", Nick: "a", Self: false,
		Buffers: []LineBuffer{{Name: "#X", Key: "#x", ID: 9}, {Name: "a", Key: "a"}},
	}, `{"network":1,"seq":5,"kind":"message","raw":":a!b@c PRIVMSG #x :hi","msgid":"m1","time":"2026-10-23T20:00:00.5Z",`+
		`"source":"a!b@c","nick":"a","self":false,"buffers":[{"name":"#X","key":"#x","id":9},{"name":"a","key":"a"}]}`)
	golden(t, LineEvent{Kind: LineMessage, Label: "L1", Buffers: []LineBuffer{}},
		`{"network":0,"seq":0,"kind":"message","raw":"","msgid":"","time":"","source":"","nick":"","self":false,"label":"L1","buffers":[]}`)
}

func TestHistoryMessageJSONNames(t *testing.T) {
	golden(t, HistoryMessage{ID: 3, Time: "2026-10-23T20:00:00.123456789Z", MsgID: "m", Command: "PRIVMSG",
		Source: "a!b@c", Buffer: "#x", Raw: "raw", Text: "hi"},
		`{"id":3,"time":"2026-10-23T20:00:00.123456789Z","msgid":"m","command":"PRIVMSG","source":"a!b@c","buffer":"#x","raw":"raw","text":"hi"}`)
}

func TestEveryFeatureHasMethods(t *testing.T) {
	for _, f := range []string{FeatureNetworks, FeatureEvents, FeatureHistory, FeatureReadMarkers, FeatureClients,
		FeatureSend, FeatureDevices, FeatureAuth, FeatureStorage, FeatureCommands} {
		if !KnownFeature(f) || len(FeatureMethods[f]) == 0 {
			t.Errorf("feature %q has no methods", f)
		}
	}
	if KnownFeature("bogus") {
		t.Error("unknown feature reported known")
	}
}
