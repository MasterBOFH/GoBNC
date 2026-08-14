package keeper

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	hello := HelloMsg{ClientVersion: 1, Mode: ModeLive, FromSeq: map[NetworkID]uint64{7: 42}}
	if err := writeFrame(&buf, msgHello, hello); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	gotType, body, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if gotType != msgHello {
		t.Fatalf("type=%v, want msgHello", gotType)
	}
	got, err := decodeFrame[HelloMsg](gotType, msgHello, body)
	if err != nil {
		t.Fatalf("decodeFrame: %v", err)
	}
	if !reflect.DeepEqual(got, hello) {
		t.Fatalf("got %+v, want %+v", got, hello)
	}
}

func TestFrameEmptyRejected(t *testing.T) {
	var buf bytes.Buffer
	var hdr [4]byte // length = 0
	buf.Write(hdr[:])
	if _, _, err := readFrame(&buf); err == nil {
		t.Fatalf("readFrame accepted a zero-length frame")
	}
}

func TestFrameOversizedRejected(t *testing.T) {
	var buf bytes.Buffer
	var hdr [4]byte
	// Claim a length far beyond maxFrameSize without actually writing that
	// much data — readFrame must reject based on the claimed length alone,
	// not attempt to allocate/read it.
	hdr[0] = 0xFF
	hdr[1] = 0xFF
	hdr[2] = 0xFF
	hdr[3] = 0xFF
	buf.Write(hdr[:])
	if _, _, err := readFrame(&buf); err == nil {
		t.Fatalf("readFrame accepted an oversized frame length")
	}
}

func TestDecodeFrameTypeMismatch(t *testing.T) {
	var buf bytes.Buffer
	_ = writeFrame(&buf, msgHello, HelloMsg{ClientVersion: 1})
	gotType, body, _ := readFrame(&buf)
	if _, err := decodeFrame[HelloAckMsg](gotType, msgHelloAck, body); err == nil {
		t.Fatalf("decodeFrame accepted a body of the wrong declared type")
	}
}

// TestUnknownFieldIgnoredNotRejected is the property the protocol's whole
// value proposition depends on: an old keeper must be able to decode a
// frame from a newer brain that added a field it doesn't know about, and
// vice versa. json.Unmarshal into a struct silently skips unrecognized
// keys unless DisallowUnknownFields was used — verify that directly rather
// than trust it.
func TestUnknownFieldIgnoredNotRejected(t *testing.T) {
	raw := []byte(`{"client_version":1,"mode":"live","from_seq":{"7":42},"a_field_from_the_future":{"nested":true},"another_one":42}`)
	var hello HelloMsg
	if err := json.Unmarshal(raw, &hello); err != nil {
		t.Fatalf("unmarshal with unknown fields failed, want it ignored: %v", err)
	}
	if hello.ClientVersion != 1 || hello.Mode != ModeLive || hello.FromSeq[7] != 42 {
		t.Fatalf("known fields not decoded correctly: %+v", hello)
	}

	// Same property through the actual frame path, not just raw json.Unmarshal.
	var buf bytes.Buffer
	var hdr [4]byte
	body := raw
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)+1))
	buf.Write(hdr[:])
	buf.WriteByte(byte(msgHello))
	buf.Write(body)

	gotType, gotBody, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	got, err := decodeFrame[HelloMsg](gotType, msgHello, gotBody)
	if err != nil {
		t.Fatalf("decodeFrame with unknown fields: %v", err)
	}
	if got.ClientVersion != 1 || got.Mode != ModeLive || got.FromSeq[7] != 42 {
		t.Fatalf("known fields not decoded correctly via frame path: %+v", got)
	}
}

// TestLineMsgPreservesNonUTF8Bytes is the other property the protocol
// depends on: IRC lines are not guaranteed valid UTF-8 (Latin-1 networks
// exist), and LineMsg.Raw must survive a marshal/unmarshal round trip
// byte-for-byte. A []byte field is base64-encoded by encoding/json without
// interpreting content; a string field would not have this property — a
// Go string containing invalid UTF-8, marshaled to JSON, gets its invalid
// bytes silently replaced with U+FFFD.
func TestLineMsgPreservesNonUTF8Bytes(t *testing.T) {
	// Latin-1 "café" ("caf" + 0xE9), not valid UTF-8.
	raw := []byte{'c', 'a', 'f', 0xE9, ':', 'h', 'i'}

	var buf bytes.Buffer
	msg := LineMsg{Network: 7, Seq: 1, Epoch: 1, Raw: raw, Time: time.Unix(1700000000, 0).UTC()}
	if err := writeFrame(&buf, msgLine, msg); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	gotType, body, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	got, err := decodeFrame[LineMsg](gotType, msgLine, body)
	if err != nil {
		t.Fatalf("decodeFrame: %v", err)
	}
	if !bytes.Equal(got.Raw, raw) {
		t.Fatalf("Raw = %v (%q), want %v (%q) — non-UTF8 bytes were mangled", got.Raw, got.Raw, raw, raw)
	}
}

func TestNegotiateVersion(t *testing.T) {
	cases := []struct {
		client  int
		want    int
		wantErr bool
	}{
		{client: 1, want: 1},
		{client: 99, want: keeperMaxVersion}, // newer client, keeper negotiates down
		{client: 0, wantErr: true},
		{client: -1, wantErr: true},
	}
	for _, c := range cases {
		got, err := negotiateVersion(c.client)
		if c.wantErr {
			if err == nil {
				t.Errorf("client=%d: got nil error, want error", c.client)
			}
			continue
		}
		if err != nil {
			t.Errorf("client=%d: unexpected error: %v", c.client, err)
		}
		if got != c.want {
			t.Errorf("client=%d: negotiated=%d, want %d", c.client, got, c.want)
		}
	}
}
