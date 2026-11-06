package push

import (
	"bytes"
	"crypto/ecdh"
	"crypto/hpke"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// RFC 9180 Appendix A.2.1 (DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, ChaCha20-Poly1305, base
// mode): the vector's first encryption (seq 0) opens with its aad. crypto/hpke does not expose
// key / base_nonce, so the AEAD opening the vector's ciphertext is the check.
func TestHPKEConformanceRFC9180A21(t *testing.T) {
	skR, err := ecdh.X25519().NewPrivateKey(mustHex(t, "8057991eef8f1f1af18f4a9491d16a1ce333f695d4db8e38da75975c4478e0fb"))
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(skR.PublicKey().Bytes()); got != "4310ee97d88cc1f088a5576c77ab0cf5c3ac797f3d95139c6c84b5429c59662a" {
		t.Fatalf("pkRm = %s", got)
	}
	k, err := hpke.NewDHKEMPrivateKey(skR)
	if err != nil {
		t.Fatal(err)
	}
	r, err := hpke.NewRecipient(mustHex(t, "1afa08d3dec047a643885163f1180476fa7ddb54c6a8029ea33f95796bf2ac4a"), k,
		hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), mustHex(t, "4f6465206f6e2061204772656369616e2055726e"))
	if err != nil {
		t.Fatal(err)
	}
	pt, err := r.Open([]byte("Count-0"), mustHex(t, "1c5250d8034ec2b784ba2cfd69dbdb8af406cfe3ff938e131f0def8c8b60b4db21993c62ce81883d2dd1b51a28"))
	if err != nil {
		t.Fatal(err)
	}
	if string(pt) != "Beauty is truth, truth beauty" {
		t.Fatalf("pt = %q", pt)
	}
}

func newKey(t *testing.T) (*ecdh.PrivateKey, string) {
	t.Helper()
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k, base64.StdEncoding.EncodeToString(k.PublicKey().Bytes())
}

func TestSealRoundTripAndBlobLength(t *testing.T) {
	priv, pubB64 := newKey(t)
	if len(pubB64) != 44 {
		t.Fatalf("key length %d", len(pubB64))
	}
	pub, err := ParsePublicKey(pubB64)
	if err != nil {
		t.Fatal(err)
	}
	pt, err := MessagePlaintext(Message{EID: NewEventID(), Src: SourceGoBNC, Profile: "P", SessionID: "net-3",
		Network: "Undernet", Buffer: "#rete", Kind: KindHighlight, Sender: "alice", Text: "me: hi", Time: 1762430400, ID: 7})
	if err != nil {
		t.Fatal(err)
	}
	blob, err := Seal(pub, pt)
	if err != nil {
		t.Fatal(err)
	}
	if len(blob) != 48+256 || !ValidBlobLen(len(blob)) {
		t.Fatalf("blob length %d", len(blob))
	}
	got, err := Open(priv, blob)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatalf("round trip: %q", got)
	}
	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil { // trailing spaces are JSON whitespace
		t.Fatal(err)
	}
	if m["v"] != float64(1) || m["type"] != "message" || m["id"] != float64(7) || m["src"] != "gobnc" {
		t.Fatalf("content = %v", m)
	}
	if _, ok := m["action"]; ok {
		t.Fatalf("action present when false: %v", m)
	}
	// Another key does not open it.
	other, _ := newKey(t)
	if _, err := Open(other, blob); err == nil {
		t.Fatal("opened with the wrong key")
	}
	blob[60] ^= 1
	if _, err := Open(priv, blob); err == nil {
		t.Fatal("opened a flipped ciphertext")
	}
}

func TestPlaintextFieldOrder(t *testing.T) {
	pt, err := MessagePlaintext(Message{EID: "e", Src: "gobnc", Profile: "p", SessionID: "net-3", Network: "N",
		Buffer: "#b", Kind: "dm", Sender: "s", Text: "t", Action: true, Time: 5, ID: 9})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"v":1,"type":"message","eid":"e","src":"gobnc","profile":"p","session_id":"net-3","network":"N","buffer":"#b","kind":"dm","sender":"s","text":"t","action":true,"time":5,"id":9}`
	if got := strings.TrimRight(string(pt), " "); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	cl, err := ClearPlaintext(Clear{EID: "c", Src: "gobnc", Profile: "p", SessionID: "net-3", Buffer: "#b", ReadID: 12, Time: 6})
	if err != nil {
		t.Fatal(err)
	}
	want = `{"v":1,"type":"clear","eid":"c","src":"gobnc","profile":"p","session_id":"net-3","buffer":"#b","read_id":12,"time":6}`
	if got := strings.TrimRight(string(cl), " "); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if len(cl) != 256 {
		t.Fatalf("clear padded to %d", len(cl))
	}
}

func TestParsePublicKeyRefusesBadKeys(t *testing.T) {
	for _, s := range []string{"", "abc", base64.StdEncoding.EncodeToString(make([]byte, 31)),
		base64.RawStdEncoding.EncodeToString(make([]byte, 32)), base64.URLEncoding.EncodeToString(bytes.Repeat([]byte{0xfb}, 32))} {
		if _, err := ParsePublicKey(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestCapTextCutsAtCodePoint(t *testing.T) {
	// 1022 ASCII bytes then a 4-byte emoji: over 1024, and the cut lands inside the emoji.
	s := strings.Repeat("a", 1022) + "😀" + "tail"
	got := CapText(s, MaxText)
	if len(got) > MaxText || !utf8.ValidString(got) || !strings.HasSuffix(got, "…") {
		t.Fatalf("len %d valid %v suffix %q", len(got), utf8.ValidString(got), got[len(got)-6:])
	}
	if got != strings.Repeat("a", 1021)+"…" {
		t.Fatalf("got %d bytes", len(got))
	}
	if CapText("short", MaxText) != "short" {
		t.Fatal("short text changed")
	}
}

func TestPlaintextBudget(t *testing.T) {
	// A long network/buffer/sender pushes the object over 2048 with a 1024-byte text: the text is
	// shortened by the excess and the plaintext is exactly 2048 bytes padded.
	m := Message{EID: NewEventID(), Src: SourceGoBNC, Profile: strings.Repeat("p", 64), SessionID: "net-1",
		Network: strings.Repeat("n", 400), Buffer: "#" + strings.Repeat("b", 300), Kind: KindHighlight,
		Sender: strings.Repeat("s", 200), Text: strings.Repeat("x", 3000), Time: 1, ID: 1}
	pt, err := MessagePlaintext(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(pt) != MaxPlaintext {
		t.Fatalf("padded length %d", len(pt))
	}
	var back Message
	if err := json.Unmarshal(pt, &back); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(back.Text, "…") || len(back.Text) >= MaxText {
		t.Fatalf("text not shortened: %d bytes", len(back.Text))
	}
	// Escaped characters take more room than their bytes: still fits.
	m.Text = strings.Repeat("\"", 3000)
	if pt, err = MessagePlaintext(m); err != nil || len(pt) > MaxPlaintext || json.Unmarshal(pt, &back) != nil ||
		!strings.HasSuffix(back.Text, "…") {
		t.Fatalf("escaped: %d %v", len(pt), err)
	}
	// Nothing fits: refused.
	m.Network = strings.Repeat("n", 3000)
	if _, err := MessagePlaintext(m); err != ErrTooLarge {
		t.Fatalf("err = %v", err)
	}
}

func TestStripFormatting(t *testing.T) {
	cases := map[string]string{
		"\x02bold\x02 \x1ditalic\x1d \x1funder\x1f \x1estrike\x1e \x11mono\x11 \x16rev\x16\x0f": "bold italic under strike mono rev",
		"\x0304red\x03 \x034,12both\x03 \x03,x":                                                 "red both ,x",
		"\x04FF0000hex\x04 \x04ff0000,00FF00two":                                               "hex two",
		"plain 5":                                                                               "plain 5",
		"\x03123":                                                                               "3",
	}
	for in, want := range cases {
		if got := StripFormatting(in); got != want {
			t.Errorf("StripFormatting(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidBlobLen(t *testing.T) {
	for n, ok := range map[int]bool{48: false, 304: true, 48 + 2048: true, 48 + 2304: false, 305: false, 0: false} {
		if ValidBlobLen(n) != ok {
			t.Errorf("ValidBlobLen(%d) = %v", n, !ok)
		}
	}
}
