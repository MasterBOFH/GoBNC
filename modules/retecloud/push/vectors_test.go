package push

import (
	"bytes"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

// The shared Rete vectors (retecloud-api documentation/push-test-vectors.json, copied unchanged
// into testdata): the content builder reproduces each plaintext byte for byte, a test-only
// RFC 9180 sealer that takes the ephemeral key reproduces each sealed blob, and a receiver
// following PUSH_ENCRYPTION.md's Opening steps reaches each case's expect.

type vectorFile struct {
	Version int          `json:"version"`
	Info    string       `json:"info"`
	Cases   []vectorCase `json:"cases"`
}

type vectorCase struct {
	Name          string   `json:"name"`
	RecipientPriv string   `json:"recipient_private_key"`
	RecipientPub  string   `json:"recipient_public_key"`
	PreviousPrivs []string `json:"previous_private_keys"`
	EphemeralPriv string   `json:"ephemeral_private_key"`
	Info          string   `json:"info"`
	PlaintextJSON string   `json:"plaintext_json"`
	PaddedB64     string   `json:"padded_plaintext"`
	TextInput     *string  `json:"text_input"`
	Now           int64    `json:"now"`
	Expect        string   `json:"expect"`
	APNsPayload   struct {
		Rete struct {
			V   int    `json:"v"`
			EID string `json:"eid"`
			Enc string `json:"enc"`
		} `json:"rete"`
	} `json:"apns_payload"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	raw, err := os.ReadFile("testdata/push-test-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var f vectorFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.Version != 1 || f.Info != Info || len(f.Cases) < 11 {
		t.Fatalf("vector file: version %d info %q, %d cases", f.Version, f.Info, len(f.Cases))
	}
	return f
}

func b64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func x25519Priv(t *testing.T, s string) *ecdh.PrivateKey {
	t.Helper()
	k, err := ecdh.X25519().NewPrivateKey(b64(t, s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// buildPlaintext rebuilds a case's plaintext with the content builder, from the case's own
// fields (and text_input where the case caps a text).
func buildPlaintext(t *testing.T, c vectorCase) []byte {
	t.Helper()
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(c.PlaintextJSON), &probe); err != nil {
		t.Fatal(err)
	}
	if probe.Type == TypeClear {
		var cl Clear
		if err := json.Unmarshal([]byte(c.PlaintextJSON), &cl); err != nil {
			t.Fatal(err)
		}
		pt, err := ClearPlaintext(cl)
		if err != nil {
			t.Fatal(err)
		}
		return pt
	}
	var m Message
	if err := json.Unmarshal([]byte(c.PlaintextJSON), &m); err != nil {
		t.Fatal(err)
	}
	if c.TextInput != nil {
		m.Text = *c.TextInput
	}
	pt, err := MessagePlaintext(m)
	if err != nil {
		t.Fatal(err)
	}
	return pt
}

func TestRetePushVectors(t *testing.T) {
	f := loadVectors(t)
	for _, c := range f.Cases {
		t.Run(c.Name, func(t *testing.T) {
			if c.Info != Info {
				t.Fatalf("info %q", c.Info)
			}
			padded := b64(t, c.PaddedB64)
			blob := b64(t, c.APNsPayload.Rete.Enc)

			// The builder: byte-exact plaintext and padding.
			if c.PlaintextJSON != "" {
				built := buildPlaintext(t, c)
				if got := bytes.TrimRight(built, " "); string(got) != c.PlaintextJSON {
					t.Fatalf("plaintext\n got  %s\n want %s", got, c.PlaintextJSON)
				}
				if c.Name != "bad-length" && !bytes.Equal(built, padded) {
					t.Fatalf("padded plaintext differs (%d vs %d bytes)", len(built), len(padded))
				}
			}

			// The sealer: the same blob from the same ephemeral key, wrapped as the relay gets it.
			recipient := x25519Priv(t, c.RecipientPriv).PublicKey()
			if len(c.PreviousPrivs) > 0 {
				recipient = x25519Priv(t, c.PreviousPrivs[0]).PublicKey()
			}
			switch c.Name {
			case "wrong-recipient", "flipped-bit":
				// Sealed to a key the file does not hold / damaged after sealing.
			default:
				sealed := sealWithEphemeral(t, recipient, x25519Priv(t, c.EphemeralPriv), padded)
				if !bytes.Equal(sealed, blob) {
					t.Fatalf("sealed blob differs from the vector's enc")
				}
				req := NewRelayRequest(TypeMessage, c.APNsPayload.Rete.EID, 0, sealed)
				if req.Enc != c.APNsPayload.Rete.Enc || req.EventID != c.APNsPayload.Rete.EID {
					t.Fatalf("relay request enc/event_id differ")
				}
			}

			// A receiver reaches expect.
			if got := receive(t, c, blob); got != c.Expect {
				t.Fatalf("receiver: %s, want %s", got, c.Expect)
			}
		})
	}
}

// receive follows PUSH_ENCRYPTION.md's Opening steps 1-4 (and the relay's length check).
func receive(t *testing.T, c vectorCase, blob []byte) string {
	if !ValidBlobLen(len(blob)) {
		return "relay_refuses"
	}
	if c.APNsPayload.Rete.V != 1 {
		return "fallback"
	}
	keys := []string{c.RecipientPriv}
	keys = append(keys, c.PreviousPrivs...)
	var pt []byte
	for _, k := range keys {
		if out, err := Open(x25519Priv(t, k), blob); err == nil {
			pt = out
			break
		}
	}
	if pt == nil {
		return "fallback"
	}
	var content struct {
		V    int    `json:"v"`
		EID  string `json:"eid"`
		Time int64  `json:"time"`
	}
	if json.Unmarshal(pt, &content) != nil || content.V != 1 || content.EID != c.APNsPayload.Rete.EID ||
		c.Now-content.Time > 26*3600 {
		return "fallback"
	}
	return "open"
}

// sealWithEphemeral is RFC 9180 base-mode Seal (DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
// ChaCha20-Poly1305, info "rete push v1", empty aad, seq 0) with a given ephemeral key; test
// only, since crypto/hpke takes none. Checked against RFC 9180 A.2.1 below.
func sealWithEphemeral(t *testing.T, pkR *ecdh.PublicKey, skE *ecdh.PrivateKey, pt []byte) []byte {
	return sealEphemeralInfo(t, pkR, skE, []byte(Info), nil, pt)
}

func sealEphemeralInfo(t *testing.T, pkR *ecdh.PublicKey, skE *ecdh.PrivateKey, info, aad, pt []byte) []byte {
	t.Helper()
	dh, err := skE.ECDH(pkR)
	if err != nil {
		t.Fatal(err)
	}
	enc := skE.PublicKey().Bytes()
	kemSuite := []byte("KEM\x00\x20")
	eaePRK := labeledExtract(t, kemSuite, nil, "eae_prk", dh)
	shared := labeledExpand(t, kemSuite, eaePRK, "shared_secret", append(append([]byte{}, enc...), pkR.Bytes()...), 32)

	suite := []byte("HPKE\x00\x20\x00\x01\x00\x03")
	pskIDHash := labeledExtract(t, suite, nil, "psk_id_hash", nil)
	infoHash := labeledExtract(t, suite, nil, "info_hash", info)
	ctx := append(append([]byte{0x00}, pskIDHash...), infoHash...)
	secret := labeledExtract(t, suite, shared, "secret", nil)
	key := labeledExpand(t, suite, secret, "key", ctx, 32)
	nonce := labeledExpand(t, suite, secret, "base_nonce", ctx, 12)
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		t.Fatal(err)
	}
	return append(enc, aead.Seal(nil, nonce, pt, aad)...)
}

func labeledExtract(t *testing.T, suite, salt []byte, label string, ikm []byte) []byte {
	t.Helper()
	in := append(append(append([]byte("HPKE-v1"), suite...), label...), ikm...)
	prk, err := hkdf.Extract(sha256.New, in, salt)
	if err != nil {
		t.Fatal(err)
	}
	return prk
}

func labeledExpand(t *testing.T, suite, prk []byte, label string, info []byte, n int) []byte {
	t.Helper()
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(n))
	in := append(append(append(append(l[:], "HPKE-v1"...), suite...), label...), info...)
	out, err := hkdf.Expand(sha256.New, prk, string(in), n)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The test-only sealer reproduces RFC 9180 A.2.1's first encryption from its ephemeral key
// (skEm derived from the vector's ikmE with the KEM's DeriveKeyPair).
func TestEphemeralSealerMatchesRFC9180A21(t *testing.T) {
	derived, err := hpke.DHKEM(ecdh.X25519()).DeriveKeyPair(mustHex(t, "909a9b35d3dc4713a5e72a4da274b55d3d3821a37e5d099e74a647db583a904b"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := derived.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	skE, err := ecdh.X25519().NewPrivateKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	pkR, err := ecdh.X25519().NewPublicKey(mustHex(t, "4310ee97d88cc1f088a5576c77ab0cf5c3ac797f3d95139c6c84b5429c59662a"))
	if err != nil {
		t.Fatal(err)
	}
	got := sealEphemeralInfo(t, pkR, skE, mustHex(t, "4f6465206f6e2061204772656369616e2055726e"), []byte("Count-0"),
		[]byte("Beauty is truth, truth beauty"))
	want := append(mustHex(t, "1afa08d3dec047a643885163f1180476fa7ddb54c6a8029ea33f95796bf2ac4a"),
		mustHex(t, "1c5250d8034ec2b784ba2cfd69dbdb8af406cfe3ff938e131f0def8c8b60b4db21993c62ce81883d2dd1b51a28")...)
	if !bytes.Equal(got, want) {
		t.Fatalf("got  %x\nwant %x", got, want)
	}
}
