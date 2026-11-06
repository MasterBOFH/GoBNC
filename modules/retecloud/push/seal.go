package push

import (
	"crypto/ecdh"
	"crypto/hpke"
	"encoding/base64"
	"errors"
)

// Info is the HPKE info string of push v1.
const Info = "rete push v1"

// ErrBadPublicKey is a public key that is not standard base64 of 32 bytes.
var ErrBadPublicKey = errors.New("public_key must be 32 bytes, base64")

// ParsePublicKey decodes a device's push key: the raw X25519 public key in standard base64 with
// padding (44 characters).
func ParsePublicKey(s string) (*ecdh.PublicKey, error) {
	raw, err := base64.StdEncoding.Strict().DecodeString(s)
	if err != nil || len(raw) != 32 {
		return nil, ErrBadPublicKey
	}
	pk, err := ecdh.X25519().NewPublicKey(raw)
	if err != nil {
		return nil, ErrBadPublicKey
	}
	return pk, nil
}

// Seal seals padded plaintext to pub with HPKE base mode (DHKEM(X25519, HKDF-SHA256),
// HKDF-SHA256, ChaCha20-Poly1305, info "rete push v1", empty aad): enc ‖ ciphertext.
func Seal(pub *ecdh.PublicKey, padded []byte) ([]byte, error) {
	pk, err := hpke.NewDHKEMPublicKey(pub)
	if err != nil {
		return nil, err
	}
	return hpke.Seal(pk, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), []byte(Info), padded)
}

// Open is Seal's inverse (tests and tools; the phone opens pushes).
func Open(priv *ecdh.PrivateKey, blob []byte) ([]byte, error) {
	k, err := hpke.NewDHKEMPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	return hpke.Open(k, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), []byte(Info), blob)
}

// ValidBlobLen reports whether a sealed blob has a length the relay accepts: 48 + 256·k, k = 1…8.
func ValidBlobLen(n int) bool {
	k := (n - 48) / padBlock
	return n > 48 && (n-48)%padBlock == 0 && k >= 1 && k <= MaxPlaintext/padBlock
}
