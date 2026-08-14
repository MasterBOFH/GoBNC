package keeper

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testCA is a minimal self-signed CA for generating server leaf certs signed
// by it, so DialConfig.CAFile can be exercised against a real chain rather
// than only the leaf-is-its-own-root case selfSignedCert covers.
type testCA struct {
	cert     *x509.Certificate
	key      *rsa.PrivateKey
	certPath string
}

func makeTestCA(t *testing.T, dir, name string) testCA {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	certPath := filepath.Join(dir, name+"-ca.crt")
	f, err := os.Create(certPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	_ = f.Close()
	return testCA{cert: cert, key: priv, certPath: certPath}
}

// makeLeaf issues a server cert for 127.0.0.1 signed by ca.
func makeLeaf(t *testing.T, dir string, ca testCA) (certPath, keyPath string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &priv.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	certPath = filepath.Join(dir, "leaf.crt")
	keyPath = filepath.Join(dir, "leaf.key")
	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	_ = certOut.Close()
	keyOut, err := os.Create(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	_ = pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	_ = keyOut.Close()
	return certPath, keyPath
}

// tlsServerWithHandshake starts a TLS listener with the given leaf cert and
// hands the first accepted, handshaken connection to fn in a goroutine.
func tlsServerWithHandshake(t *testing.T, certPath, keyPath string) (addr string, closeFn func()) {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		if tc, ok := c.(*tls.Conn); ok {
			_ = tc.Handshake()
		}
		_ = c.Close()
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

func TestDialTLSCustomCAVerifies(t *testing.T) {
	dir := t.TempDir()
	ca := makeTestCA(t, dir, "good")
	certPath, keyPath := makeLeaf(t, dir, ca)

	addr, closeLn := tlsServerWithHandshake(t, certPath, keyPath)
	defer closeLn()
	host, port := hostPort(addr)

	k := newTestKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := k.Dial(ctx, DialConfig{Host: host, Port: port, TLS: true, CAFile: ca.certPath})
	if err != nil {
		t.Fatalf("Dial with matching CA: %v", err)
	}
	_ = k.Close()
}

func TestDialTLSCustomCAMismatch(t *testing.T) {
	dir := t.TempDir()
	signingCA := makeTestCA(t, dir, "signer")
	otherCA := makeTestCA(t, dir, "other")
	certPath, keyPath := makeLeaf(t, dir, signingCA)

	addr, closeLn := tlsServerWithHandshake(t, certPath, keyPath)
	defer closeLn()
	host, port := hostPort(addr)

	k := newTestKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := k.Dial(ctx, DialConfig{Host: host, Port: port, TLS: true, CAFile: otherCA.certPath})
	if err == nil {
		_ = k.Close()
		t.Fatalf("Dial verified a leaf against an unrelated CA, want error")
	}
	if !containsAny(err.Error(), "unknown authority", "certificate signed by unknown authority") {
		t.Fatalf("got %v, want an x509 verification error", err)
	}
}

func TestDialTLSCAFileMalformedPEM(t *testing.T) {
	dir := t.TempDir()
	ca := makeTestCA(t, dir, "good")
	certPath, keyPath := makeLeaf(t, dir, ca)
	addr, closeLn := tlsServerWithHandshake(t, certPath, keyPath)
	defer closeLn()
	host, port := hostPort(addr)

	badCA := filepath.Join(dir, "garbage.crt")
	if err := os.WriteFile(badCA, []byte("this is not a PEM file"), 0o600); err != nil {
		t.Fatal(err)
	}

	k := newTestKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := k.Dial(ctx, DialConfig{Host: host, Port: port, TLS: true, CAFile: badCA})
	if err == nil {
		_ = k.Close()
		t.Fatalf("Dial with malformed CA PEM succeeded, want error")
	}
	if !containsAny(err.Error(), "no certificates found") {
		t.Fatalf("got %v, want a no-certificates-found error naming the CA bundle", err)
	}
}

func TestDialTLSCAFileMissing(t *testing.T) {
	dir := t.TempDir()
	ca := makeTestCA(t, dir, "good")
	certPath, keyPath := makeLeaf(t, dir, ca)
	addr, closeLn := tlsServerWithHandshake(t, certPath, keyPath)
	defer closeLn()
	host, port := hostPort(addr)

	k := newTestKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := k.Dial(ctx, DialConfig{Host: host, Port: port, TLS: true, CAFile: filepath.Join(dir, "does-not-exist.crt")})
	if err == nil {
		_ = k.Close()
		t.Fatalf("Dial with missing CA file succeeded, want error")
	}
	if !containsAny(err.Error(), "no such file", "cannot find the file") {
		t.Fatalf("got %v, want a file-not-found error", err)
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
