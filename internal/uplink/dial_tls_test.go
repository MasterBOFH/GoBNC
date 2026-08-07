package uplink

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestDialTLSNoVerifyMismatchedHostname dials a TLS listener whose certificate is
// self-signed for a different DNS name than the dial target.
func TestDialTLSNoVerifyMismatchedHostname(t *testing.T) {
	cert, err := selfSignedServerCert("other.host.example")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	acceptOnce := func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		_, _ = c.Read(buf)
	}

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fails: untrusted CA + hostname "127.0.0.1" ≠ cert DNS "other.host.example".
	go acceptOnce()
	uStrict := New(Config{
		Network: store.Network{
			Name: "t", Host: "127.0.0.1", Port: port, Nick: "n", TLS: true, TLSNoVerify: false,
		},
	}, &regHandler{})
	if _, err := uStrict.dial(ctx); err == nil {
		t.Fatal("expected dial to fail without tls_noverify")
	}

	// Succeeds when verification is skipped.
	go acceptOnce()
	uSkip := New(Config{
		Network: store.Network{
			Name: "t", Host: "127.0.0.1", Port: port, Nick: "n", TLS: true, TLSNoVerify: true,
		},
	}, &regHandler{})
	c, err := uSkip.dial(ctx)
	if err != nil {
		t.Fatalf("tls_noverify dial: %v", err)
	}
	_ = c.Close()
}

func selfSignedServerCert(dnsName string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{dnsName},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}
