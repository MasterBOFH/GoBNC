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
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestDialBindHost(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portStr)

	got := make(chan net.Addr, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			got <- nil
			return
		}
		defer c.Close()
		got <- c.RemoteAddr()
	}()

	u := New(Config{
		Network: store.Network{
			Host: "127.0.0.1", Port: port, Nick: "me", BindHost: "127.0.0.1",
		},
	}, nil)
	c, err := u.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	select {
	case ra := <-got:
		tcp, ok := ra.(*net.TCPAddr)
		if !ok || !tcp.IP.Equal(net.ParseIP("127.0.0.1")) {
			t.Fatalf("remote addr=%v", ra)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("accept timeout")
	}
}

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

func TestDialPresentsUplinkClientCert(t *testing.T) {
	serverCert, err := selfSignedServerCert("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	clientCert, clientKeyPEM, err := selfSignedClientCertPEM()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath := filepath.Join(dir, "client.crt")
	keyPath := filepath.Join(dir, "client.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, clientKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	gotPeer := make(chan bool, 1)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAnyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			gotPeer <- false
			return
		}
		defer c.Close()
		tc := c.(*tls.Conn)
		if err := tc.Handshake(); err != nil {
			gotPeer <- false
			return
		}
		st := tc.ConnectionState()
		gotPeer <- len(st.PeerCertificates) > 0
	}()

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portStr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	u := New(Config{
		Network: store.Network{
			Name: "t", Host: "127.0.0.1", Port: port, Nick: "n", TLS: true, TLSNoVerify: true,
			TLSCert: certPath, TLSKey: keyPath,
		},
	}, &regHandler{})
	c, err := u.dial(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = c.Close()

	select {
	case ok := <-gotPeer:
		if !ok {
			t.Fatal("server did not see client certificate")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for peer cert check")
	}
}

func selfSignedClientCertPEM() (tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "gobnc-uplink"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	return pair, keyPEM, err
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
