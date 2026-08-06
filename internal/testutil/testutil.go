// Package testutil provides shared test harness helpers for GoBNC.
package testutil

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// FakePeer is a bidirectional IRC line transport over net.Pipe.
type FakePeer struct {
	Conn   net.Conn
	Peer   net.Conn
	r      *bufio.Reader
	mu     sync.Mutex
	closed bool
}

// NewFakePeer returns two connected ends; FakePeer wraps one side for Send/Expect.
func NewFakePeer(t testing.TB) *FakePeer {
	t.Helper()
	a, b := net.Pipe()
	fp := &FakePeer{Conn: a, Peer: b, r: bufio.NewReader(a)}
	t.Cleanup(func() { fp.Close() })
	return fp
}

// Close both ends.
func (f *FakePeer) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	_ = f.Conn.Close()
	_ = f.Peer.Close()
}

// Send writes a raw IRC line (CRLF appended if missing).
func (f *FakePeer) Send(line string) error {
	if !strings.HasSuffix(line, "\r\n") {
		line += "\r\n"
	}
	_, err := io.WriteString(f.Conn, line)
	return err
}

// Expect reads one line and requires exact match (without CRLF).
func (f *FakePeer) Expect(want string, timeout time.Duration) error {
	line, err := f.ReadLine(timeout)
	if err != nil {
		return err
	}
	if line != want {
		return fmt.Errorf("got %q want %q", line, want)
	}
	return nil
}

// ExpectContains reads one line and requires substring.
func (f *FakePeer) ExpectContains(substr string, timeout time.Duration) (string, error) {
	line, err := f.ReadLine(timeout)
	if err != nil {
		return "", err
	}
	if !strings.Contains(line, substr) {
		return line, fmt.Errorf("got %q, missing %q", line, substr)
	}
	return line, nil
}

// ReadLine reads one IRC line with deadline.
func (f *FakePeer) ReadLine(timeout time.Duration) (string, error) {
	_ = f.Conn.SetReadDeadline(time.Now().Add(timeout))
	line, err := f.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// ScriptedServer plays a request/response script against Peer.
type ScriptStep struct {
	// Expect from peer (empty = skip expect).
	Expect string
	// ExpectContains if non-empty.
	ExpectContains string
	// Send to peer (empty = skip send).
	Send string
}

// RunScript reads/writes on conn (the peer side of FakePeer.Peer or any conn).
func RunScript(ctx context.Context, conn net.Conn, steps []ScriptStep) error {
	r := bufio.NewReader(conn)
	for i, st := range steps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if st.Expect != "" || st.ExpectContains != "" {
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			line, err := r.ReadString('\n')
			if err != nil {
				return fmt.Errorf("step %d read: %w", i, err)
			}
			line = strings.TrimRight(line, "\r\n")
			if st.Expect != "" && line != st.Expect {
				return fmt.Errorf("step %d: got %q want %q", i, line, st.Expect)
			}
			if st.ExpectContains != "" && !strings.Contains(line, st.ExpectContains) {
				return fmt.Errorf("step %d: got %q missing %q", i, line, st.ExpectContains)
			}
		}
		if st.Send != "" {
			msg := st.Send
			if !strings.HasSuffix(msg, "\r\n") {
				msg += "\r\n"
			}
			if _, err := io.WriteString(conn, msg); err != nil {
				return fmt.Errorf("step %d send: %w", i, err)
			}
		}
	}
	return nil
}

// TLSFixture holds ephemeral CA, server, and client certificates.
type TLSFixture struct {
	Dir            string
	CACertPEM      []byte
	ServerCertPEM  []byte
	ServerKeyPEM   []byte
	ClientCertPEM  []byte
	ClientKeyPEM   []byte
	ClientSHA256   string // hex fingerprint of client cert DER
	ServerTLS      *tls.Config
	ClientTLS      *tls.Config
}

// NewTLSFixture generates certs under t.TempDir().
func NewTLSFixture(t testing.TB) *TLSFixture {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "GoBNC Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	serverCertPEM, serverKeyPEM := mustLeaf(t, caCert, caKey, "localhost", true)
	clientCertPEM, clientKeyPEM := mustLeaf(t, caCert, caKey, "client", false)

	clientTLSCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(clientTLSCert.Certificate[0])
	fp := hex.EncodeToString(sum[:])

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	serverTLSCert, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatal(err)
	}

	fx := &TLSFixture{
		Dir:           dir,
		CACertPEM:     caPEM,
		ServerCertPEM: serverCertPEM,
		ServerKeyPEM:  serverKeyPEM,
		ClientCertPEM: clientCertPEM,
		ClientKeyPEM:  clientKeyPEM,
		ClientSHA256:  fp,
		ServerTLS: &tls.Config{
			Certificates: []tls.Certificate{serverTLSCert},
			ClientCAs:    pool,
			ClientAuth:   tls.RequestClientCert,
			MinVersion:   tls.VersionTLS12,
		},
		ClientTLS: &tls.Config{
			Certificates:       []tls.Certificate{clientTLSCert},
			RootCAs:            pool,
			ServerName:         "localhost",
			InsecureSkipVerify: false,
			MinVersion:         tls.VersionTLS12,
		},
	}
	_ = os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0o644)
	_ = os.WriteFile(filepath.Join(dir, "server.crt"), serverCertPEM, 0o644)
	_ = os.WriteFile(filepath.Join(dir, "server.key"), serverKeyPEM, 0o600)
	_ = os.WriteFile(filepath.Join(dir, "client.crt"), clientCertPEM, 0o644)
	_ = os.WriteFile(filepath.Join(dir, "client.key"), clientKeyPEM, 0o600)
	return fx
}

func mustLeaf(t testing.TB, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, server bool) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// CertFingerprintSHA256 returns hex SHA-256 of the leaf DER.
func CertFingerprintSHA256(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
}
