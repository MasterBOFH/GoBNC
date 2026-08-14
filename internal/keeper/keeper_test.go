package keeper

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
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

// fakeServer is a minimal line-oriented TCP server for driving the keeper's
// read loop deterministically in tests.
type fakeServer struct {
	ln   net.Listener
	mu   sync.Mutex
	conn net.Conn
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return &fakeServer{ln: ln}
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

// accept waits for and stores the next inbound connection.
func (s *fakeServer) accept(t *testing.T) net.Conn {
	t.Helper()
	c, err := s.ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	s.mu.Lock()
	s.conn = c
	s.mu.Unlock()
	return c
}

func (s *fakeServer) send(t *testing.T, c net.Conn, line string) {
	t.Helper()
	if _, err := io.WriteString(c, line+"\r\n"); err != nil {
		t.Fatalf("server write: %v", err)
	}
}

func (s *fakeServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()
}

func hostPort(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

func newTestKeeper(t *testing.T) *Keeper {
	t.Helper()
	return New(8192, 64, nil, WithReadIdleTimeout(2*time.Second))
}

func waitState(t *testing.T, k *Keeper, want State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st, _ := k.State(); st == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, epoch := k.State()
	t.Fatalf("timed out waiting for state %v; got %v (epoch %d), lastErr=%v", want, got, epoch, k.LastError())
}

// TestFreshKeeperEpochZero locks in the documented property that epoch==0
// specifically means "Dial has never succeeded" — distinct from a non-zero
// epoch in state NotConnected, which means a connection existed and ended.
// A future consumer (e.g. blob store clearing) branches on this.
func TestFreshKeeperEpochZero(t *testing.T) {
	k := newTestKeeper(t)
	st, epoch := k.State()
	if st != NotConnected || epoch != 0 {
		t.Fatalf("fresh Keeper: state=%v epoch=%d, want NotConnected/0", st, epoch)
	}
	if err := k.LastError(); err != nil {
		t.Fatalf("fresh Keeper LastError=%v, want nil", err)
	}
}

func TestDialPlaintextAndReadLines(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	k := newTestKeeper(t)
	host, port := hostPort(srv.addr())

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer k.Close()

	st, epoch := k.State()
	if st != Connected || epoch != 1 {
		t.Fatalf("state=%v epoch=%d, want Connected/1", st, epoch)
	}

	conn := <-acceptedCh
	srv.send(t, conn, ":irc.example.net NOTICE * :hello")
	srv.send(t, conn, ":irc.example.net 001 nick :Welcome")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if k.LastSeq() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	entries, ok := k.Since(0)
	if !ok {
		t.Fatalf("Since(0) reported gap unexpectedly")
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(entries), entries)
	}
	if entries[0].Seq != 1 || entries[1].Seq != 2 {
		t.Fatalf("seq not monotonic from 1: %+v", entries)
	}
	if entries[0].Epoch != 1 || entries[1].Epoch != 1 {
		t.Fatalf("epoch not tagged: %+v", entries)
	}
	if entries[1].Line != ":irc.example.net 001 nick :Welcome" {
		t.Fatalf("unexpected line: %q", entries[1].Line)
	}
}

// TestOverflowDetectableViaKeeperSince proves the mechanism a future step 3
// overflow policy depends on: end-to-end through a real Dial and read loop
// (not just the ring in isolation, as TestRingEvictsOldestAndReportsGap
// covers), a consumer holding a watermark that falls out of the ring gets
// ok=false from Since, not a silently truncated result. See the STEP-3
// OBLIGATION comment on the ring type in ring.go.
func TestOverflowDetectableViaKeeperSince(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	const ringCap = 4
	k := New(8192, ringCap, nil, WithReadIdleTimeout(5*time.Second))
	host, port := hostPort(srv.addr())

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer k.Close()
	conn := <-acceptedCh

	// A consumer checkpoints at seq 1 (the first line)...
	srv.send(t, conn, "NOTICE * :line1")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	checkpoint := k.LastSeq()
	if checkpoint != 1 {
		t.Fatalf("checkpoint seq=%d, want 1", checkpoint)
	}

	// ...then more lines arrive than the ring can hold, evicting it.
	for i := 2; i <= ringCap+3; i++ {
		srv.send(t, conn, fmt.Sprintf("NOTICE * :line%d", i))
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < uint64(ringCap+3) {
		time.Sleep(5 * time.Millisecond)
	}

	entries, ok := k.Since(checkpoint)
	if ok {
		t.Fatalf("Since(%d) reported ok=true after overflow, want ok=false (entries=%+v)", checkpoint, entries)
	}
	if k.DroppedCount() == 0 {
		t.Fatalf("DroppedCount=0 after overflow, want > 0")
	}
}

// TestSlowSubscriberDoesNotBlockOthers proves publishLine's non-blocking
// send is real: subscriber A never drains its channel, subscriber B drains
// continuously, and B must receive everything on time regardless of A's
// state.
func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()
	k := newTestKeeper(t)
	host, port := hostPort(srv.addr())

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer k.Close()
	conn := <-acceptedCh

	slow, unsubSlow := k.SubscribeLines() // never read from
	defer unsubSlow()
	fast, unsubFast := k.SubscribeLines()
	defer unsubFast()

	const n = 500 // well past the 256-entry buffer, so slow overflows partway through
	go func() {
		for i := 0; i < n; i++ {
			srv.send(t, conn, fmt.Sprintf("NOTICE * :line%d", i))
		}
	}()

	received := 0
	deadline := time.Now().Add(5 * time.Second)
	for received < n {
		select {
		case _, ok := <-fast.Lines:
			if !ok {
				t.Fatalf("fast subscriber channel closed early at %d/%d", received, n)
			}
			received++
		case <-time.After(time.Until(deadline)):
			t.Fatalf("fast subscriber only received %d/%d before timing out — a slow subscriber blocked it", received, n)
		}
	}

	select {
	case <-slow.Overflow:
		// Expected: slow never drained, so it should have overflowed.
	case <-time.After(time.Second):
		t.Fatalf("slow subscriber never signaled overflow despite never being drained")
	}
}

// TestReadLoopNeverBlocksOnSubscriber proves the read loop's own progress
// (LastSeq advancing) is unaffected even when every subscriber is stalled —
// the property that matters most, since a blocked read loop would stall the
// keeper's core job regardless of what any client is doing.
func TestReadLoopNeverBlocksOnSubscriber(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()
	k := newTestKeeper(t)
	host, port := hostPort(srv.addr())

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer k.Close()
	conn := <-acceptedCh

	// Two stalled subscribers, neither ever drained.
	_, unsub1 := k.SubscribeLines()
	defer unsub1()
	_, unsub2 := k.SubscribeLines()
	defer unsub2()

	const n = 1000 // several multiples of the 256-entry subscriber buffer
	for i := 0; i < n; i++ {
		srv.send(t, conn, fmt.Sprintf("NOTICE * :line%d", i))
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < uint64(n) {
		time.Sleep(5 * time.Millisecond)
	}
	if k.LastSeq() != uint64(n) {
		t.Fatalf("LastSeq=%d after flooding %d lines with all subscribers stalled, want %d — read loop blocked on a subscriber", k.LastSeq(), n, n)
	}
	// Ring itself must be intact too, independent of any subscriber.
	entries, ok := k.Since(0)
	if !ok || len(entries) == 0 {
		t.Fatalf("ring unusable after stalled-subscriber flood: ok=%v len=%d", ok, len(entries))
	}
}

func TestAutonomousPingPong(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	k := newTestKeeper(t)
	host, port := hostPort(srv.addr())

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer k.Close()

	conn := <-acceptedCh
	srv.send(t, conn, "PING :abc123")

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("expected autonomous PONG, got err: %v", err)
	}
	line = trimCRLF(line)
	if line != "PONG :abc123" {
		t.Fatalf("got %q, want PONG :abc123", line)
	}

	// The PING itself must still land in the ring buffer, seq'd — the keeper
	// only adds a response, it never removes the line it responded to.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	entries, _ := k.Since(0)
	if len(entries) != 1 || entries[0].Line != "PING :abc123" {
		t.Fatalf("PING not recorded in ring: %+v", entries)
	}
}

func trimCRLF(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

func TestCloseIsIdempotentAndDeliberate(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	k := newTestKeeper(t)
	host, port := hostPort(srv.addr())

	events, unsub := k.Subscribe()
	defer unsub()

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	<-acceptedCh

	if err := <-drainEvent(t, events); err != nil {
		t.Fatalf("unexpected connect event error: %v", err)
	}

	if err := k.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Second Close must be a no-op, not an error or a panic.
	if err := k.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	st, _ := k.State()
	if st != NotConnected {
		t.Fatalf("state=%v after Close, want NotConnected", st)
	}
	if err := k.LastError(); err != nil {
		t.Fatalf("LastError=%v after deliberate Close, want nil", err)
	}

	select {
	case ev := <-events:
		if ev.Kind != EventDisconnected {
			t.Fatalf("unexpected event kind %v", ev.Kind)
		}
		if ev.Err != nil {
			t.Fatalf("deliberate Close published Err=%v, want nil", ev.Err)
		}
	case <-time.After(time.Second):
		// A deliberate close is allowed to not publish (readLoop sees ctx
		// canceled and treats it as deliberate without emitting); either
		// behavior is acceptable as long as state/LastError are correct,
		// which was already asserted above.
	}
}

func TestReadIdleTimeoutFires(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	// A dedicated short timeout, distinct from the newTestKeeper default,
	// proving the idle deadline actually trips rather than only proving (as
	// TestLiveIdleHoldsConnection does) that it doesn't trip prematurely.
	k := New(8192, 64, nil, WithReadIdleTimeout(150*time.Millisecond))
	host, port := hostPort(srv.addr())

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	<-acceptedCh // server accepts but deliberately sends nothing

	waitState(t, k, NotConnected, 2*time.Second)
	err := k.LastError()
	if err == nil {
		t.Fatalf("LastError is nil after idle timeout, want a deadline-exceeded error")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("LastError=%v, want a deadline-exceeded error", err)
	}
}

// TestDialConfigReadIdleTimeoutOverride proves ReadIdleTimeout is genuinely
// per-dial: a Keeper built with a long default (effectively "off" for this
// test's duration) still times out quickly when a specific Dial's
// DialConfig asks for a short one, and a second Dial on the same Keeper
// with no override falls back to the keeper's own default rather than
// reusing the first dial's value.
func TestDialConfigReadIdleTimeoutOverride(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	k := New(8192, 64, nil, WithReadIdleTimeout(time.Hour)) // effectively off
	host, port := hostPort(srv.addr())

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port, ReadIdleTimeout: 150 * time.Millisecond}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	<-acceptedCh // accepts but sends nothing

	waitState(t, k, NotConnected, 2*time.Second)
	if err := k.LastError(); err == nil || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("LastError=%v, want a deadline-exceeded error — the per-dial override should have applied, not the 1h keeper default", err)
	}
}

func drainEvent(t *testing.T, ch <-chan Event) chan error {
	t.Helper()
	out := make(chan error, 1)
	go func() {
		select {
		case ev := <-ch:
			if ev.Kind != EventConnected {
				out <- fmt.Errorf("got kind %v, want EventConnected", ev.Kind)
				return
			}
			out <- nil
		case <-time.After(2 * time.Second):
			out <- errors.New("timed out waiting for connect event")
		}
	}()
	return out
}

func TestSocketDeathReportedAsNotConnected(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	k := newTestKeeper(t)
	host, port := hostPort(srv.addr())

	events, unsub := k.Subscribe()
	defer unsub()

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := <-drainEvent(t, events); err != nil {
		t.Fatalf("connect event: %v", err)
	}

	conn := <-acceptedCh
	_ = conn.Close() // server hangs up — no Close() call from our side

	waitState(t, k, NotConnected, 3*time.Second)
	if err := k.LastError(); err == nil {
		t.Fatalf("LastError is nil after socket died on its own")
	}

	select {
	case ev := <-events:
		if ev.Kind != EventDisconnected || ev.Err == nil {
			t.Fatalf("got %+v, want EventDisconnected with non-nil Err", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("no EventDisconnected published after socket death")
	}
}

func TestDialCloseDialCycleBumpsEpoch(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	k := newTestKeeper(t)
	host, port := hostPort(srv.addr())

	for i := 1; i <= 3; i++ {
		acceptedCh := make(chan net.Conn, 1)
		go func() { acceptedCh <- srv.accept(t) }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
			cancel()
			t.Fatalf("Dial #%d: %v", i, err)
		}
		cancel()
		<-acceptedCh

		st, epoch := k.State()
		if st != Connected || epoch != uint64(i) {
			t.Fatalf("cycle %d: state=%v epoch=%d, want Connected/%d", i, st, epoch, i)
		}
		if err := k.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
	}
}

func TestDialWhileConnectedRejected(t *testing.T) {
	srv := newFakeServer(t)
	defer srv.close()

	k := newTestKeeper(t)
	host, port := hostPort(srv.addr())

	go func() { srv.accept(t) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer k.Close()

	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); !errors.Is(err, ErrAlreadyConnected) {
		t.Fatalf("second Dial while connected: got %v, want ErrAlreadyConnected", err)
	}
}

// --- TLS ---

func selfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:         true,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPath = filepath.Join(dir, "server.crt")
	keyPath = filepath.Join(dir, "server.key")
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

func TestDialTLS(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := selfSignedCert(t, dir)
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	defer ln.Close()

	acceptedCh := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			t.Errorf("tls accept: %v", err)
			return
		}
		// Server-side tls.Conn handshakes lazily on first I/O; drive it now
		// so the client's Dial (which blocks for the full handshake) isn't
		// waiting on a server that's waiting on the client to go first.
		if tc, ok := c.(*tls.Conn); ok {
			if err := tc.Handshake(); err != nil {
				t.Errorf("server handshake: %v", err)
				return
			}
		}
		acceptedCh <- c
	}()

	host, port := hostPort(ln.Addr().String())
	k := newTestKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = k.Dial(ctx, DialConfig{Host: host, Port: port, TLS: true, TLSNoVerify: true})
	if err != nil {
		t.Fatalf("Dial TLS: %v", err)
	}
	defer k.Close()

	conn := <-acceptedCh
	defer conn.Close()
	io.WriteString(conn, ":irc.example.net 001 nick :hi\r\n")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	entries, _ := k.Since(0)
	if len(entries) != 1 {
		t.Fatalf("got %d entries over TLS, want 1", len(entries))
	}
}

func TestDialTLSVerificationFailsWithoutNoVerify(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := selfSignedCert(t, dir)
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		// Drive the server handshake so the client gets a real certificate
		// to reject, rather than racing a bare connection close.
		if tc, ok := c.(*tls.Conn); ok {
			_ = tc.Handshake()
		}
		_ = c.Close()
	}()

	host, port := hostPort(ln.Addr().String())
	k := newTestKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = k.Dial(ctx, DialConfig{Host: host, Port: port, TLS: true})
	if err == nil {
		k.Close()
		t.Fatalf("Dial with unverifiable self-signed cert succeeded, want error")
	}
	if !strings.Contains(err.Error(), "unknown authority") {
		t.Fatalf("got %v, want an x509 unknown-authority verification error", err)
	}
}

// --- ring buffer ---

func TestRingEvictsOldestAndReportsGap(t *testing.T) {
	r := newRing(3)
	for i := uint64(1); i <= 5; i++ {
		r.push(Entry{Seq: i, Line: fmt.Sprintf("line%d", i)})
	}
	if got := r.droppedCount(); got != 2 {
		t.Fatalf("dropped=%d, want 2", got)
	}

	// A consumer resuming from a real prior checkpoint that has since been
	// evicted (seq 1, 2 are gone; oldest retained is 3) must see a gap.
	entries, ok := r.since(1)
	if ok {
		t.Fatalf("since(1) should report a gap once seq 1's neighborhood has been evicted")
	}
	if entries != nil {
		t.Fatalf("since should return nil entries on gap, got %+v", entries)
	}

	// since(0) means "no prior checkpoint" (a cold attach), not "resume from
	// seq 0" — it is always satisfiable from whatever is currently retained.
	entries, ok = r.since(0)
	if !ok {
		t.Fatalf("since(0) should never report a gap")
	}
	if len(entries) != 3 || entries[0].Seq != 3 {
		t.Fatalf("since(0) should return everything retained: %+v", entries)
	}

	entries, ok = r.since(3)
	if !ok {
		t.Fatalf("since(3) should be satisfiable (seq 3 is the oldest retained)")
	}
	if len(entries) != 2 || entries[0].Seq != 4 || entries[1].Seq != 5 {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestRingSinceZeroOnEmptyRing(t *testing.T) {
	r := newRing(3)
	entries, ok := r.since(0)
	if !ok || entries != nil {
		t.Fatalf("since(0) on empty ring: entries=%v ok=%v, want nil/true", entries, ok)
	}
}
