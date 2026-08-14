//go:build unix

package keeper

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnsureSocketDirCreatesWith0700(t *testing.T) {
	base := t.TempDir() // t.TempDir() itself is not necessarily 0700
	dir := filepath.Join(base, "runtime", "gobnc")
	if err := ensureSocketDir(dir); err != nil {
		t.Fatalf("ensureSocketDir: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o700 {
		t.Fatalf("created dir mode=%04o, want 0700", mode)
	}
}

func TestEnsureSocketDirRejectsWrongPermissions(t *testing.T) {
	dir := t.TempDir() // typically 0700 already on most systems' TempDir, force it wrong
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	err := ensureSocketDir(dir)
	if err == nil {
		t.Fatalf("ensureSocketDir accepted a 0755 directory, want rejection")
	}
	t.Logf("rejected as expected: %v", err)
}

func TestEnsureSocketDirRejectsNonDirectory(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "not-a-dir")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := ensureSocketDir(path); err == nil {
		t.Fatalf("ensureSocketDir accepted a plain file, want rejection")
	}
}

// TestServeRefusesBadSocketDirPermissions is the end-to-end version: Serve
// itself must refuse rather than bind inside a directory whose permissions
// don't meet ensureSocketDir's requirement, and it must fail fast (not
// silently hang or bind anyway).
//
// Serve is run in a goroutine with a bounded wait rather than called
// synchronously and expected to return quickly on its own: if this check
// ever regressed to a no-op, Serve would proceed to Accept() and block
// forever with nothing to connect, and a synchronous call would hang this
// test (and, worse, the whole test binary until the outer `go test`
// timeout) instead of failing cleanly with a clear message. Confirmed by
// deliberately disabling the check during development — the naive version
// of this test hung for the full 120s command timeout instead of failing;
// this version turns that same regression into an immediate, readable
// failure.
func TestServeRefusesBadSocketDirPermissions(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	mgr := NewManager(8192, 4096, nil)
	l := NewListener(mgr, nil)

	errCh := make(chan error, 1)
	go func() { errCh <- l.Serve(t.Context(), filepath.Join(dir, "keeper.sock")) }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("Serve succeeded inside a 0755 directory, want refusal")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Serve neither refused nor returned within 2s — it likely bound anyway and is blocked in Accept (see doc comment)")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "keeper.sock")); statErr == nil {
		t.Fatalf("Serve created a socket file despite refusing the directory")
	}
}

func TestVerifySocketOwnerAcceptsOwnSocket(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, "keeper.sock")
	// verifySocketOwner only stats the path; it doesn't need a real
	// listener bound there, just a filesystem entry this process owns.
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := verifySocketOwner(path); err != nil {
		t.Fatalf("verifySocketOwner rejected our own file: %v", err)
	}
}

func TestVerifySocketOwnerRejectsMissingSocket(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	err := verifySocketOwner(filepath.Join(dir, "does-not-exist.sock"))
	if err == nil {
		t.Fatalf("verifySocketOwner accepted a nonexistent path")
	}
}

// TestUidAuthorized is the rejection-logic unit test the UID-mismatch case
// otherwise can't get without a second real user account, which isn't
// available in this environment. It isolates the comparison
// authorizePeer/Attach's checks are built on rather than the OS-level fact
// of a different process's real credentials.
func TestUidAuthorized(t *testing.T) {
	if !uidAuthorized(1000, 1000) {
		t.Fatalf("matching uids rejected")
	}
	if uidAuthorized(1000, 1001) {
		t.Fatalf("mismatched uids accepted")
	}
}

// TestUIDMismatchRejectedEndToEnd exercises the whole peer-verification
// path — accept, SO_PEERCRED retrieval, comparison, rejection — for real,
// rather than the accept-loop logic and the comparison logic separately.
// There's no second real user account in this environment to connect from,
// so WithExpectedUID stands in: the connecting process's UID is completely
// real and retrieved via the genuine syscall path, only the "what counts as
// authorized" expectation is deliberately wrong. That's exactly the
// injection point a deployment running the brain as a different UID from
// the keeper would need anyway.
func TestUIDMismatchRejectedEndToEnd(t *testing.T) {
	if !peerCredSupported {
		t.Skip("peer credential verification not supported on this platform")
	}
	mgr := NewManager(8192, 4096, nil)
	wrongUID := uint32(os.Getuid()) + 1
	sockPath := startTestListener(t, mgr, WithExpectedUID(wrongUID))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeValidate})
	if err == nil {
		t.Fatalf("Attach succeeded despite a deliberately wrong expected UID, want rejection")
	}
	t.Logf("rejected as expected: %v", err)
}
