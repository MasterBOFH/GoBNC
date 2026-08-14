//go:build linux

package keeper

import (
	"net"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// TestPeerUIDReturnsRealUID is the positive-path proof that SO_PEERCRED
// actually works end-to-end: a real unix socket pair, both ends in this
// same process, and peerUID must report this process's real UID — proving
// the syscall plumbing (SyscallConn, Control, GetsockoptUcred) is wired up
// correctly. Genuinely testing rejection of a *different* UID would need a
// second real user account, which this environment doesn't have; that gap
// is closed instead by TestUidAuthorized isolating the comparison logic.
func TestPeerUIDReturnsRealUID(t *testing.T) {
	srvConn, cliConn := socketpair(t)
	defer srvConn.Close()
	defer cliConn.Close()

	uid, err := peerUID(srvConn)
	if err != nil {
		t.Fatalf("peerUID: %v", err)
	}
	if want := uint32(os.Getuid()); uid != want {
		t.Fatalf("peerUID=%d, want %d (os.Getuid())", uid, want)
	}
}

func socketpair(t *testing.T) (srv, cli *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	f1 := os.NewFile(uintptr(fds[0]), "sp0")
	f2 := os.NewFile(uintptr(fds[1]), "sp1")
	c1, err := net.FileConn(f1)
	if err != nil {
		t.Fatalf("FileConn: %v", err)
	}
	_ = f1.Close()
	c2, err := net.FileConn(f2)
	if err != nil {
		t.Fatalf("FileConn: %v", err)
	}
	_ = f2.Close()
	return c1.(*net.UnixConn), c2.(*net.UnixConn)
}
