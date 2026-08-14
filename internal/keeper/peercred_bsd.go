//go:build darwin || freebsd

package keeper

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

const peerCredSupported = true

// peerUID returns the UID of the process on the other end of a unix
// socket connection via LOCAL_PEERCRED, BSD/macOS's equivalent of Linux's
// SO_PEERCRED.
//
// UNVERIFIED: this whole project's tooling in the session that wrote this
// file was Linux-only — this code has never been compiled or run, only
// written against golang.org/x/sys/unix's documented API for this
// platform. Verify on an actual darwin/freebsd host or in CI before
// relying on it in production; if it fails to build or behaves
// unexpectedly, suspect this file before the platform.
func peerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("keeper: SyscallConn: %w", err)
	}
	var cred *unix.Xucred
	var sockErr error
	if ctrlErr := raw.Control(func(fd uintptr) {
		cred, sockErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); ctrlErr != nil {
		return 0, fmt.Errorf("keeper: Control: %w", ctrlErr)
	}
	if sockErr != nil {
		return 0, fmt.Errorf("keeper: LOCAL_PEERCRED: %w", sockErr)
	}
	return cred.Uid, nil
}
