//go:build linux

package keeper

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

const peerCredSupported = true

// peerUID returns the UID of the process on the other end of a unix
// socket connection via SO_PEERCRED — the kernel-verified identity of the
// connecting process, independent of filesystem permissions (which only
// gate opening the socket, not what an already-open fd is later used for).
func peerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("keeper: SyscallConn: %w", err)
	}
	var cred *unix.Ucred
	var sockErr error
	if ctrlErr := raw.Control(func(fd uintptr) {
		cred, sockErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); ctrlErr != nil {
		return 0, fmt.Errorf("keeper: Control: %w", ctrlErr)
	}
	if sockErr != nil {
		return 0, fmt.Errorf("keeper: SO_PEERCRED: %w", sockErr)
	}
	return cred.Uid, nil
}
