//go:build linux

package module

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// Copied from internal/keeper/peercred_linux.go (the keeper is not imported from here).

const peerCredSupported = true

// peerUID returns the UID of the process on the other end of a unix socket connection via
// SO_PEERCRED — the kernel-verified identity of the connecting process.
func peerUID(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("module: SyscallConn: %w", err)
	}
	var cred *unix.Ucred
	var sockErr error
	if ctrlErr := raw.Control(func(fd uintptr) {
		cred, sockErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); ctrlErr != nil {
		return 0, fmt.Errorf("module: Control: %w", ctrlErr)
	}
	if sockErr != nil {
		return 0, fmt.Errorf("module: SO_PEERCRED: %w", sockErr)
	}
	return int(cred.Uid), nil
}
