//go:build darwin || freebsd

package module

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// Copied from internal/keeper/peercred_bsd.go (the keeper is not imported from here).

const peerCredSupported = true

// peerUID returns the UID of the process on the other end of a unix socket connection via
// LOCAL_PEERCRED, BSD/macOS's equivalent of Linux's SO_PEERCRED.
func peerUID(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("module: SyscallConn: %w", err)
	}
	var cred *unix.Xucred
	var sockErr error
	if ctrlErr := raw.Control(func(fd uintptr) {
		cred, sockErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); ctrlErr != nil {
		return 0, fmt.Errorf("module: Control: %w", ctrlErr)
	}
	if sockErr != nil {
		return 0, fmt.Errorf("module: LOCAL_PEERCRED: %w", sockErr)
	}
	return int(cred.Uid), nil
}
