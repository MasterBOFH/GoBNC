//go:build unix && !linux && !darwin && !freebsd

package module

import (
	"errors"
	"net"
)

// Copied from internal/keeper/peercred_unsupported.go (the keeper is not imported from here).

const peerCredSupported = false

// peerUID is unimplemented on this platform; the server relies on the socket's 0600 mode and
// warns once at startup.
func peerUID(c *net.UnixConn) (int, error) {
	return 0, errors.New("module: peer credential verification is not implemented on this platform")
}
