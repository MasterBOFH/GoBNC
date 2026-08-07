//go:build !linux && !darwin

package control

import (
	"fmt"
	"net"
)

// PeerUID is not supported on this platform.
func PeerUID(c net.Conn) (int, error) {
	return -1, fmt.Errorf("peer UID not supported on this platform")
}
