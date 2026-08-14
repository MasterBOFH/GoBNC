//go:build unix && !linux && !darwin && !freebsd

package keeper

import "net"

const peerCredSupported = false

// peerUID is unimplemented on this platform. The keeper falls back to
// directory-permission enforcement alone (ensureSocketDir) and the
// listener logs a loud warning once at startup rather than silently
// running without this layer — see security.go's doc comment for why
// that's a degraded posture, not an unsafe one, as long as the socket
// directory is genuinely 0700 and correctly owned.
func peerUID(conn *net.UnixConn) (uint32, error) {
	return 0, errPeerCredUnsupported
}
