//go:build unix

package keeper

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// This file, and peercred_*.go alongside it, implement the keeper<->brain
// socket's security posture. Read this before touching either.
//
// The threat is not sniffing — a unix socket's data path isn't exposed to
// other processes. The threat is another local process connecting and
// speaking the protocol: it would get every line from every network,
// including private messages, the ability to inject lines as the user, and
// — because the brain sends DialConfig over this socket — the ability to
// make the keeper connect anywhere and load any keypair the user can read.
// The keeper becomes a confused deputy.
//
// Two independent layers defend against that:
//  1. A 0700 directory (ensureSocketDir) that the socket lives inside.
//     net.Listen("unix", path) creates the socket file itself with
//     0777 &^ umask — there is a real window between creation and any
//     chmod — so the directory is what actually gates access, not the
//     socket's own mode.
//  2. Peer credential verification (peerUID, platform-specific) on every
//     accepted connection, and a pre-connect ownership check on the client
//     side. This is defence in depth, not the primary control — the
//     directory should already prevent an unauthorized connection — but it
//     catches a misconfigured deployment (wrong directory, loosened
//     permissions after the fact) rather than silently trusting one.
//
// What this does NOT protect against, on purpose — write these down so
// nobody assumes more than what's actually true:
//   - Another process running as the same UID. It can ptrace the keeper or
//     read its memory under most default configurations; same-UID is not a
//     security boundary. The protection here is against other users on a
//     shared host, which is the threat this design is scoped to.
//   - A first-come-first-served race for the single live attach slot. That
//     exclusivity (see Listener.liveAttached) exists to prevent two brains
//     corrupting a network's state by consuming it concurrently — it is
//     not an access control, and a hostile local process passing the UID
//     check could still win that race and either take the stream or deny
//     the real brain. UID verification is what has to stop it from getting
//     that far at all.

// socketDirMode is the required permission bits on the directory a keeper
// socket lives in. Not configurable — see ensureSocketDir's doc comment
// for why a looser mode defeats the point.
const socketDirMode = 0o700

// ensureSocketDir requires dir to already be a 0700 directory owned by the
// calling user, creating it (with that mode) if it doesn't exist yet. It
// refuses to serve rather than loosen or tighten an existing directory's
// permissions — a pre-existing directory with the wrong mode is a
// deployment error to fix, not something to silently correct out from
// under whoever set it up.
//
// Never point this at /tmp or any other world-writable location: such a
// directory's own permissions don't protect the socket regardless of what
// mode the socket file gets, and unlinking a stale socket there on startup
// races a symlink an attacker placed at that path. $XDG_RUNTIME_DIR
// (/run/user/UID, already 0700, already per-user, already cleaned up by
// the OS on logout) is the natural home; DefaultRuntimeDir returns it.
func ensureSocketDir(dir string) error {
	info, err := os.Stat(dir)
	switch {
	case os.IsNotExist(err):
		if err := os.MkdirAll(dir, socketDirMode); err != nil {
			return fmt.Errorf("keeper: create socket dir %s: %w", dir, err)
		}
		info, err = os.Stat(dir)
		if err != nil {
			return fmt.Errorf("keeper: stat socket dir %s after create: %w", dir, err)
		}
	case err != nil:
		return fmt.Errorf("keeper: stat socket dir %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("keeper: socket dir %s exists and is not a directory", dir)
	}
	if mode := info.Mode().Perm(); mode != socketDirMode {
		return fmt.Errorf("keeper: socket dir %s has mode %04o, want %04o — refusing to serve; fix its permissions or remove it so it can be recreated correctly", dir, mode, socketDirMode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("keeper: cannot determine owner of socket dir %s on this platform", dir)
	}
	if want := uint32(os.Getuid()); stat.Uid != want {
		return fmt.Errorf("keeper: socket dir %s is owned by uid %d, not this process's uid %d — refusing to serve", dir, stat.Uid, want)
	}
	return nil
}

// DefaultRuntimeDir returns $XDG_RUNTIME_DIR if set, the natural home for
// a per-user unix socket (already 0700, already cleaned up by the OS). It
// does not fall back to /tmp — see ensureSocketDir's doc comment for why —
// so a deployment without XDG_RUNTIME_DIR set must choose its own socket
// directory explicitly rather than silently landing somewhere unsafe.
func DefaultRuntimeDir() (string, bool) {
	dir := os.Getenv("XDG_RUNTIME_DIR")
	if dir == "" {
		return "", false
	}
	return dir, true
}

// verifySocketOwner is the client-side half of peer verification: before
// dialing, confirm the socket file itself is owned by this process's own
// UID. Uses Lstat, not Stat, so a symlink planted at the expected path
// doesn't get silently followed into checking the wrong file's ownership —
// this shouldn't be reachable if the containing directory is genuinely
// 0700 and correctly owned (ensureSocketDir's job, keeper-side), but this
// check is the brain's own defence in depth, not dependent on trusting
// that the keeper did its job right.
func verifySocketOwner(sockPath string) error {
	info, err := os.Lstat(sockPath)
	if err != nil {
		return fmt.Errorf("keeper attach: stat socket %s: %w", sockPath, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("keeper attach: cannot determine owner of socket %s on this platform", sockPath)
	}
	if want := uint32(os.Getuid()); !uidAuthorized(stat.Uid, want) {
		return fmt.Errorf("keeper attach: socket %s is owned by uid %d, not this process's uid %d — refusing to connect", sockPath, stat.Uid, want)
	}
	return nil
}

// uidAuthorized is the one-line comparison every peer-credential check
// (server-side accept, client-side pre-connect) reduces to. Split out so
// the comparison itself is unit-testable without needing a second real
// user account, which this environment doesn't have.
func uidAuthorized(peer, want uint32) bool {
	return peer == want
}

var errPeerCredUnsupported = errors.New("keeper: peer credential verification is not implemented on this platform")
