// Package control provides a Unix socket API for live daemon management.
package control

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// Commands understood by the daemon.
const (
	CmdPing             = "PING"
	CmdStartNetwork     = "START_NETWORK"
	CmdStopNetwork      = "STOP_NETWORK"
	CmdReloadNetwork    = "RELOAD_NETWORK"    // refresh config for next reconnect; do not drop uplink
	CmdReconnectNetwork = "RECONNECT_NETWORK" // reload config and drop uplink to force reconnect
	CmdRehash           = "REHASH"            // reload gobnc.json + refresh networks (same as SIGHUP)
	CmdReload           = "RELOAD"            // restart the brain process; keeper and uplinks stay
	CmdReloadHandoff    = "RELOAD_HANDOFF"    // sent by a freshly spawned replacement brain to confirm it's ready to take the live keeper attach
	CmdDie              = "DIE"               // stop brain and keeper (QUIT every uplink)
	CmdShutdown         = "SHUTDOWN"          // graceful brain stop (same as SIGTERM); keeper stays
	CmdStatus           = "STATUS"            // live daemon/network status (OK <json>)
)

// Client sends a single command to a running daemon and returns the reply line.
func Client(socketPath, line string) (string, error) {
	c, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	if _, err := c.Write([]byte(line)); err != nil {
		return "", err
	}
	resp, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(resp, "\r\n"), nil
}

// TryNotify dials the control socket; returns (notified, error).
// If the daemon is not running, returns (false, nil).
func TryNotify(socketPath, line string) (bool, error) {
	_, ok, err := TryQuery(socketPath, line)
	return ok, err
}

// TryQuery dials the control socket and returns an optional OK payload.
// Replies: "OK", "OK <payload>", or "ERR …". If the daemon is not running, returns ("", false, nil).
func TryQuery(socketPath, line string) (payload string, ok bool, err error) {
	resp, err := Client(socketPath, line)
	if err != nil {
		if os.IsNotExist(err) || isConnRefused(err) {
			return "", false, nil
		}
		// Dial errors when socket missing
		if _, ok := err.(*net.OpError); ok {
			return "", false, nil
		}
		return "", false, err
	}
	if strings.HasPrefix(resp, "ERR ") {
		return "", true, fmt.Errorf("%s", strings.TrimPrefix(resp, "ERR "))
	}
	if resp == "OK" {
		return "", true, nil
	}
	if strings.HasPrefix(resp, "OK ") {
		return strings.TrimPrefix(resp, "OK "), true, nil
	}
	return "", true, fmt.Errorf("unexpected reply: %s", resp)
}

// NotifyStream is TryQuery's counterpart for a command whose handler needs
// to report progress before it can know the outcome — currently only
// CmdReload, whose handler (Server.handleReloadRequest) spawns a
// replacement process and waits for it to confirm readiness before
// replying at all. The wire shape stays plain newline-delimited text (no
// framing change): every line up to the final one is a progress line
// (passed to onLine verbatim, without the "STATUS " prefix); the final
// line is "OK", "OK <payload>", or "ERR <reason>", exactly like TryQuery's
// single-line replies. deadline bounds the whole exchange, not each read,
// since the caller is waiting on a multi-step server-side operation with
// its own internal timeout — this should be set comfortably longer than
// that so the server's own timeout fires first and produces a real ERR
// line instead of the client giving up first.
func NotifyStream(socketPath, line string, deadline time.Duration, onLine func(string)) (ok bool, err error) {
	c, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		if os.IsNotExist(err) || isConnRefused(err) {
			return false, nil
		}
		if _, ok := err.(*net.OpError); ok {
			return false, nil
		}
		return false, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(deadline))
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	if _, err := c.Write([]byte(line)); err != nil {
		return false, err
	}
	r := bufio.NewReader(c)
	for {
		resp, err := r.ReadString('\n')
		if err != nil {
			return false, err
		}
		resp = strings.TrimRight(resp, "\r\n")
		if strings.HasPrefix(resp, "STATUS ") {
			if onLine != nil {
				onLine(strings.TrimPrefix(resp, "STATUS "))
			}
			continue
		}
		if strings.HasPrefix(resp, "ERR ") {
			return true, fmt.Errorf("%s", strings.TrimPrefix(resp, "ERR "))
		}
		if resp == "OK" {
			return true, nil
		}
		if strings.HasPrefix(resp, "OK ") {
			return true, nil
		}
		return true, fmt.Errorf("unexpected reply: %s", resp)
	}
}

func isConnRefused(err error) bool {
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "no such file") ||
		strings.Contains(s, "connect: ")
}
