package control

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// ListenUnix creates a Unix domain socket at path with owner-only permissions.
// The parent directory is created with mode 0700 when missing (and chmod'd to 0700).
// The socket itself is chmod'd to 0600 after bind.
func ListenUnix(path string) (net.Listener, error) {
	if path == "" {
		return nil, fmt.Errorf("empty control socket path")
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("control socket dir: %w", err)
		}
		_ = os.Chmod(dir, 0o700)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("chmod control socket: %w", err)
	}
	return ln, nil
}
