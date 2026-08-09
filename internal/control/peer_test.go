//go:build linux || darwin || freebsd

package control

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestPeerUIDSameUser(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "peer.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	errc := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer c.Close()
		uid, err := PeerUID(c)
		if err != nil {
			errc <- err
			return
		}
		if uid != os.Getuid() {
			errc <- fmt.Errorf("peer uid=%d want %d", uid, os.Getuid())
			return
		}
		errc <- nil
	}()

	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}
