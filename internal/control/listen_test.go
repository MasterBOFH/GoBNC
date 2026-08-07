package control

import (
	"os"
	"path/filepath"
	"testing"
)

func TestListenUnixOwnerOnly(t *testing.T) {
	// Keep path short: Darwin sun_path is ~104 bytes.
	dir := filepath.Join(t.TempDir(), "d")
	sock := filepath.Join(dir, "s.sock")
	ln, err := ListenUnix(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = ln.Close()
		_ = os.Remove(sock)
	}()

	st, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("socket mode=%04o want owner-only", perm)
	}
	dst, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := dst.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("dir mode=%04o want owner-only", perm)
	}
}
