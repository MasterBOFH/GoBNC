package testutil

import (
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TempStore opens a migrated SQLite DB in t.TempDir().
func TempStore(t testing.TB) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "gobnc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
