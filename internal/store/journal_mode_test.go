package store

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func journalMode(t *testing.T, db *sql.DB) string {
	t.Helper()
	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}

// TestOpenUsesWAL: a new store writes ahead (journal_mode wal).
func TestOpenUsesWAL(t *testing.T) {
	s := openTemp(t)
	if got := journalMode(t, s.db); got != "wal" {
		t.Fatalf("journal_mode = %q, want wal", got)
	}
}

// TestOpenConvertsRollbackStoreToWAL: a store made in the old rollback-journal mode, with its
// rows, is in WAL mode after Open and keeps them.
func TestOpenConvertsRollbackStoreToWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`PRAGMA journal_mode=DELETE`, `CREATE TABLE bouncer_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`INSERT INTO bouncer_meta (key, value) VALUES ('kept', 'yes')`} {
		if _, err := old.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if got := journalMode(t, old); got != "delete" {
		t.Fatalf("setup journal_mode = %q", got)
	}
	_ = old.Close()

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := journalMode(t, s.db); got != "wal" {
		t.Fatalf("journal_mode = %q, want wal", got)
	}
	var v string
	if err := s.db.QueryRow(`SELECT value FROM bouncer_meta WHERE key='kept'`).Scan(&v); err != nil || v != "yes" {
		t.Fatalf("kept row = %q, %v", v, err)
	}
}
