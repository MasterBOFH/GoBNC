package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestOpenSetsBusyTimeout: the store's connection waits busyTimeoutMillis for a lock.
func TestOpenSetsBusyTimeout(t *testing.T) {
	s := openTemp(t)
	var ms int
	if err := s.db.QueryRow(`PRAGMA busy_timeout`).Scan(&ms); err != nil {
		t.Fatal(err)
	}
	if ms != busyTimeoutMillis {
		t.Fatalf("busy_timeout = %d, want %d", ms, busyTimeoutMillis)
	}
}

// TestWriteWaitsForAnotherWriter: a write from a second store on the same file (the CLI beside
// the running bouncer, a brain starting while the one it replaces still writes) while the first
// holds a write lock waits for it and lands, instead of failing with "database is locked".
// (Open itself ignores most migration errors, so the write is made explicitly.)
func TestWriteWaitsForAnotherWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gobnc.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	ctx := context.Background()
	tx, err := first.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO bouncer_meta (key, value) VALUES ('first', 'yes')`); err != nil {
		t.Fatal(err)
	}
	released := make(chan error, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		released <- tx.Commit()
	}()

	if _, err := second.db.Exec(`INSERT OR REPLACE INTO bouncer_meta (key, value) VALUES ('second', 'yes')`); err != nil {
		t.Fatalf("write while another store holds the lock: %v", err)
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	var n int
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM bouncer_meta WHERE key IN ('first', 'second')`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("rows = %d, %v", n, err)
	}
}
