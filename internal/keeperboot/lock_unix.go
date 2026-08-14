//go:build unix

package keeperboot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// fileLock is an exclusive, cross-process lock backed by flock(2) on a
// dedicated file — not the pidfile or socket themselves, so acquiring it
// never races their own creation/removal.
type fileLock struct {
	f *os.File
}

// acquireLock takes an exclusive lock on path, polling with LOCK_NB
// (rather than a single blocking LOCK_EX) so the wait can be bounded by
// both ctx and timeout instead of potentially hanging forever if the
// holder never releases.
func acquireLock(ctx context.Context, path string, timeout time.Duration) (*fileLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(timeout)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &fileLock{f: f}, nil
		}
		if time.Now().After(deadline) {
			f.Close()
			return nil, fmt.Errorf("timed out after %s waiting for lock on %s", timeout, path)
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (l *fileLock) release() {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
}
