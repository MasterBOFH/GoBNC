package main

import (
	"fmt"
	"os"
	"sync"
)

// rotatingWriter is a minimal size-capped log writer: one active file plus
// one ".1" backup, no external dependency. An unattended multi-day soak
// with no bound on its own log is how a disk fills on day nine and takes
// out the soak that can't easily be restarted — this exists only to prevent
// that, not to be a general logging solution.
type rotatingWriter struct {
	mu      sync.Mutex
	path    string
	maxSize int64
	f       *os.File
	size    int64
}

func newRotatingWriter(path string, maxSize int64) (*rotatingWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat log file: %w", err)
	}
	return &rotatingWriter{path: path, maxSize: maxSize, f: f, size: info.Size()}, nil
}

func (w *rotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.size+int64(len(p)) > w.maxSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *rotatingWriter) rotate() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	backup := w.path + ".1"
	_ = os.Remove(backup)
	if err := os.Rename(w.path, backup); err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	w.size = 0
	return nil
}

func (w *rotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
