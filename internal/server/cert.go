package server

import (
	"crypto/tls"
	"fmt"
	"sync"
)

// certHolder holds the listener certificate for hot-reload via GetCertificate.
type certHolder struct {
	mu   sync.RWMutex
	cert *tls.Certificate
}

// Load replaces the in-memory certificate from PEM files on disk.
func (h *certHolder) Load(certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("load tls: %w", err)
	}
	h.mu.Lock()
	h.cert = &cert
	h.mu.Unlock()
	return nil
}

// GetCertificate implements tls.Config.GetCertificate.
func (h *certHolder) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.cert == nil {
		return nil, fmt.Errorf("no tls certificate loaded")
	}
	return h.cert, nil
}
