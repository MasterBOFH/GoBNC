// Package retecloud is the gobnc-retecloud module: it serves the ReteCloud WebSocket protocol in
// lite mode (Rete on iOS) for every network of the bouncer over one WebSocket, on top of GoBNC's
// module API. The protocol is retecloud-api's (documentation/WEBSOCKET_PROTOCOL_LITE.md, pinned
// revision in docs/module-api-design.md); only authenticate differs.
package retecloud

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/MasterBOFH/GoBNC/internal/config"
)

// Config is the module's "config" object in gobnc.json.
type Config struct {
	ListenAddr     string   `json:"listen_addr"`
	TLSCert        string   `json:"tls_cert"`
	TLSKey         string   `json:"tls_key"`
	MaxConnections int      `json:"max_connections"`
	AllowedIPs     []string `json:"allowed_ips"`
	// CompressionLevel is the deflate-stream flate level: 0 = default (1), 1-9, negative = not
	// offered.
	CompressionLevel int `json:"compression_level"`
}

// Defaults.
const (
	DefaultListenAddr     = "127.0.0.1:8443"
	DefaultMaxConnections = 16
)

// ParseConfig reads the module config JSON (GOBNC_MODULE_CONFIG) and applies defaults.
func ParseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("retecloud config: %w", err)
		}
	}
	if c.ListenAddr == "" {
		c.ListenAddr = DefaultListenAddr
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = DefaultMaxConnections
	}
	if c.TLSCert == "" || c.TLSKey == "" {
		return c, errors.New("retecloud config: tls_cert and tls_key are required")
	}
	if _, err := config.ParseAllowedIPs(c.AllowedIPs); err != nil {
		return c, fmt.Errorf("retecloud config: %w", err)
	}
	return c, nil
}
