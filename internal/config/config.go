// Package config loads bootstrap settings (paths, listen, TLS).
// Network/auth details live in SQLite after first run.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/version"
)

// Config is process bootstrap configuration.
type Config struct {
	ListenAddr    string `json:"listen_addr"`
	TLSCert       string `json:"tls_cert"`
	TLSKey        string `json:"tls_key"`
	DBPath        string `json:"db_path"`
	ControlSocket string `json:"control_socket"`
	LogLevel      string `json:"log_level"`
	LogFile       string `json:"log_file,omitempty"` // JSON logs; in debug mode console stays human-readable

	// QuitMessage is sent as QUIT to all uplinks on process shutdown (not per-network).
	// Empty uses version.QuitMessage() ("GoBNC <version>").
	QuitMessage string `json:"quit_message"`

	// Auth modes: either or both may be enabled. If neither, fail closed.
	AllowPasswordAuth bool `json:"allow_password_auth"`
	AllowCertAuth     bool `json:"allow_cert_auth"`
}

// Default returns sensible defaults for local development.
func Default() Config {
	return Config{
		ListenAddr:        "127.0.0.1:6697",
		TLSCert:           "certs/server.crt",
		TLSKey:            "certs/server.key",
		DBPath:            "gobnc.db",
		ControlSocket:     "gobnc.sock",
		LogLevel:          "info",
		QuitMessage:       version.QuitMessage(),
		AllowPasswordAuth: true,
		AllowCertAuth:     true,
	}
}

// ShutdownTimeout is how long graceful uplink flush+QUIT may take before forced close.
const ShutdownTimeout = 5 * time.Second

// QuitReason returns the QUIT text for uplink shutdown.
func (c Config) QuitReason() string {
	if c.QuitMessage == "" {
		return version.QuitMessage()
	}
	return c.QuitMessage
}

// LoadJSON loads config from path; missing file returns Default.
func LoadJSON(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// Validate checks required fields for serving.
func (c Config) Validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("listen_addr required")
	}
	if c.DBPath == "" {
		return fmt.Errorf("db_path required")
	}
	if !c.AllowPasswordAuth && !c.AllowCertAuth {
		return fmt.Errorf("at least one of allow_password_auth or allow_cert_auth must be true")
	}
	return nil
}
