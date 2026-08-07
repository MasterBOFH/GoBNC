// Package config loads bootstrap settings (paths, listen, TLS).
// Network/auth details live in SQLite after first run.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/version"
)

// DefaultMaxClients is the default concurrent downlink connection limit.
const DefaultMaxClients = 32

// DefaultMaxFloodQueue is the default max uplink flood send-queue depth.
// Sized for reconnect / catch-up bursts; 0 disables the cap.
const DefaultMaxFloodQueue = 16384

// DefaultLegacyPlaybackMax is the default max PRIVMSG/NOTICE lines per target on attach.
const DefaultLegacyPlaybackMax = 50000

// DefaultChatHistoryMax is the default max lines per CHATHISTORY query (ISUPPORT CHATHISTORY=N).
const DefaultChatHistoryMax = 100

// Config is process bootstrap configuration.
type Config struct {
	ListenAddr    string `json:"listen_addr"`
	TLSCert       string `json:"tls_cert"`
	TLSKey        string `json:"tls_key"`
	DBPath        string `json:"db_path"`
	ControlSocket string `json:"control_socket"`
	PidFile       string `json:"pid_file,omitempty"`
	LogLevel      string `json:"log_level"`
	LogFile       string `json:"log_file,omitempty"` // JSON logs; in debug mode console stays human-readable

	// MaxClients caps concurrent TLS client connections (0 = DefaultMaxClients).
	MaxClients int `json:"max_clients,omitempty"`

	// MaxFloodQueue caps paced uplink send queue depth (0 = unlimited).
	MaxFloodQueue int `json:"max_flood_queue"`

	// LegacyPlaybackMax caps PRIVMSG/NOTICE lines per target on legacy attach (0 = unlimited).
	LegacyPlaybackMax int `json:"legacy_playback_max"`

	// ChatHistoryMax is the max lines returned per CHATHISTORY query and advertised
	// as ISUPPORT CHATHISTORY=N. Distinct from LegacyPlaybackMax (attach backlog).
	// 0 or negative uses DefaultChatHistoryMax.
	ChatHistoryMax int `json:"chathistory_max"`

	// HistoryRetentionDays prunes stored messages older than N days (0 = no prune).
	HistoryRetentionDays int `json:"history_retention_days"`

	// QuitMessage is sent as QUIT to all uplinks on process shutdown (not per-network).
	// Empty uses version.QuitMessage() ("GoBNC <version>").
	QuitMessage string `json:"quit_message"`

	// Auth modes: either or both may be enabled. If neither, fail closed.
	AllowPasswordAuth bool `json:"allow_password_auth"`
	AllowCertAuth     bool `json:"allow_cert_auth"`
}

// Default returns sensible defaults for local development.
// ControlSocket empty means ResolvedControlSocket picks a private per-user path.
func Default() Config {
	return Config{
		ListenAddr:           "127.0.0.1:6697",
		TLSCert:              "certs/server.crt",
		TLSKey:               "certs/server.key",
		DBPath:               "gobnc.db",
		ControlSocket:        "",
		LogLevel:             "info",
		MaxClients:           DefaultMaxClients,
		MaxFloodQueue:        DefaultMaxFloodQueue,
		LegacyPlaybackMax:    DefaultLegacyPlaybackMax,
		ChatHistoryMax:       DefaultChatHistoryMax,
		HistoryRetentionDays: 0,
		QuitMessage:          version.QuitMessage(),
		AllowPasswordAuth:    true,
		AllowCertAuth:        true,
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

// ResolvedControlSocket returns the Unix control socket path.
// Empty or the legacy default "gobnc.sock" resolve to a private location:
// $XDG_RUNTIME_DIR/gobnc/gobnc.sock when set, otherwise ~/.gobnc/gobnc.sock.
// Any other explicit control_socket value is returned unchanged.
func (c Config) ResolvedControlSocket() string {
	switch c.ControlSocket {
	case "", "gobnc.sock":
		return filepath.Join(defaultStateDir(), "gobnc.sock")
	default:
		return c.ControlSocket
	}
}

// ResolvedPidFile returns the PID file path.
// Empty or "gobnc.pid" → $XDG_RUNTIME_DIR/gobnc/gobnc.pid or ~/.gobnc/gobnc.pid.
func (c Config) ResolvedPidFile() string {
	switch c.PidFile {
	case "", "gobnc.pid":
		return filepath.Join(defaultStateDir(), "gobnc.pid")
	default:
		return c.PidFile
	}
}

// ResolvedLogFile returns log_file, or a default under the state dir when
// useDefault is true and log_file is empty (daemon mode without an explicit path).
func (c Config) ResolvedLogFile(useDefault bool) string {
	if c.LogFile != "" {
		return c.LogFile
	}
	if useDefault {
		return filepath.Join(defaultStateDir(), "gobnc.log")
	}
	return ""
}

func defaultStateDir() string {
	if runtimeDir := os.Getenv("XDG_RUNTIME_DIR"); runtimeDir != "" {
		return filepath.Join(runtimeDir, "gobnc")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "."
	}
	return filepath.Join(home, ".gobnc")
}

// LoadJSON loads config from path; missing file returns Default.
// // line comments and /* block comments */ are allowed (JSONC-style).
func LoadJSON(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	data, err = stripJSONC(data)
	if err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if cfg.ChatHistoryMax <= 0 {
		cfg.ChatHistoryMax = DefaultChatHistoryMax
	}
	return cfg, nil
}

// stripJSONC removes // and /* */ comments outside of JSON strings.
func stripJSONC(in []byte) ([]byte, error) {
	var out []byte
	inString := false
	escaped := false
	for i := 0; i < len(in); i++ {
		c := in[i]
		if inString {
			out = append(out, c)
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		if c == '"' {
			inString = true
			out = append(out, c)
			continue
		}
		if c == '/' && i+1 < len(in) {
			switch in[i+1] {
			case '/':
				i += 2
				for i < len(in) && in[i] != '\n' {
					i++
				}
				if i < len(in) {
					out = append(out, '\n')
				}
				continue
			case '*':
				i += 2
				for i+1 < len(in) && !(in[i] == '*' && in[i+1] == '/') {
					i++
				}
				if i+1 >= len(in) {
					return nil, fmt.Errorf("unterminated block comment")
				}
				i++ // skip '/'
				continue
			}
		}
		out = append(out, c)
	}
	if inString {
		return nil, fmt.Errorf("unterminated string")
	}
	return out, nil
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
