// Package config loads bootstrap settings (paths, listen, TLS).
// Network/auth details live in SQLite after first run.
package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	ListenAddr string `json:"listen_addr"`
	TLSCert    string `json:"tls_cert"`
	TLSKey     string `json:"tls_key"`
	// TLSClientCert / TLSClientKey are the global uplink client identity (CERTFP / SASL EXTERNAL).
	// Empty means no global client cert; per-network paths override or inherit these.
	TLSClientCert string `json:"tls_client_cert,omitempty"`
	TLSClientKey  string `json:"tls_client_key,omitempty"`
	// BindHost is the default local address for uplink dials (IP or hostname).
	// Empty = OS default. Per-network bind_host overrides; none or - disables.
	BindHost      string `json:"bind_host,omitempty"`
	DBPath        string `json:"db_path"`
	ControlSocket string `json:"control_socket"`
	PidFile       string `json:"pid_file,omitempty"`
	LogLevel      string `json:"log_level"`
	LogFile       string `json:"log_file,omitempty"` // JSON logs; in debug mode console stays human-readable

	// MaxClients caps concurrent TLS client connections (0 = DefaultMaxClients).
	MaxClients int `json:"max_clients,omitempty"`

	// PingIdleSeconds / PingGraceSeconds control the downlink client keepalive
	// probe: after this many seconds of silence from a client, gobnc sends it
	// a PING; if it doesn't answer within the grace period, gobnc closes the
	// connection. 0 uses the package defaults (downlink.KeepaliveIdle/Grace,
	// 300s/120s). Raise these if a client's own periodic ping runs on a
	// longer interval than that.
	PingIdleSeconds  int `json:"ping_idle_seconds,omitempty"`
	PingGraceSeconds int `json:"ping_grace_seconds,omitempty"`

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

	// CTCPPing / CTCPVersion / CTCPOther control how incoming CTCP requests
	// (any CTCP-framed PRIVMSG, whether targeted at our own nick or a
	// channel we're in) are handled: "relay" forwards live to attached
	// downlinks unchanged (default); "edge" answers the request directly
	// from the bouncer over the uplink instead of relaying it; "disable"
	// drops the request entirely (no reply from anyone). CTCP requests are
	// never stored for chathistory/legacy replay regardless of mode.
	// CTCPOther (every CTCP verb besides PING/VERSION/ACTION) only supports
	// "relay"/"disable" — there is no bouncer-side reply for those.
	CTCPPing    string `json:"ctcp_ping,omitempty"`
	CTCPVersion string `json:"ctcp_version,omitempty"`
	CTCPOther   string `json:"ctcp_other,omitempty"`

	// QuitMessage is sent as QUIT to all uplinks on process shutdown (not per-network).
	// Empty uses version.QuitMessage() ("GoBNC <version>").
	QuitMessage string `json:"quit_message"`

	// Defaults applied by `network add` when nick / identity flags are omitted.
	DefaultNick     string `json:"default_nick,omitempty"`
	DefaultUsername string `json:"default_username,omitempty"`
	DefaultRealname string `json:"default_realname,omitempty"`
	DefaultAltNick  string `json:"default_alt_nick,omitempty"`

	// Auth modes: either or both may be enabled. If neither, fail closed.
	AllowPasswordAuth bool `json:"allow_password_auth"`
	AllowCertAuth     bool `json:"allow_cert_auth"`

	// AllowedIPs restricts which source addresses may complete a downlink
	// connection at all. Each entry is a CIDR ("10.0.0.0/8") or a bare IP
	// (treated as a /32 or /128). Empty means unrestricted — the default,
	// for backward compatibility with existing configs. Enforced by
	// internal/downlink before the TLS handshake, let alone any IRC line.
	AllowedIPs []string `json:"allowed_ips,omitempty"`
}

// DefaultUsername / DefaultRealnameFallback are used when config defaults are empty.
const (
	DefaultUsernameFallback = "gobnc"
	DefaultRealnameFallback = "GoBNC"
)

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
		CTCPPing:             "relay",
		CTCPVersion:          "relay",
		CTCPOther:            "relay",
		DefaultUsername:      DefaultUsernameFallback,
		DefaultRealname:      DefaultRealnameFallback,
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

// NetworkIdentityDefaults returns nick/username/realname/alt_nick for `network add`
// when those fields are not set on the command line.
func (c Config) NetworkIdentityDefaults() (nick, username, realname, altNick string) {
	nick = c.DefaultNick
	username = c.DefaultUsername
	if username == "" {
		username = DefaultUsernameFallback
	}
	realname = c.DefaultRealname
	if realname == "" {
		realname = DefaultRealnameFallback
	}
	altNick = c.DefaultAltNick
	return nick, username, realname, altNick
}

// ResolveTLSClientCert picks uplink client cert/key paths.
// Network tls_cert/tls_key empty inherits global tls_client_*; "none" or "-" disables.
// Both cert and key must be non-empty for ok=true.
func ResolveTLSClientCert(netCert, netKey, globalCert, globalKey string) (cert, key string, ok bool) {
	cert, key = netCert, netKey
	if isTLSClientCertDisabled(cert) || isTLSClientCertDisabled(key) {
		return "", "", false
	}
	if cert == "" && key == "" {
		cert, key = globalCert, globalKey
	}
	if isTLSClientCertDisabled(cert) || isTLSClientCertDisabled(key) {
		return "", "", false
	}
	if cert == "" || key == "" {
		return "", "", false
	}
	return cert, key, true
}

func isTLSClientCertDisabled(p string) bool {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case "none", "-":
		return true
	default:
		return false
	}
}

// ResolveBindHost picks the local bind address for an uplink dial.
// Network bind_host empty inherits global bind_host; "none" or "-" disables.
func ResolveBindHost(netHost, globalHost string) string {
	netHost = strings.TrimSpace(netHost)
	if isBindHostDisabled(netHost) {
		return ""
	}
	if netHost != "" {
		return netHost
	}
	globalHost = strings.TrimSpace(globalHost)
	if isBindHostDisabled(globalHost) {
		return ""
	}
	return globalHost
}

func isBindHostDisabled(p string) bool {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case "none", "-":
		return true
	default:
		return false
	}
}

// DialLocalAddr resolves bindHost to a TCP local address for net.Dialer.LocalAddr.
// bindHost may be an IP or a hostname that resolves to a local interface address.
// Empty bindHost returns (nil, nil).
func DialLocalAddr(bindHost string) (net.Addr, error) {
	bindHost = strings.TrimSpace(bindHost)
	if bindHost == "" {
		return nil, nil
	}
	if host, port, err := net.SplitHostPort(bindHost); err == nil {
		// Allow "1.2.3.4:0" style; non-zero ports are unusual for bind but accepted.
		ip := net.ParseIP(host)
		if ip == nil {
			return nil, fmt.Errorf("bind_host %q: host is not an IP", bindHost)
		}
		p, err := strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("bind_host %q: %w", bindHost, err)
		}
		return &net.TCPAddr{IP: ip, Port: p}, nil
	}
	if ip := net.ParseIP(bindHost); ip != nil {
		return &net.TCPAddr{IP: ip}, nil
	}
	ips, err := net.LookupIP(bindHost)
	if err != nil {
		return nil, fmt.Errorf("bind_host %q: %w", bindHost, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("bind_host %q: no addresses", bindHost)
	}
	return &net.TCPAddr{IP: ips[0]}, nil
}

// ResolvedControlSocket returns the Unix control socket path.
// Empty or the legacy default "gobnc.sock" resolve to a private location:
// $XDG_RUNTIME_DIR/gobnc/gobnc.sock when set, otherwise ~/.gobnc/gobnc.sock.
// Any other explicit control_socket value is returned unchanged.
func (c Config) ResolvedControlSocket() string {
	switch c.ControlSocket {
	case "", "gobnc.sock":
		return filepath.Join(DefaultStateDir(), "gobnc.sock")
	default:
		return c.ControlSocket
	}
}

// ResolvedPidFile returns the PID file path.
// Empty or "gobnc.pid" → $XDG_RUNTIME_DIR/gobnc/gobnc.pid or ~/.gobnc/gobnc.pid.
func (c Config) ResolvedPidFile() string {
	switch c.PidFile {
	case "", "gobnc.pid":
		return filepath.Join(DefaultStateDir(), "gobnc.pid")
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
		return filepath.Join(DefaultStateDir(), "gobnc.log")
	}
	return ""
}

// DefaultStateDir is gobnc's private per-user state directory:
// $XDG_RUNTIME_DIR/gobnc when set, otherwise ~/.gobnc. Exported so other
// packages that need a gobnc-adjacent file location (e.g. internal/keeperboot's
// keeper socket/pidfile/lock) follow the exact same convention as
// pid_file/control_socket/log_file rather than inventing a parallel one.
func DefaultStateDir() string {
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
	if cfg.CTCPPing == "" {
		cfg.CTCPPing = "relay"
	}
	if cfg.CTCPVersion == "" {
		cfg.CTCPVersion = "relay"
	}
	if cfg.CTCPOther == "" {
		cfg.CTCPOther = "relay"
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
	if _, err := ParseAllowedIPs(c.AllowedIPs); err != nil {
		return err
	}
	if c.CTCPPing != "relay" && c.CTCPPing != "edge" && c.CTCPPing != "disable" {
		return fmt.Errorf("ctcp_ping: invalid value %q (want relay, edge, or disable)", c.CTCPPing)
	}
	if c.CTCPVersion != "relay" && c.CTCPVersion != "edge" && c.CTCPVersion != "disable" {
		return fmt.Errorf("ctcp_version: invalid value %q (want relay, edge, or disable)", c.CTCPVersion)
	}
	if c.CTCPOther != "relay" && c.CTCPOther != "disable" {
		return fmt.Errorf("ctcp_other: invalid value %q (want relay or disable)", c.CTCPOther)
	}
	return nil
}

// ParseAllowedIPs compiles AllowedIPs entries (CIDR or bare IP) into
// *net.IPNet for fast membership checks. A bare IP is treated as an
// exact-host CIDR (/32 for IPv4, /128 for IPv6). Called from Validate (so a
// malformed entry fails config load/rehash outright, matching this field's
// fail-closed intent) and by internal/downlink to build its runtime filter.
func ParseAllowedIPs(entries []string) ([]*net.IPNet, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	out := make([]*net.IPNet, 0, len(entries))
	for _, raw := range entries {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			return nil, fmt.Errorf("allowed_ips: empty entry")
		}
		if !strings.Contains(entry, "/") {
			ip := net.ParseIP(entry)
			if ip == nil {
				return nil, fmt.Errorf("allowed_ips: invalid IP %q", entry)
			}
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			entry = fmt.Sprintf("%s/%d", entry, bits)
		}
		_, ipnet, err := net.ParseCIDR(entry)
		if err != nil {
			return nil, fmt.Errorf("allowed_ips: invalid CIDR %q: %w", raw, err)
		}
		out = append(out, ipnet)
	}
	return out, nil
}
