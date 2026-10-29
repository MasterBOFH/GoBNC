package retecloud

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/MasterBOFH/GoBNC/internal/config"
)

// Listener serves WSS at /ws: TLS (a client certificate is requested, never required),
// allowed_ips, and at most MaxConnections WebSockets at a time (more get HTTP 503).
type Listener struct {
	cfg     Config
	log     *slog.Logger
	nets    []*net.IPNet
	slots   chan struct{}
	upgrade websocket.Upgrader
	// accept serves one upgraded WebSocket until it closes. certSHA256 is the client
	// certificate's fingerprint ("" without one); remoteIP has no port.
	accept func(ws *websocket.Conn, certSHA256, remoteIP string)
}

// NewListener builds the WSS listener; accept runs for each WebSocket.
func NewListener(cfg Config, log *slog.Logger, accept func(ws *websocket.Conn, certSHA256, remoteIP string)) (*Listener, error) {
	nets, err := config.ParseAllowedIPs(cfg.AllowedIPs)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return &Listener{cfg: cfg, log: log, nets: nets, slots: make(chan struct{}, cfg.MaxConnections), accept: accept,
		upgrade: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}}, nil
}

// Serve listens on cfg.ListenAddr until ctx ends.
func (l *Listener) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", l.cfg.ListenAddr)
	if err != nil {
		return err
	}
	return l.ServeListener(ctx, ln)
}

// ServeListener serves on an existing TCP listener (tests listen on port 0).
func (l *Listener) ServeListener(ctx context.Context, ln net.Listener) error {
	cert, err := tls.LoadX509KeyPair(l.cfg.TLSCert, l.cfg.TLSKey)
	if err != nil {
		ln.Close()
		return err
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequestClientCert,
		MinVersion: tls.VersionTLS12}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", l.serveWS)
	srv := &http.Server{Handler: mux, TLSConfig: tlsCfg, ReadHeaderTimeout: 10 * time.Second,
		ErrorLog: slog.NewLogLogger(l.log.Handler(), slog.LevelDebug)}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	err = srv.Serve(tls.NewListener(ln, tlsCfg))
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (l *Listener) serveWS(w http.ResponseWriter, r *http.Request) {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	if !l.ipAllowed(ip) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	select {
	case l.slots <- struct{}{}:
	default:
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	defer func() { <-l.slots }()
	ws, err := l.upgrade.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	fp := ""
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		sum := sha256.Sum256(r.TLS.PeerCertificates[0].Raw)
		fp = hex.EncodeToString(sum[:])
	}
	l.accept(ws, fp, ip)
}

func (l *Listener) ipAllowed(ip string) bool {
	if len(l.nets) == 0 {
		return true
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range l.nets {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}
