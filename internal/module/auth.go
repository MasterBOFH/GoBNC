package module

import (
	"context"
	"encoding/json"
	"net"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodAuthCheck, handleAuthCheck, wire.FeatureAuth)
}

// Failed auth.check calls per remote IP within authFailWindow before further checks fail
// without verifying.
const (
	authFailLimit  = 5
	authFailWindow = 60 * time.Second
)

// handleAuthCheck verifies a module client's credential the way the IRC downlink does: the
// bouncer password (when allow_password_auth) or a client-certificate SHA-256 registered in
// cert_fingerprints (when allow_cert_auth), from an address allowed by allowed_ips.
func handleAuthCheck(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.AuthCheckParams](params)
	if err != nil {
		return nil, err
	}
	if (p.Password == "") == (p.CertSHA256 == "") {
		return nil, wire.Errorf(wire.CodeBadParams, "exactly one of password and cert_sha256")
	}
	s := c.srv
	cfg := s.backend.Config()
	if !ipAllowed(cfg, p.RemoteIP) {
		return wire.AuthCheckResult{Reason: wire.AuthInvalid}, nil
	}
	if wait := s.authLimited(p.RemoteIP); wait > 0 {
		return wire.AuthCheckResult{Reason: wire.AuthRateLimited, RetryAfter: wait}, nil
	}
	ok := false
	db := s.backend.Store()
	switch {
	case p.Password != "" && cfg.AllowPasswordAuth:
		hash, err := db.PasswordHash(ctx)
		if err != nil {
			return nil, err
		}
		ok = hash != "" && auth.VerifyPassword(hash, p.Password)
	case p.CertSHA256 != "" && cfg.AllowCertAuth:
		fp, err := auth.NormalizeFingerprint(p.CertSHA256)
		if err == nil {
			if ok, err = db.HasFingerprint(ctx, fp); err != nil {
				return nil, err
			}
		}
	}
	if !ok {
		s.authFailed(p.RemoteIP)
		return wire.AuthCheckResult{Reason: wire.AuthInvalid}, nil
	}
	return wire.AuthCheckResult{OK: true}, nil
}

// ipAllowed applies allowed_ips (empty = everyone; an unparsable address is refused when a
// restriction is configured).
func ipAllowed(cfg config.Config, ip string) bool {
	nets, err := config.ParseAllowedIPs(cfg.AllowedIPs)
	if err != nil {
		return false
	}
	if len(nets) == 0 {
		return true
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}

// authLimited returns 0 when ip may try, else the whole seconds (rounded up) until fewer than
// authFailLimit of its failures fall within the window.
func (s *Server) authLimited(ip string) int {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	fails := s.recentFailuresLocked(ip)
	if len(fails) < authFailLimit {
		return 0
	}
	free := fails[len(fails)-authFailLimit].Add(authFailWindow).Sub(s.now())
	return max(1, int((free+time.Second-1)/time.Second))
}

func (s *Server) authFailed(ip string) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.authFails == nil {
		s.authFails = map[string][]time.Time{}
	}
	s.authFails[ip] = append(s.recentFailuresLocked(ip), s.now())
}

func (s *Server) recentFailuresLocked(ip string) []time.Time {
	cutoff := s.now().Add(-authFailWindow)
	fails := s.authFails[ip]
	i := 0
	for i < len(fails) && !fails[i].After(cutoff) {
		i++
	}
	fails = fails[i:]
	if len(fails) == 0 {
		delete(s.authFails, ip)
	} else {
		s.authFails[ip] = fails
	}
	return fails
}
