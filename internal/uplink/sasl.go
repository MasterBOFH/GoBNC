package uplink

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/connio"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/xdg-go/scram"
)

// IRCv3 AUTHENTICATE base64 chunks are at most 400 bytes.
const authenticateChunkSize = 400

func (u *Uplink) saslWanted() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.cfg.Network.SASL
}

func hasClientCert(tc *tls.Config) bool {
	if tc == nil {
		return false
	}
	if len(tc.Certificates) > 0 {
		return true
	}
	return tc.GetClientCertificate != nil
}

func clientCertPathsConfigured(n store.Network, globalCert, globalKey string) bool {
	_, _, ok := config.ResolveTLSClientCert(n.TLSCert, n.TLSKey, globalCert, globalKey)
	return ok
}

func (u *Uplink) noteSASLOffer(val string, present bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !present {
		u.saslAvailable = false
		u.saslMechs = nil
		return
	}
	u.saslAvailable = true
	if val == "" {
		return
	}
	var mechs []string
	for _, m := range strings.Split(val, ",") {
		m = strings.TrimSpace(m)
		if m != "" {
			mechs = append(mechs, m)
		}
	}
	if len(mechs) > 0 {
		u.saslMechs = mechs
	}
}

func (u *Uplink) clearSASLExchange() {
	u.mu.Lock()
	u.saslMech = ""
	u.scramConv = nil
	u.mu.Unlock()
}

// noteAccountFrom900 stores the account from RPL_LOGGEDIN.
// Format: 900 <nick> <nick>!<user>@<host> <account> :You are now logged in as …
func (u *Uplink) noteAccountFrom900(msg irc.Message) {
	if len(msg.Params) < 3 {
		return
	}
	acct := msg.Params[2]
	if acct == "" || acct == "*" {
		return
	}
	u.mu.Lock()
	u.account = acct
	u.mu.Unlock()
}

// startSASL begins AUTHENTICATE with a mechanism we can complete.
func (u *Uplink) startSASL(c *connio.Conn) error {
	mech, ok := u.pickSASLMech()
	if !ok {
		u.mu.RLock()
		required := u.cfg.Network.SASLRequired
		mechs := append([]string(nil), u.saslMechs...)
		u.mu.RUnlock()
		u.log.Info("SASL: no supported mechanism", "offered", mechs)
		if required {
			return fmt.Errorf("SASL: no supported mechanism in %v", mechs)
		}
		if !u.Registered() {
			return c.WriteLine("CAP END")
		}
		return nil
	}
	u.mu.Lock()
	u.saslMech = mech
	u.scramConv = nil
	u.mu.Unlock()
	return c.WriteLine("AUTHENTICATE " + mech)
}

// pickSASLMech prefers SCRAM-SHA-256, then PLAIN, then EXTERNAL.
// EXTERNAL only when SASL is enabled with no user/pass and a client cert is available.
// Empty advertised list (pre-302 / no value): PLAIN if password auth, else EXTERNAL with cert.
func (u *Uplink) pickSASLMech() (string, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	n := u.cfg.Network
	if !n.SASL {
		return "", false
	}
	passwordOK := n.SASLUser != "" && n.SASLPass != ""
	externalOK := n.SASLUser == "" && n.SASLPass == "" &&
		(hasClientCert(u.cfg.TLSConf) ||
			clientCertPathsConfigured(n, u.cfg.GlobalTLSClientCert, u.cfg.GlobalTLSClientKey))

	var preferred []string
	if passwordOK {
		preferred = append(preferred, "SCRAM-SHA-256", "PLAIN")
	}
	if externalOK {
		preferred = append(preferred, "EXTERNAL")
	}
	if len(preferred) == 0 {
		return "", false
	}

	if len(u.saslMechs) == 0 {
		if passwordOK {
			return "PLAIN", true
		}
		return "EXTERNAL", true
	}

	for _, want := range preferred {
		for _, m := range u.saslMechs {
			if strings.EqualFold(m, want) {
				return want, true
			}
		}
	}
	return "", false
}

// handleAuthenticate continues an in-progress SASL exchange.
func (u *Uplink) handleAuthenticate(c *connio.Conn, msg irc.Message) error {
	param := msg.Param(0)
	if param == "" {
		return nil
	}
	if param == "*" {
		u.clearSASLExchange()
		return nil
	}

	u.mu.RLock()
	mech := u.saslMech
	user, pass := u.cfg.Network.SASLUser, u.cfg.Network.SASLPass
	u.mu.RUnlock()
	if mech == "" {
		return nil
	}

	switch strings.ToUpper(mech) {
	case "PLAIN":
		if param != "+" {
			return nil
		}
		if user == "" || pass == "" {
			return u.abortSASL(c, fmt.Errorf("SASL PLAIN: missing username/password"))
		}
		payload := "\x00" + user + "\x00" + pass
		return writeAuthenticate(c, base64.StdEncoding.EncodeToString([]byte(payload)))

	case "EXTERNAL":
		if param != "+" {
			return nil
		}
		if user == "" {
			return writeAuthenticate(c, "+")
		}
		return writeAuthenticate(c, base64.StdEncoding.EncodeToString([]byte(user)))

	case "SCRAM-SHA-256":
		return u.handleSCRAM(c, param, user, pass)

	default:
		return u.abortSASL(c, fmt.Errorf("SASL: unexpected mechanism %q", mech))
	}
}

func (u *Uplink) handleSCRAM(c *connio.Conn, param, user, pass string) error {
	if user == "" || pass == "" {
		return u.abortSASL(c, fmt.Errorf("SASL SCRAM-SHA-256: missing username/password"))
	}

	var challenge string
	if param == "+" {
		challenge = ""
	} else {
		raw, err := base64.StdEncoding.DecodeString(param)
		if err != nil {
			return u.abortSASL(c, fmt.Errorf("SASL SCRAM: bad challenge encoding: %w", err))
		}
		challenge = string(raw)
	}

	u.mu.Lock()
	conv := u.scramConv
	if conv == nil {
		client, err := scram.SHA256.NewClient(user, pass, "")
		if err != nil {
			u.mu.Unlock()
			return u.abortSASL(c, err)
		}
		conv = client.NewConversation()
		u.scramConv = conv
	}
	u.mu.Unlock()

	resp, err := conv.Step(challenge)
	if err != nil {
		return u.abortSASL(c, err)
	}
	if conv.Done() && resp == "" {
		return nil
	}
	if resp == "" {
		return writeAuthenticate(c, "+")
	}
	return writeAuthenticate(c, base64.StdEncoding.EncodeToString([]byte(resp)))
}

func (u *Uplink) abortSASL(c *connio.Conn, err error) error {
	u.clearSASLExchange()
	_ = c.WriteLine("AUTHENTICATE *")
	u.mu.RLock()
	required := u.cfg.Network.SASLRequired
	u.mu.RUnlock()
	if required {
		return err
	}
	u.log.Info("SASL aborted", "err", err)
	if !u.Registered() {
		return c.WriteLine("CAP END")
	}
	return nil
}

func writeAuthenticate(c *connio.Conn, b64 string) error {
	if b64 == "" || b64 == "+" {
		return c.WriteLine("AUTHENTICATE +")
	}
	for {
		if len(b64) > authenticateChunkSize {
			if err := c.WriteLine("AUTHENTICATE " + b64[:authenticateChunkSize]); err != nil {
				return err
			}
			b64 = b64[authenticateChunkSize:]
			continue
		}
		if err := c.WriteLine("AUTHENTICATE " + b64); err != nil {
			return err
		}
		if len(b64) == authenticateChunkSize {
			return c.WriteLine("AUTHENTICATE +")
		}
		return nil
	}
}
