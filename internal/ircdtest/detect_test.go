//go:build ircd

package ircd_test

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// Expected DetectIRCd result from each docker/ircd matrix banner.
var wantIRCd = map[string]string{
	"ircd-irc2": irc.IRCdIrc2,
	"unreal4":   irc.IRCdUnreal,
	"hybrid":    irc.IRCdHybrid,
	"ircu2":     irc.IRCdIrcu,
	"bahamut":   irc.IRCdBahamut,
	"ngircd":    irc.IRCdNgIRCd,
	"charybdis": irc.IRCdCharybdis,
	"inspircd":  irc.IRCdInspIRCd,
	"ergo":      irc.IRCdErgo,
}

func TestIRCdDetectionLive(t *testing.T) {
	if os.Getenv("GOBNC_IRCD") == "0" {
		t.Skip("GOBNC_IRCD=0")
	}
	base := fmt.Sprintf("d%d", time.Now().Unix()%100000)
	for _, srv := range servers {
		srv := srv
		want, ok := wantIRCd[srv.Name]
		if !ok {
			t.Fatalf("no expected family for %s — update wantIRCd", srv.Name)
		}
		t.Run(srv.Name, func(t *testing.T) {
			t.Parallel()
			nick := sanitizeNick(base + srv.Name)
			r002, r004, err := fetchWelcome(srv.Addr, nick)
			if err != nil {
				t.Fatal(err)
			}
			trail002 := trailing(r002)
			ver004 := version004(r004)
			got := irc.DetectIRCd(trail002)
			if got == "" {
				got = irc.DetectIRCdFrom004(ver004)
			}
			t.Logf("002=%q 004ver=%q -> %q", trail002, ver004, got)
			if got != want {
				t.Fatalf("DetectIRCd=%q want %q (002=%q 004=%q)", got, want, trail002, ver004)
			}
		})
	}
}

func fetchWelcome(addr, nick string) (r002, r004 string, err error) {
	c, err := net.DialTimeout("tcp", addr, 8*time.Second)
	if err != nil {
		return "", "", err
	}
	defer c.Close()
	r := bufio.NewReader(c)
	write := func(s string) error {
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, e := c.Write([]byte(s + "\r\n"))
		return e
	}
	// Prefer plain registration — classic ircd2 rejects CAP (421).
	_ = write("NICK " + nick)
	_ = write("USER " + nick + " 0 * :probe")

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, e := r.ReadString('\n')
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Timeout() {
				continue // ident / "please wait" stalls on some ircds
			}
			if r002 != "" {
				return r002, r004, nil
			}
			return r002, r004, e
		}
		line = strings.TrimRight(line, "\r\n")
		body := stripTags(line)
		msg, perr := irc.Parse(body)
		if perr != nil {
			continue
		}
		switch msg.Command {
		case "PING":
			trail := msg.Trailing()
			if trail == "" {
				trail = msg.Param(0)
			}
			_ = write("PONG :" + trail)
		case "002":
			r002 = body
		case "004":
			r004 = body
		case "376", "422":
			if r002 != "" {
				return r002, r004, nil
			}
		}
	}
	if r002 == "" && r004 == "" {
		return "", "", fmt.Errorf("no 002/004 from %s", addr)
	}
	return r002, r004, nil
}

func trailing(line string) string {
	msg, err := irc.Parse(stripTags(line))
	if err != nil {
		if i := strings.Index(line, " :"); i >= 0 {
			return line[i+2:]
		}
		return line
	}
	return msg.Trailing()
}

func version004(line string) string {
	msg, err := irc.Parse(stripTags(line))
	if err != nil || len(msg.Params) < 3 {
		return ""
	}
	return msg.Params[2]
}

func stripTags(line string) string {
	if line == "" || line[0] != '@' {
		return line
	}
	i := strings.IndexByte(line, ' ')
	if i < 0 {
		return line
	}
	return strings.TrimLeft(line[i+1:], " ")
}
