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

var servers = []struct {
	Name string
	Addr string
}{
	{"ircd-irc2", "127.0.0.1:4440"},
	{"unreal4", "127.0.0.1:4441"},
	{"hybrid", "127.0.0.1:4442"},
	{"ircu2", "127.0.0.1:4443"},
	{"bahamut", "127.0.0.1:4444"},
	{"ngircd", "127.0.0.1:4445"},
	{"charybdis", "127.0.0.1:4447"},
	{"inspircd", "127.0.0.1:4448"},
	{"ergo", "127.0.0.1:6667"},
}

type probeStats struct {
	lines       int
	withTags    int
	seen        map[string]int
	statusmsg   bool
	serverTime  bool
	messageTags bool
	botJoined   bool
	haveOps     bool
	kickSeen    bool
}

type ircClient struct {
	conn  net.Conn
	r     *bufio.Reader
	write func(string) error
}

func TestIRCDParserInterop(t *testing.T) {
	if os.Getenv("GOBNC_IRCD") == "0" {
		t.Skip("GOBNC_IRCD=0")
	}
	base := fmt.Sprintf("g%d", time.Now().Unix()%100000)
	for _, srv := range servers {
		srv := srv
		t.Run(srv.Name, func(t *testing.T) {
			t.Parallel()
			nick := sanitizeNick(base + srv.Name)
			bot := sanitizeNick("b" + base + srv.Name)
			st, err := probeServer(t, srv.Name, srv.Addr, nick, bot)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: lines=%d tagged=%d cmds=%v botJoined=%v haveOps=%v kick=%v statusmsg=%v server-time=%v message-tags=%v",
				srv.Name, st.lines, st.withTags, st.seen, st.botJoined, st.haveOps, st.kickSeen, st.statusmsg, st.serverTime, st.messageTags)
			for _, req := range []string{"001", "JOIN"} {
				if st.seen[req] == 0 {
					t.Errorf("%s: never observed %s", srv.Name, req)
				}
			}
			if !st.botJoined {
				t.Errorf("%s: bot never joined primary's channel", srv.Name)
			} else if st.haveOps && !st.kickSeen {
				t.Errorf("%s: primary was op but KICK was not observed", srv.Name)
			}
		})
	}
}

func sanitizeNick(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i == 0 && r >= '0' && r <= '9' {
			b.WriteByte('n')
		}
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		}
	}
	out := b.String()
	if len(out) > 9 {
		out = out[:9]
	}
	if out == "" {
		out = "gobnc"
	}
	return out
}

func dialClient(addr string) (*ircClient, error) {
	c, err := net.DialTimeout("tcp", addr, 8*time.Second)
	if err != nil {
		return nil, err
	}
	cl := &ircClient{conn: c, r: bufio.NewReader(c)}
	cl.write = func(s string) error {
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := c.Write([]byte(s + "\r\n"))
		return err
	}
	return cl, nil
}

func probeServer(t *testing.T, name, addr, nick, botNick string) (*probeStats, error) {
	t.Helper()
	st := &probeStats{seen: map[string]int{}}
	cm := irc.CaseRFC1459

	ch1 := "#" + sanitizeNick("c1"+nick)
	ch2 := "#" + sanitizeNick("c2"+nick)
	ch3 := "#" + sanitizeNick("c3"+nick)

	primary, err := dialClient(addr)
	if err != nil {
		return st, fmt.Errorf("dial primary: %w", err)
	}
	defer primary.conn.Close()

	bot, err := dialClient(addr)
	if err != nil {
		return st, fmt.Errorf("dial bot: %w", err)
	}
	defer bot.conn.Close()

	isupport := irc.NewISUPPORT()
	record := func(line string) error {
		if line == "" {
			return nil
		}
		st.lines++
		msg, err := irc.Parse(line)
		if err != nil {
			return fmt.Errorf("parse %q: %w", line, err)
		}
		st.seen[msg.Command]++
		if len(msg.Tags) > 0 {
			st.withTags++
		}
		if msg.Command != "" {
			if _, err := irc.Parse(msg.Encode()); err != nil {
				return fmt.Errorf("reparse from %q: %w", line, err)
			}
		}
		if msg.Command == "005" {
			isupport.Parse005(msg.Params)
			if isupport.Raw["STATUSMSG"] != "" {
				st.statusmsg = true
			}
		}
		return nil
	}

	nickPtr := &nick
	botPtr := &botNick
	if err := registerClient(primary, nickPtr, "gobnc", true, st, record); err != nil {
		return st, fmt.Errorf("primary register: %w", err)
	}
	nick = *nickPtr
	if err := registerClient(bot, botPtr, "bot", true, st, record); err != nil {
		return st, fmt.Errorf("bot register: %w", err)
	}
	botNick = *botPtr

	// Keep bot connection alive (PINGs) in background.
	stopBot := make(chan struct{})
	defer close(stopBot)
	go func() {
		for {
			select {
			case <-stopBot:
				return
			default:
			}
			_ = bot.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			raw, err := bot.r.ReadString('\n')
			if err != nil {
				continue
			}
			msg, err := irc.Parse(strings.TrimRight(raw, "\r\n"))
			if err != nil {
				continue
			}
			if msg.Command == "PING" {
				_ = bot.write("PONG :" + msg.Trailing())
			}
		}
	}()

	// 1) Primary joins alone first → channel creator / op on most servers.
	_ = primary.write("JOIN " + ch1)
	if err := waitMatch(primary, 15*time.Second, record, func(msg irc.Message) bool {
		return msg.Command == "JOIN" && cm.Equal(joinChan(msg), ch1)
	}); err != nil {
		return st, fmt.Errorf("primary join %s: %w", ch1, err)
	}
	// Creator +o may arrive as a separate MODE after JOIN.
	_ = waitMatch(primary, 2*time.Second, record, func(msg irc.Message) bool {
		if msg.Command != "MODE" || !cm.Equal(msg.Param(0), ch1) {
			return false
		}
		modes := strings.Join(msg.Params[1:], " ")
		if strings.Contains(modes, "+o") && (strings.Contains(modes, nick) || len(msg.Params) >= 3 && cm.Equal(msg.Param(2), nick)) {
			st.haveOps = true
			return true
		}
		// Some servers send "+o nick" in params.
		for i, p := range msg.Params {
			if p == "+o" && i+1 < len(msg.Params) && cm.Equal(msg.Params[i+1], nick) {
				st.haveOps = true
				return true
			}
		}
		return false
	})
	// If no explicit MODE, still try kick — first joiner is often silently op.
	if !st.haveOps {
		st.haveOps = true // assume until 482 proves otherwise
	}

	// 2) Bot joins; primary must see that JOIN before kicking.
	_ = bot.write("JOIN " + ch1)
	if err := waitMatch(primary, 15*time.Second, record, func(msg irc.Message) bool {
		return msg.Command == "JOIN" && cm.Equal(joinChan(msg), ch1) && cm.Equal(msg.Nick(), botNick)
	}); err != nil {
		return st, fmt.Errorf("bot join visible to primary: %w", err)
	}
	st.botJoined = true

	// 3) Kick immediately (before other traffic / rate limits).
	_ = primary.write("KICK " + ch1 + " " + botNick + " :out")
	_ = waitMatch(primary, 8*time.Second, record, func(msg irc.Message) bool {
		switch msg.Command {
		case "KICK":
			st.kickSeen = true
			return true
		case "482": // not channel operator (e.g. classic ircd-irc2)
			st.haveOps = false
			return true
		}
		return false
	})

	// 4) Broader parse traffic after kick attempt.
	_ = primary.write("PRIVMSG " + ch1 + " :hello channel")
	_ = primary.write("NOTICE " + ch1 + " :channel notice")
	_ = primary.write("PRIVMSG " + botNick + " :hello pm")
	_ = primary.write("NOTICE " + botNick + " :private notice")
	_ = primary.write("MODE " + nick + " +i") // user mode; no channel ops needed
	_ = primary.write("MODE " + ch1)          // mode query
	if st.kickSeen {
		// Channel mode changes need ops (confirmed via successful KICK).
		_ = primary.write("MODE " + ch1 + " +n")
		_ = primary.write("MODE " + ch1 + " +b *!*@*")
	}
	if sm := isupport.Raw["STATUSMSG"]; sm != "" {
		_ = primary.write("PRIVMSG " + string(sm[0]) + ch1 + " :statusmsg probe")
		_ = primary.write("NOTICE " + string(sm[0]) + ch1 + " :statusmsg notice")
	}
	_ = primary.write("JOIN " + ch2 + "," + ch3 + " key1,key2")
	_ = primary.write("PART " + ch2 + " :leaving two")
	_ = primary.write("PART " + ch3)

	_ = primary.conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	for {
		raw, err := primary.r.ReadString('\n')
		if err != nil {
			break
		}
		line := strings.TrimRight(raw, "\r\n")
		msg, _ := irc.Parse(line)
		if msg.Command == "PING" {
			_ = primary.write("PONG :" + msg.Trailing())
		}
		if msg.Command == "KICK" {
			st.kickSeen = true
		}
		if err := record(line); err != nil {
			return st, err
		}
	}
	_ = primary.write("QUIT :done")
	_ = bot.write("QUIT :done")
	return st, nil
}

func registerClient(cl *ircClient, nick *string, user string, wantCaps bool, st *probeStats, record func(string) error) error {
	_ = cl.write("NICK " + *nick)
	_ = cl.write("USER " + user + " 0 * :" + user)
	if wantCaps {
		_ = cl.write("CAP LS 302")
	}
	capDone := !wantCaps
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		_ = cl.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		raw, err := cl.r.ReadString('\n')
		if err != nil {
			return err
		}
		line := strings.TrimRight(raw, "\r\n")
		if err := record(line); err != nil {
			return err
		}
		msg, _ := irc.Parse(line)
		switch msg.Command {
		case "PING":
			_ = cl.write("PONG :" + msg.Trailing())
		case "CAP":
			if len(msg.Params) < 2 {
				continue
			}
			switch strings.ToUpper(msg.Params[1]) {
			case "LS":
				var req []string
				for _, c := range strings.Fields(msg.Trailing()) {
					c = strings.Split(c, "=")[0]
					switch c {
					case "message-tags", "server-time", "account-tag", "batch", "multi-prefix":
						req = append(req, c)
					}
				}
				if len(req) > 0 {
					_ = cl.write("CAP REQ :" + strings.Join(req, " "))
				} else {
					_ = cl.write("CAP END")
					capDone = true
				}
			case "ACK", "NAK":
				if strings.Contains(msg.Trailing(), "message-tags") {
					st.messageTags = true
				}
				if strings.Contains(msg.Trailing(), "server-time") {
					st.serverTime = true
				}
				_ = cl.write("CAP END")
				capDone = true
			}
		case "001":
			if !capDone {
				_ = cl.write("CAP END")
			}
			return nil
		case "432", "433":
			*nick = *nick + "x"
			if len(*nick) > 9 {
				*nick = (*nick)[:9]
			}
			_ = cl.write("NICK " + *nick)
		case "ERROR":
			return fmt.Errorf("ERROR: %s", msg.Trailing())
		}
	}
	return fmt.Errorf("register timeout")
}

func waitMatch(cl *ircClient, timeout time.Duration, record func(string) error, match func(irc.Message) bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_ = cl.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		raw, err := cl.r.ReadString('\n')
		if err != nil {
			continue
		}
		line := strings.TrimRight(raw, "\r\n")
		if err := record(line); err != nil {
			return err
		}
		msg, _ := irc.Parse(line)
		if msg.Command == "PING" {
			_ = cl.write("PONG :" + msg.Trailing())
			continue
		}
		if match(msg) {
			return nil
		}
	}
	return fmt.Errorf("timeout")
}

func joinChan(msg irc.Message) string {
	if p := msg.Param(0); p != "" {
		return p
	}
	return msg.Trailing()
}
