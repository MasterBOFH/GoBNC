//go:build ircd

package ircd_test

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// TestRouteRepliesNumericProbe exercises solicitous-style commands (and intentional
// error cases) against the docker ircd matrix. It logs which numerics actually arrive
// so we can confirm ZNC-style error ends before wiring them into RequestTracker.
func TestRouteRepliesNumericProbe(t *testing.T) {
	if os.Getenv("GOBNC_IRCD") == "0" {
		t.Skip("GOBNC_IRCD=0")
	}
	base := fmt.Sprintf("r%d", time.Now().Unix()%100000)
	for _, srv := range servers {
		srv := srv
		t.Run(srv.Name, func(t *testing.T) {
			t.Parallel()
			nick := sanitizeNick(base + srv.Name)
			got, err := probeRouteReplies(srv.Addr, nick)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range sortedKeys(countKeys(got)) {
				t.Logf("%s: %v", name, sortedKeys(got[name]))
			}
		})
	}
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func countKeys(m map[string]map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k := range m {
		out[k] = 1
	}
	return out
}

func probeRouteReplies(addr, nick string) (map[string]map[string]int, error) {
	out := map[string]map[string]int{}
	note := func(probe, cmd string) {
		if out[probe] == nil {
			out[probe] = map[string]int{}
		}
		out[probe][cmd]++
	}

	cl, err := dialClient(addr)
	if err != nil {
		return out, err
	}
	defer cl.conn.Close()

	st := &probeStats{seen: map[string]int{}}
	record := func(line string) error {
		msg, err := irc.Parse(line)
		if err != nil {
			return err
		}
		st.seen[msg.Command]++
		if msg.Command == "PING" {
			_ = cl.write("PONG :" + msg.Trailing())
		}
		return nil
	}
	nickPtr := &nick
	if err := registerClient(cl, nickPtr, "route", false, st, record); err != nil {
		return out, fmt.Errorf("register: %w", err)
	}
	nick = *nickPtr
	ch := "#" + sanitizeNick("rp"+nick)
	_ = cl.write("JOIN " + ch)
	_ = drainUntil(cl, 8*time.Second, record, func(msg irc.Message) bool {
		return msg.Command == "JOIN" && strings.EqualFold(joinChan(msg), ch)
	})

	// Collect replies for a short window after each probe command.
	run := func(probe string, send string, window time.Duration) {
		before := snapshotSeen(st.seen)
		_ = cl.write(send)
		_ = drainFor(cl, window, record)
		after := st.seen
		for cmd, n := range after {
			if n > before[cmd] {
				note(probe, cmd)
			}
		}
	}

	run("WHO_ok", "WHO "+ch, 3*time.Second)
	run("WHO_nosuch", "WHO #gobnc_nosuch_chan_xyz", 3*time.Second)
	run("LIST", "LIST", 4*time.Second)
	run("NAMES_ok", "NAMES "+ch, 3*time.Second)
	run("NAMES_nosuch", "NAMES #gobnc_nosuch_chan_xyz", 3*time.Second)
	run("LINKS", "LINKS", 3*time.Second)
	run("ISON_ok", "ISON "+nick, 2*time.Second)
	run("ISON_empty", "ISON", 2*time.Second)
	run("USERHOST", "USERHOST "+nick, 2*time.Second)
	run("USERHOST_empty", "USERHOST", 2*time.Second)
	run("WHOIS", "WHOIS "+nick, 4*time.Second)
	run("WHOWAS", "WHOWAS gobnc_never_was_nick_zzz", 4*time.Second)
	run("LUSERS", "LUSERS", 3*time.Second)
	run("TIME", "TIME", 2*time.Second)
	run("TRACE", "TRACE", 3*time.Second)
	run("USERS", "USERS", 3*time.Second)
	run("MAP", "MAP", 3*time.Second)
	run("MODE_chan", "MODE "+ch, 2*time.Second)
	run("MODE_b", "MODE "+ch+" b", 2*time.Second)
	run("MODE_umode", "MODE "+nick, 2*time.Second)
	// MODE-change errors (not enquiry); see if servers emit 467/501/502.
	run("MODE_badumode", "MODE "+nick+" +ZZZ", 2*time.Second)
	run("MODE_other", "MODE someoneelse +i", 2*time.Second)
	run("TOPIC_get", "TOPIC "+ch, 2*time.Second)
	run("TOPIC_nosuch", "TOPIC #gobnc_nosuch_chan_xyz", 2*time.Second)

	_ = cl.write("QUIT :done")
	return out, nil
}

func snapshotSeen(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func drainFor(cl *ircClient, d time.Duration, record func(string) error) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		_ = cl.conn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
		raw, err := cl.r.ReadString('\n')
		if err != nil {
			continue
		}
		line := strings.TrimRight(raw, "\r\n")
		msg, perr := irc.Parse(line)
		if perr != nil {
			return perr
		}
		if msg.Command == "PING" {
			_ = cl.write("PONG :" + msg.Trailing())
		}
		if err := record(line); err != nil {
			return err
		}
	}
	return nil
}

func drainUntil(cl *ircClient, d time.Duration, record func(string) error, match func(irc.Message) bool) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		_ = cl.conn.SetReadDeadline(time.Now().Add(time.Second))
		raw, err := cl.r.ReadString('\n')
		if err != nil {
			continue
		}
		line := strings.TrimRight(raw, "\r\n")
		msg, perr := irc.Parse(line)
		if perr != nil {
			return perr
		}
		if msg.Command == "PING" {
			_ = cl.write("PONG :" + msg.Trailing())
		}
		if err := record(line); err != nil {
			return err
		}
		if match(msg) {
			return nil
		}
	}
	return fmt.Errorf("timeout waiting for match")
}
