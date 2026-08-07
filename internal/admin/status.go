package admin

import (
	"fmt"
	"sort"
	"strings"
)

// Status is a daemon/network snapshot for the status command.
type Status struct {
	// Running is false when the daemon is not reachable (CLI offline view).
	Running    bool             `json:"running"`
	ListenAddr string           `json:"listen_addr"`
	Clients    int              `json:"clients"`
	Networks   []NetworkStatus  `json:"networks"`
}

// NetworkStatus is one configured network and its live uplink state (if any).
type NetworkStatus struct {
	Name       string `json:"name"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	TLS        bool   `json:"tls"`
	Enabled    bool   `json:"enabled"`
	ConfigNick string `json:"config_nick"`
	Running    bool   `json:"running"`    // session/uplink process present
	Registered bool   `json:"registered"` // uplink welcome received
	Nick       string `json:"nick"`
	Clients    int    `json:"clients"`
}

// FormatStatus renders Status as human-readable lines.
func FormatStatus(st Status) []string {
	var lines []string
	if !st.Running {
		lines = append(lines, "daemon not running")
	} else {
		lines = append(lines, "daemon running")
	}
	if st.ListenAddr != "" {
		lines = append(lines, "listen "+st.ListenAddr)
	}
	lines = append(lines, fmt.Sprintf("clients %d", st.Clients))
	nets := append([]NetworkStatus(nil), st.Networks...)
	sort.Slice(nets, func(i, j int) bool {
		return strings.ToLower(nets[i].Name) < strings.ToLower(nets[j].Name)
	})
	for _, n := range nets {
		state := "stopped"
		switch {
		case !n.Enabled:
			state = "disabled"
		case n.Registered:
			state = "connected"
		case n.Running:
			state = "connecting"
		}
		nick := n.Nick
		if nick == "" {
			nick = n.ConfigNick
		}
		lines = append(lines, fmt.Sprintf("network %s %s nick=%s host=%s:%d tls=%v clients=%d",
			n.Name, state, nick, n.Host, n.Port, n.TLS, n.Clients))
	}
	return lines
}
