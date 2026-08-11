// Package admin implements shared network/rehash management for the CLI and
// the in-band IRC BNC command.
package admin

import (
	"context"
	"fmt"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// Runtime applies live daemon actions after SQLite updates.
// For the CLI control-socket backend, ok=false means the daemon was not running
// (soft miss). In-process Server runtime always returns ok=true on success.
type Runtime interface {
	StartNetwork(name string) (ok bool, err error)
	StopNetwork(name string) (ok bool, err error)
	ReloadNetwork(name string) (ok bool, err error)
	ReconnectNetwork(name string) error
	Rehash() error
	// Status returns a live snapshot. ok=false means the daemon is not running.
	Status() (st Status, ok bool, err error)
}

// Deps are inputs for Run.
type Deps struct {
	Store      *store.Store
	ListenAddr string
	Nick       string
	Username   string
	Realname   string
	AltNick    string
	Runtime    Runtime
	// CurrentNetwork is the network of the IRC connection issuing BNC (empty for CLI).
	// When set, reconnect/disconnect may omit <name> to target this network.
	CurrentNetwork string
}

// Options control SASL password handling.
type Options struct {
	// AllowInlineSASLPass accepts --sasl-pass=secret (BNC path). When false (CLI),
	// --sasl-pass=value is rejected and PromptSASL is used instead.
	AllowInlineSASLPass bool
	// PromptSASL is called for bare --sasl-pass when AllowInlineSASLPass is false.
	PromptSASL func() (string, error)
}

// Help returns BNC-oriented help text (no serve/auth/stop).
func Help() string {
	return strings.TrimSpace(`BNC commands:
  help
  status
  rehash
  reconnect [<name>]
  disconnect [<name>]
  network add <name> <host> <port> [nick] [--nick=] [--tls=true] [--tls-noverify=true|false] [--tls-cert=] [--tls-key=] [--bind-host=] [--user=] [--realname=] [--sasl=true|false] [--sasl-user=] [--sasl-pass=] [--flood-burst=] [--flood-rate=] [--alt-nick=] [--nick-recovery=true|false]
  network mod <name> [--host=] [--port=] [--nick=] [--tls=true|false] [--tls-noverify=true|false] [--tls-cert=] [--tls-key=] [--bind-host=] [--user=] [--realname=] [--sasl=true|false] [--sasl-user=] [--sasl-pass=] [--flood-burst=] [--flood-rate=] [--alt-nick=] [--nick-recovery=true|false]
  network list
  network delete <name>
  network reconnect [<name>]
  network disconnect [<name>]
(<name> defaults to this connection's network when omitted)`)
}

// Run executes a management command and returns output lines.
// Accepted verbs: help, status, network, rehash, reconnect, disconnect.
// serve/auth/stop are rejected.
func Run(ctx context.Context, deps Deps, opts Options, args []string) ([]string, error) {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		return strings.Split(Help(), "\n"), nil
	}
	switch args[0] {
	case "status":
		return runStatus(ctx, deps)
	case "network":
		return runNetwork(ctx, deps, opts, args[1:])
	case "reconnect":
		return runReconnect(deps, args[1:], "reconnect [<name>]")
	case "disconnect":
		return runDisconnect(deps, args[1:], "disconnect [<name>]")
	case "rehash":
		if deps.Runtime == nil {
			return nil, fmt.Errorf("runtime not configured")
		}
		if err := deps.Runtime.Rehash(); err != nil {
			return nil, err
		}
		return []string{"Rehash complete."}, nil
	case "serve", "auth", "stop":
		return nil, fmt.Errorf("%s is not available via BNC; use the gobnc CLI", args[0])
	default:
		return nil, fmt.Errorf("unknown command %q", args[0])
	}
}

// resolveNetworkName picks an explicit name, else CurrentNetwork (IRC BNC context).
func resolveNetworkName(deps Deps, args []string, usage string) (string, error) {
	if len(args) >= 1 && args[0] != "" {
		return args[0], nil
	}
	if deps.CurrentNetwork != "" {
		return deps.CurrentNetwork, nil
	}
	return "", fmt.Errorf("usage: %s", usage)
}

func runReconnect(deps Deps, args []string, usage string) ([]string, error) {
	if deps.Runtime == nil {
		return nil, fmt.Errorf("runtime not configured")
	}
	name, err := resolveNetworkName(deps, args, usage)
	if err != nil {
		return nil, err
	}
	if err := deps.Runtime.ReconnectNetwork(name); err != nil {
		return nil, err
	}
	return []string{fmt.Sprintf("reconnect requested for %s", name)}, nil
}

func runDisconnect(deps Deps, args []string, usage string) ([]string, error) {
	if deps.Runtime == nil {
		return nil, fmt.Errorf("runtime not configured")
	}
	name, err := resolveNetworkName(deps, args, usage)
	if err != nil {
		return nil, err
	}
	ok, err := deps.Runtime.StopNetwork(name)
	if err != nil {
		return nil, err
	}
	if ok {
		return []string{fmt.Sprintf("disconnected %s", name)}, nil
	}
	return []string{fmt.Sprintf("disconnected %s (daemon not running)", name)}, nil
}

func runStatus(ctx context.Context, deps Deps) ([]string, error) {
	if deps.Runtime != nil {
		st, ok, err := deps.Runtime.Status()
		if err != nil {
			return nil, err
		}
		if ok {
			return FormatStatus(st), nil
		}
	}
	// Offline / daemon down: config listen addr + DB networks.
	st := Status{Running: false, ListenAddr: deps.ListenAddr}
	if deps.Store != nil {
		nets, err := deps.Store.ListNetworks(ctx)
		if err != nil {
			return nil, err
		}
		for _, n := range nets {
			st.Networks = append(st.Networks, NetworkStatus{
				Name: n.Name, Host: n.Host, Port: n.Port, TLS: n.TLS,
				Enabled: n.Enabled, ConfigNick: n.Nick, Nick: n.Nick,
			})
		}
	}
	return FormatStatus(st), nil
}

func runNetwork(ctx context.Context, deps Deps, opts Options, args []string) ([]string, error) {
	if deps.Store == nil {
		return nil, fmt.Errorf("store not configured")
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("network subcommand required")
	}
	switch args[0] {
	case "list":
		nets, err := deps.Store.ListNetworks(ctx)
		if err != nil {
			return nil, err
		}
		lines := make([]string, 0, len(nets))
		for _, n := range nets {
			lines = append(lines, fmt.Sprintf("%s\t%s:%d\ttls=%v\ttls_noverify=%v\ttls_cert=%s\tbind_host=%s\tnick=%s\talt_nick=%s\tnick_recovery=%v\tflood_burst=%d\tflood_rate=%g",
				n.Name, n.Host, n.Port, n.TLS, n.TLSNoVerify, n.TLSCert, n.BindHost, n.Nick, n.AltNick, n.NickRecovery, n.FloodBurst, n.FloodRate))
		}
		return lines, nil
	case "reconnect":
		return runReconnect(deps, args[1:], "network reconnect <name>")
	case "disconnect":
		return runDisconnect(deps, args[1:], "network disconnect <name>")
	case "delete":
		if len(args) < 2 {
			return nil, fmt.Errorf("usage: network delete <name>")
		}
		if deps.Runtime == nil {
			return nil, fmt.Errorf("runtime not configured")
		}
		name := args[1]
		ok, err := deps.Runtime.StopNetwork(name)
		if err != nil {
			return nil, fmt.Errorf("stop network in daemon: %w", err)
		}
		if err := deps.Store.DeleteNetwork(ctx, name); err != nil {
			return nil, err
		}
		if ok {
			return []string{"deleted and stopped in running daemon"}, nil
		}
		return []string{"deleted (daemon not running; will apply on next start)"}, nil
	case "add":
		return networkAdd(ctx, deps, opts, args)
	case "mod":
		return networkMod(ctx, deps, opts, args)
	default:
		return nil, fmt.Errorf("unknown network command")
	}
}

func networkAdd(ctx context.Context, deps Deps, opts Options, args []string) ([]string, error) {
	if len(args) < 4 {
		return nil, fmt.Errorf("usage: network add <name> <host> <port> [nick] [--nick=] [--tls=true] [--tls-noverify=true|false] [--tls-cert=] [--tls-key=] [--bind-host=] [--user=] [--realname=] [--sasl=true|false] [--sasl-user=] [--sasl-pass] [--flood-burst=] [--flood-rate=] [--alt-nick=] [--nick-recovery=true|false]")
	}
	if deps.Runtime == nil {
		return nil, fmt.Errorf("runtime not configured")
	}
	n := store.Network{
		Name: args[1], Host: args[2], Nick: deps.Nick, TLS: true, Enabled: true,
		Username: deps.Username, Realname: deps.Realname, AltNick: deps.AltNick, NickRecovery: true,
	}
	fmt.Sscanf(args[3], "%d", &n.Port)
	i := 4
	if len(args) > 4 && !strings.HasPrefix(args[4], "-") {
		n.Nick = args[4]
		i = 5
	}
	wantSASLPass := false
	saslFlagSet := false
	for ; i < len(args); i++ {
		a := args[i]
		if a == "-config" {
			i++ // skip path
			continue
		}
		if strings.HasPrefix(a, "-config=") {
			continue
		}
		switch {
		case strings.HasPrefix(a, "--nick="):
			n.Nick = strings.TrimPrefix(a, "--nick=")
		case strings.HasPrefix(a, "--tls="):
			n.TLS = a != "--tls=false"
		case strings.HasPrefix(a, "--tls-noverify="):
			n.TLSNoVerify = strings.TrimPrefix(a, "--tls-noverify=") != "false"
		case strings.HasPrefix(a, "--tls-cert="):
			n.TLSCert = strings.TrimPrefix(a, "--tls-cert=")
		case strings.HasPrefix(a, "--tls-key="):
			n.TLSKey = strings.TrimPrefix(a, "--tls-key=")
		case strings.HasPrefix(a, "--bind-host="):
			n.BindHost = strings.TrimPrefix(a, "--bind-host=")
		case strings.HasPrefix(a, "--user="):
			n.Username = strings.TrimPrefix(a, "--user=")
		case strings.HasPrefix(a, "--username="):
			n.Username = strings.TrimPrefix(a, "--username=")
		case strings.HasPrefix(a, "--realname="):
			n.Realname = strings.TrimPrefix(a, "--realname=")
		case strings.HasPrefix(a, "--sasl="):
			n.SASL = strings.TrimPrefix(a, "--sasl=") != "false"
			saslFlagSet = true
		case strings.HasPrefix(a, "--sasl-user="):
			n.SASLUser = strings.TrimPrefix(a, "--sasl-user=")
		case a == "--sasl-pass":
			wantSASLPass = true
		case strings.HasPrefix(a, "--sasl-pass="):
			pass, err := parseInlineSASLPass(a, opts)
			if err != nil {
				return nil, err
			}
			n.SASLPass = pass
		case strings.HasPrefix(a, "--flood-burst="):
			fmt.Sscanf(strings.TrimPrefix(a, "--flood-burst="), "%d", &n.FloodBurst)
		case strings.HasPrefix(a, "--flood-rate="):
			fmt.Sscanf(strings.TrimPrefix(a, "--flood-rate="), "%f", &n.FloodRate)
		case strings.HasPrefix(a, "--alt-nick="):
			n.AltNick = strings.TrimPrefix(a, "--alt-nick=")
		case strings.HasPrefix(a, "--nick-recovery="):
			n.NickRecovery = strings.TrimPrefix(a, "--nick-recovery=") != "false"
		default:
			return nil, fmt.Errorf("unknown flag %q", a)
		}
	}
	if n.Nick == "" {
		return nil, fmt.Errorf("nick required: pass [nick] / --nick=, or set default_nick in gobnc.json")
	}
	pass, err := resolveSASLPass(opts, wantSASLPass)
	if err != nil {
		return nil, err
	}
	if pass != "" {
		n.SASLPass = pass
	}
	// User alone (EXTERNAL authzid) or user+pass implies SASL on.
	if !saslFlagSet && n.SASLUser != "" {
		n.SASL = true
	}
	if _, err := deps.Store.UpsertNetwork(ctx, n); err != nil {
		return nil, err
	}
	ok, err := deps.Runtime.StartNetwork(n.Name)
	if err != nil {
		return nil, fmt.Errorf("saved to db but failed to start in daemon: %w", err)
	}
	if ok {
		return []string{"added and started in running daemon"}, nil
	}
	return []string{"added (daemon not running; will apply on next start)"}, nil
}

func networkMod(ctx context.Context, deps Deps, opts Options, args []string) ([]string, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("usage: network mod <name> [--host=] [--port=] [--nick=] [--tls=true|false] [--tls-noverify=true|false] [--tls-cert=] [--tls-key=] [--bind-host=] [--user=] [--realname=] [--sasl=true|false] [--sasl-user=] [--sasl-pass] [--flood-burst=] [--flood-rate=] [--alt-nick=] [--nick-recovery=true|false]")
	}
	if deps.Runtime == nil {
		return nil, fmt.Errorf("runtime not configured")
	}
	n, err := deps.Store.NetworkByName(ctx, args[1])
	if err != nil {
		return nil, fmt.Errorf("network %q: %w", args[1], err)
	}
	changed := false
	wantSASLPass := false
	for i := 2; i < len(args); i++ {
		a := args[i]
		if a == "-config" {
			i++ // skip path
			continue
		}
		if strings.HasPrefix(a, "-config=") {
			continue
		}
		switch {
		case strings.HasPrefix(a, "--host="):
			n.Host = strings.TrimPrefix(a, "--host=")
			changed = true
		case strings.HasPrefix(a, "--port="):
			fmt.Sscanf(strings.TrimPrefix(a, "--port="), "%d", &n.Port)
			changed = true
		case strings.HasPrefix(a, "--nick="):
			n.Nick = strings.TrimPrefix(a, "--nick=")
			changed = true
		case strings.HasPrefix(a, "--tls="):
			n.TLS = strings.TrimPrefix(a, "--tls=") != "false"
			changed = true
		case strings.HasPrefix(a, "--tls-noverify="):
			n.TLSNoVerify = strings.TrimPrefix(a, "--tls-noverify=") != "false"
			changed = true
		case strings.HasPrefix(a, "--tls-cert="):
			n.TLSCert = strings.TrimPrefix(a, "--tls-cert=")
			changed = true
		case strings.HasPrefix(a, "--tls-key="):
			n.TLSKey = strings.TrimPrefix(a, "--tls-key=")
			changed = true
		case strings.HasPrefix(a, "--bind-host="):
			n.BindHost = strings.TrimPrefix(a, "--bind-host=")
			changed = true
		case strings.HasPrefix(a, "--sasl="):
			n.SASL = strings.TrimPrefix(a, "--sasl=") != "false"
			changed = true
		case strings.HasPrefix(a, "--sasl-user="):
			n.SASLUser = strings.TrimPrefix(a, "--sasl-user=")
			changed = true
		case a == "--sasl-pass":
			wantSASLPass = true
			changed = true
		case strings.HasPrefix(a, "--sasl-pass="):
			pass, err := parseInlineSASLPass(a, opts)
			if err != nil {
				return nil, err
			}
			n.SASLPass = pass
			changed = true
		case strings.HasPrefix(a, "--user="):
			n.Username = strings.TrimPrefix(a, "--user=")
			changed = true
		case strings.HasPrefix(a, "--username="):
			n.Username = strings.TrimPrefix(a, "--username=")
			changed = true
		case strings.HasPrefix(a, "--realname="):
			n.Realname = strings.TrimPrefix(a, "--realname=")
			changed = true
		case strings.HasPrefix(a, "--flood-burst="):
			fmt.Sscanf(strings.TrimPrefix(a, "--flood-burst="), "%d", &n.FloodBurst)
			changed = true
		case strings.HasPrefix(a, "--flood-rate="):
			fmt.Sscanf(strings.TrimPrefix(a, "--flood-rate="), "%f", &n.FloodRate)
			changed = true
		case strings.HasPrefix(a, "--alt-nick="):
			n.AltNick = strings.TrimPrefix(a, "--alt-nick=")
			changed = true
		case strings.HasPrefix(a, "--nick-recovery="):
			n.NickRecovery = strings.TrimPrefix(a, "--nick-recovery=") != "false"
			changed = true
		default:
			return nil, fmt.Errorf("unknown flag %q", a)
		}
	}
	pass, err := resolveSASLPass(opts, wantSASLPass)
	if err != nil {
		return nil, err
	}
	if pass != "" {
		n.SASLPass = pass
		changed = true
	}
	if !changed {
		return nil, fmt.Errorf("network mod: no changes; pass at least one --host/--port/--nick/--tls=/--tls-noverify=/--tls-cert=/--tls-key=/--bind-host=/--sasl=/--user=/--realname=/--sasl-*/--flood-*/--alt-nick=/--nick-recovery=")
	}
	if _, err := deps.Store.UpsertNetwork(ctx, n); err != nil {
		return nil, err
	}
	ok, err := deps.Runtime.ReloadNetwork(n.Name)
	if err != nil {
		return nil, fmt.Errorf("saved to db but failed to reload in daemon: %w", err)
	}
	if ok {
		return []string{fmt.Sprintf("updated %s (flood pacing applies now; host/TLS/SASL/cert/bind_host on next uplink reconnect)", n.Name)}, nil
	}
	return []string{fmt.Sprintf("updated %s (daemon not running; will apply on next start)", n.Name)}, nil
}

func parseInlineSASLPass(flag string, opts Options) (string, error) {
	if !opts.AllowInlineSASLPass {
		return "", fmt.Errorf("--sasl-pass=value is not allowed; use --sasl-pass to prompt")
	}
	return strings.TrimPrefix(flag, "--sasl-pass="), nil
}

func resolveSASLPass(opts Options, wantPrompt bool) (string, error) {
	if !wantPrompt {
		return "", nil
	}
	if opts.AllowInlineSASLPass {
		return "", fmt.Errorf("SASL password required: use --sasl-pass=secret")
	}
	if opts.PromptSASL == nil {
		return "", fmt.Errorf("SASL password required")
	}
	return opts.PromptSASL()
}
