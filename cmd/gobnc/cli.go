package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/control"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func runCLI(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Println(`gobnc commands:
  serve [-config gobnc.json]
  auth set-password <password>
  auth add-fingerprint <sha256-hex> [label]
  auth list-fingerprints
  network add <name> <host> <port> <nick> [--tls=true] [--user=] [--realname=] [--sasl-user=] [--sasl-pass=]
  network mod <name> [--host=] [--port=] [--nick=] [--tls=true|false] [--user=] [--realname=] [--sasl-user=] [--sasl-pass=]
  network list
  network delete <name>

When the daemon is running, network add/delete also notify it via the control socket
(control_socket in gobnc.json, default gobnc.sock) so uplinks start/stop immediately.
network mod updates SQLite and refreshes the running session config; the current
uplink stays up and new host/port/TLS/SASL apply on the next reconnect.`)
		return nil
	}
	cfgPath := "gobnc.json"
	for i, a := range args {
		if a == "-config" && i+1 < len(args) {
			cfgPath = args[i+1]
		}
	}
	cfg, _ := config.LoadJSON(cfgPath)
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	ctx := context.Background()

	switch args[0] {
	case "auth":
		return cmdAuth(ctx, st, args[1:])
	case "network":
		return cmdNetwork(ctx, st, cfg, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func cmdAuth(ctx context.Context, st *store.Store, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("auth subcommand required")
	}
	switch args[0] {
	case "set-password":
		if len(args) < 2 {
			return fmt.Errorf("usage: auth set-password <password>")
		}
		h, err := auth.HashPassword(args[1])
		if err != nil {
			return err
		}
		return st.SetPasswordHash(ctx, h)
	case "add-fingerprint":
		if len(args) < 2 {
			return fmt.Errorf("usage: auth add-fingerprint <sha256-hex> [label]")
		}
		label := ""
		if len(args) > 2 {
			label = args[2]
		}
		return st.AddFingerprint(ctx, strings.ToLower(args[1]), label)
	case "list-fingerprints":
		fps, err := st.ListFingerprints(ctx)
		if err != nil {
			return err
		}
		for _, fp := range fps {
			fmt.Println(fp)
		}
		return nil
	default:
		return fmt.Errorf("unknown auth command")
	}
}

func cmdNetwork(ctx context.Context, st *store.Store, cfg config.Config, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("network subcommand required")
	}
	switch args[0] {
	case "list":
		nets, err := st.ListNetworks(ctx)
		if err != nil {
			return err
		}
		for _, n := range nets {
			fmt.Printf("%s\t%s:%d\ttls=%v\tnick=%s\n", n.Name, n.Host, n.Port, n.TLS, n.Nick)
		}
		return nil
	case "delete":
		if len(args) < 2 {
			return fmt.Errorf("usage: network delete <name>")
		}
		name := args[1]
		notified, err := control.TryNotify(cfg.ControlSocket, control.CmdStopNetwork+" "+name)
		if err != nil {
			return fmt.Errorf("stop network in daemon: %w", err)
		}
		if err := st.DeleteNetwork(ctx, name); err != nil {
			return err
		}
		if notified {
			fmt.Println("deleted and stopped in running daemon")
		} else {
			fmt.Println("deleted (daemon not running; will apply on next start)")
		}
		return nil
	case "add":
		if len(args) < 5 {
			return fmt.Errorf("usage: network add <name> <host> <port> <nick> [--tls=true] [--user=] [--realname=] [--sasl-user=] [--sasl-pass=]")
		}
		n := store.Network{
			Name: args[1], Host: args[2], Nick: args[4], TLS: true, Enabled: true, Username: "gobnc", Realname: "GoBNC",
		}
		fmt.Sscanf(args[3], "%d", &n.Port)
		for i := 5; i < len(args); i++ {
			a := args[i]
			if a == "-config" {
				i++ // skip path
				continue
			}
			if strings.HasPrefix(a, "-config=") {
				continue
			}
			switch {
			case strings.HasPrefix(a, "--tls="):
				n.TLS = a != "--tls=false"
			case strings.HasPrefix(a, "--user="):
				n.Username = strings.TrimPrefix(a, "--user=")
			case strings.HasPrefix(a, "--username="):
				n.Username = strings.TrimPrefix(a, "--username=")
			case strings.HasPrefix(a, "--realname="):
				n.Realname = strings.TrimPrefix(a, "--realname=")
			case strings.HasPrefix(a, "--sasl-user="):
				n.SASLUser = strings.TrimPrefix(a, "--sasl-user=")
			case strings.HasPrefix(a, "--sasl-pass="):
				n.SASLPass = strings.TrimPrefix(a, "--sasl-pass=")
			default:
				return fmt.Errorf("unknown flag %q", a)
			}
		}
		if _, err := st.UpsertNetwork(ctx, n); err != nil {
			return err
		}
		notified, err := control.TryNotify(cfg.ControlSocket, control.CmdStartNetwork+" "+n.Name)
		if err != nil {
			return fmt.Errorf("saved to db but failed to start in daemon: %w", err)
		}
		if notified {
			fmt.Println("added and started in running daemon")
		} else {
			fmt.Println("added (daemon not running; will apply on next start)")
		}
		return nil
	case "mod":
		if len(args) < 2 {
			return fmt.Errorf("usage: network mod <name> [--host=] [--port=] [--nick=] [--tls=true|false] [--user=] [--realname=] [--sasl-user=] [--sasl-pass=]")
		}
		n, err := st.NetworkByName(ctx, args[1])
		if err != nil {
			return fmt.Errorf("network %q: %w", args[1], err)
		}
		changed := false
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
			case strings.HasPrefix(a, "--sasl-user="):
				n.SASLUser = strings.TrimPrefix(a, "--sasl-user=")
				changed = true
			case strings.HasPrefix(a, "--sasl-pass="):
				n.SASLPass = strings.TrimPrefix(a, "--sasl-pass=")
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
			default:
				return fmt.Errorf("unknown flag %q", a)
			}
		}
		if !changed {
			return fmt.Errorf("network mod: no changes; pass at least one --host/--port/--nick/--tls=/--user=/--realname=/--sasl-*")
		}
		if _, err := st.UpsertNetwork(ctx, n); err != nil {
			return err
		}
		notified, err := control.TryNotify(cfg.ControlSocket, control.CmdReloadNetwork+" "+n.Name)
		if err != nil {
			return fmt.Errorf("saved to db but failed to reload in daemon: %w", err)
		}
		if notified {
			fmt.Printf("updated %s (applies on next uplink reconnect)\n", n.Name)
		} else {
			fmt.Printf("updated %s (daemon not running; will apply on next start)\n", n.Name)
		}
		return nil
	default:
		return fmt.Errorf("unknown network command")
	}
}

var _ = os.Stderr
