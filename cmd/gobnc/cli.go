package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/admin"
	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/control"
	"github.com/MasterBOFH/GoBNC/internal/daemon"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/version"
	"golang.org/x/term"
)

// errMustUpgrade is the can-upgrade verdict that the running keeper is
// incompatible with this binary. main maps it to exit status 2.
var errMustUpgrade = errors.New("keeper must be upgraded")

func runCLI(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Println(`gobnc commands:
  serve [-config gobnc.json] [-foreground|-f] [-debug|-d]
  stop [-config gobnc.json]
  die [-config gobnc.json]
  status [-config gobnc.json]
  can-upgrade [-config gobnc.json]
  rehash [-config gobnc.json]
  reload [-config gobnc.json]
  auth set-password
  auth add-fingerprint <sha256-hex> [label]
  auth list-fingerprints
  auth delete-fingerprint <#N|sha256-hex|prefix>
  network add <name> <host> <port> [nick] [--nick=] [--tls=true] [--tls-noverify=true|false] [--tls-cert=] [--tls-key=] [--bind-host=] [--user=] [--realname=] [--sasl=true|false] [--sasl-user=] [--sasl-pass] [--flood-burst=] [--flood-rate=] [--alt-nick=] [--nick-recovery=true|false]
  network mod <name> [--host=] [--port=] [--nick=] [--tls=true|false] [--tls-noverify=true|false] [--tls-cert=] [--tls-key=] [--bind-host=] [--user=] [--realname=] [--sasl=true|false] [--sasl-user=] [--sasl-pass] [--flood-burst=] [--flood-rate=] [--alt-nick=] [--nick-recovery=true|false]
  network list
  network delete <name>
  network reconnect <name>
  network disconnect <name>

serve backgrounds by default (re-exec + pid file). Use -debug/-d or -foreground/-f
to stay attached (required under systemd/rc.d). -debug also forces a debug console
(file still uses gobnc.json log_level). Daemon mode defaults log_file to the state
dir when unset.

Secrets (bouncer password, SASL password) are prompted on a TTY; they are not accepted on the command line.
auth set-password asks whether to generate a random password (default yes); otherwise you enter one.

When the daemon is running, network add/delete also notify it via the control socket
(control_socket in gobnc.json; default under $XDG_RUNTIME_DIR/gobnc or ~/.gobnc)
so uplinks start/stop immediately.
status shows listen address, attached clients, per-network uplink state
(connected/connecting/stopped), and brain/keeper generations with keeper-upgrade
advice (none / should / must).
can-upgrade probes the running keeper over IPC (validate attach — does not
replace the live brain) and reports whether this binary's keeper is current,
should be replaced (gobnc die then start; drops uplinks), or must be replaced
that way before this brain can attach. Exit 0 for none/should, 2 for must.
rehash reloads gobnc.json and refreshes networks (same as SIGHUP), including
log_level, log_file, and listen_addr (existing TLS clients stay connected when
the listen address changes). Restart still required for db_path and control_socket.
reload restarts the brain process from the on-disk binary; the keeper holds
every uplink. Use this after installing a new gobnc (run can-upgrade first).
Also available as BNC reload.
stop asks the daemon (brain) to shut down (control socket, else SIGTERM via pid
file). The keeper keeps running and uplinks stay up.
die stops the brain and the keeper (QUIT to every IRC server). Also BNC die.
network mod updates SQLite and refreshes the running session config; the current
uplink stays up and new host/port/TLS/SASL/cert/bind_host apply on the next reconnect.
network reconnect reloads DB settings and drops the uplink so it dials again now
(downlinks stay attached). If the network is stopped, reconnect starts it.
network disconnect stops the uplink without deleting the network (use reconnect to bring it back).
Flood pacing (--flood-burst bytes, --flood-rate bytes/sec) applies immediately; 0 disables.
--alt-nick= sets a fallback nick when the primary is taken; --nick-recovery= (default true)
enables the nick ladder and ISON reclaim of the primary/alt nick.
--tls-cert= / --tls-key= set a per-network cert GoBNC presents to the IRC server
(empty inherits tls_client_cert/key from gobnc.json; none or - disables).
--bind-host= sets the local address for uplink dials (empty inherits bind_host from
gobnc.json; none or - uses the OS default).
--sasl=true enables bouncer SASL (SCRAM/PLAIN with --sasl-user/--sasl-pass; EXTERNAL
when sasl is on, password is empty, and a client cert is present — optional
--sasl-user= is sent as the EXTERNAL authorization identity). A client cert alone
does not enable SASL. network add with --sasl-user= implies --sasl=true.
network add uses default_nick / default_username / default_realname / default_alt_nick
from gobnc.json when those fields are omitted.
Pass --sasl-pass (no value) to prompt for a SASL password.`)
		return nil
	}
	cfgPath := "gobnc.json"
	for i, a := range args {
		if a == "-config" && i+1 < len(args) {
			cfgPath = args[i+1]
		}
	}
	cfg, _ := config.LoadJSON(cfgPath)

	switch args[0] {
	case "rehash":
		return printAdmin(admin.Run(context.Background(), adminDeps(nil, cfg), admin.Options{}, []string{"rehash"}))
	case "reload":
		return cmdReload(cfg, cfgPath)
	case "status":
		st, err := store.Open(cfg.DBPath)
		if err != nil {
			return err
		}
		defer st.Close()
		return printAdmin(admin.Run(context.Background(), adminDeps(st, cfg), admin.Options{}, []string{"status"}))
	case "stop":
		return cmdStop(cfg)
	case "die":
		return cmdDie(cfg)
	case "can-upgrade":
		return cmdCanUpgrade()
	}

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
		opts := admin.Options{
			PromptSASL: func() (string, error) { return promptSecret("SASL password: ") },
		}
		return printAdmin(admin.Run(ctx, adminDeps(st, cfg), opts, args))
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func adminDeps(st *store.Store, cfg config.Config) admin.Deps {
	nick, user, real, alt := cfg.NetworkIdentityDefaults()
	return admin.Deps{
		Store:      st,
		ListenAddr: cfg.ListenAddr,
		Nick:       nick,
		Username:   user,
		Realname:   real,
		AltNick:    alt,
		Runtime:    admin.ControlRuntime{Socket: cfg.ResolvedControlSocket()},
	}
}

func printAdmin(lines []string, err error) error {
	if err != nil {
		return err
	}
	for _, line := range lines {
		fmt.Println(line)
	}
	return nil
}

func cmdStop(cfg config.Config) error {
	notified, err := control.TryNotify(cfg.ResolvedControlSocket(), control.CmdShutdown)
	if err != nil {
		return err
	}
	if notified {
		fmt.Fprintln(os.Stderr, "Shutdown requested.")
		return nil
	}
	if err := daemon.Stop(cfg.ResolvedPidFile(), 8*time.Second); err != nil {
		return fmt.Errorf("daemon not running via control socket; pid stop: %w", err)
	}
	fmt.Fprintln(os.Stderr, "Stopped via pid file.")
	return nil
}

func cmdReload(cfg config.Config, cfgPath string) error {
	rt := admin.ControlRuntime{Socket: cfg.ResolvedControlSocket()}
	if err := rt.Reload(); err == nil {
		fmt.Fprintln(os.Stderr, "Reload requested.")
		return nil
	} else if err != nil && strings.Contains(err.Error(), "must be upgraded") {
		return err
	}
	// Old brain (no RELOAD command) or not running: stop the brain if it
	// is up, then spawn this binary. Keeper is left alone.
	_, _ = control.TryNotify(cfg.ResolvedControlSocket(), control.CmdShutdown)
	if err := daemon.Stop(cfg.ResolvedPidFile(), 8*time.Second); err != nil && !os.IsNotExist(err) && !strings.Contains(err.Error(), "not running") {
		// Still try to spawn; a missing pidfile is the not-running case.
		if !strings.Contains(err.Error(), "no such file") {
			fmt.Fprintf(os.Stderr, "brain stop: %v\n", err)
		}
	}
	pid, err := daemon.SpawnReplacement(cfgPath, cfg.ResolvedPidFile(), false, false)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Started brain pid %d (keeper unchanged).\n", pid)
	return nil
}

func keeperPidFile() string {
	return filepath.Join(config.DefaultStateDir(), "keeper.pid")
}

func stopKeeperProcess() error {
	err := daemon.Stop(keeperPidFile(), 10*time.Second)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) || strings.Contains(err.Error(), "not running") || strings.Contains(err.Error(), "empty pid") || strings.Contains(err.Error(), "invalid pid") {
		return nil
	}
	return err
}

func cmdDie(cfg config.Config) error {
	notified, err := control.TryNotify(cfg.ResolvedControlSocket(), control.CmdDie)
	if err != nil && strings.Contains(err.Error(), "unknown command") {
		notified, err = false, nil
	}
	if err != nil {
		return err
	}
	if notified {
		_ = daemon.Stop(cfg.ResolvedPidFile(), 8*time.Second)
	} else {
		_, _ = control.TryNotify(cfg.ResolvedControlSocket(), control.CmdShutdown)
		if err := daemon.Stop(cfg.ResolvedPidFile(), 8*time.Second); err != nil && !os.IsNotExist(err) && !strings.Contains(err.Error(), "not running") && !strings.Contains(err.Error(), "no such file") {
			fmt.Fprintf(os.Stderr, "brain stop: %v\n", err)
		}
	}
	if err := stopKeeperProcess(); err != nil {
		return fmt.Errorf("keeper: %w", err)
	}
	fmt.Fprintln(os.Stderr, "Brain and keeper stopped.")
	return nil
}

func cmdCanUpgrade() error {
	sock := filepath.Join(config.DefaultStateDir(), "keeper.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := keeper.Probe(ctx, sock)
	if err != nil {
		return fmt.Errorf("keeper not reachable at %s: %w", sock, err)
	}
	u := version.CanUpgrade(res.KeeperVersion)
	running := version.NormalizeKeeperVersion(res.KeeperVersion)
	fmt.Printf("brain %d\n", version.BrainVersion)
	fmt.Printf("keeper %d\n", running)
	fmt.Printf("keeper-upgrade %s\n", u.String())
	switch u {
	case version.UpgradeShould:
		fmt.Printf("running keeper is older than this binary (keeper %d) but compatible; gobnc die then start to spawn a new keeper (drops uplinks)\n", version.KeeperVersion)
	case version.UpgradeMust:
		fmt.Printf("running keeper is incompatible; this binary requires keeper >= %d. gobnc die then start this binary so it can spawn a new keeper\n", version.MinKeeperVersion)
		return errMustUpgrade
	}
	return nil
}

func cmdAuth(ctx context.Context, st *store.Store, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("auth subcommand required")
	}
	switch args[0] {
	case "set-password":
		if len(args) > 1 {
			return fmt.Errorf("usage: auth set-password (password is prompted; do not pass on the command line)")
		}
		gen, err := promptYesNo("Generate a random password?", true)
		if err != nil {
			return err
		}
		var pass string
		if gen {
			pass, err = auth.GeneratePassword(0)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Generated password (save this; it will not be shown again):\n%s\n", pass)
		} else {
			pass, err = promptPasswordConfirm("New bouncer password: ")
			if err != nil {
				return err
			}
		}
		h, err := auth.HashPassword(pass)
		if err != nil {
			return err
		}
		if err := st.SetPasswordHash(ctx, h); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "Password updated.")
		return nil
	case "add-fingerprint":
		if len(args) < 2 {
			return fmt.Errorf("usage: auth add-fingerprint <sha256-hex> [label]")
		}
		label := ""
		if len(args) > 2 {
			label = strings.Join(args[2:], " ")
		}
		fp := strings.ToLower(args[1])
		if err := st.AddFingerprint(ctx, fp, label); err != nil {
			return err
		}
		if label != "" {
			fmt.Fprintf(os.Stderr, "Fingerprint added (%s).\n", label)
		} else {
			fmt.Fprintln(os.Stderr, "Fingerprint added.")
		}
		return nil
	case "list-fingerprints":
		fps, err := st.ListFingerprints(ctx)
		if err != nil {
			return err
		}
		if len(fps) == 0 {
			fmt.Fprintln(os.Stderr, "No fingerprints.")
			return nil
		}
		for i, e := range fps {
			if e.Label != "" {
				fmt.Printf("%d\t%s\t%s\n", i+1, e.Fingerprint, e.Label)
			} else {
				fmt.Printf("%d\t%s\n", i+1, e.Fingerprint)
			}
		}
		return nil
	case "delete-fingerprint", "remove-fingerprint":
		if len(args) != 2 {
			return fmt.Errorf("usage: auth delete-fingerprint <#N|sha256-hex|prefix>")
		}
		fp, err := st.ResolveFingerprint(ctx, args[1])
		if err != nil {
			return err
		}
		if err := st.RemoveFingerprint(ctx, fp); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Deleted %s\n", fp)
		return nil
	default:
		return fmt.Errorf("unknown auth command")
	}
}

func promptPasswordConfirm(prompt string) (string, error) {
	pass, err := promptSecret(prompt)
	if err != nil {
		return "", err
	}
	again, err := promptSecret("Confirm password: ")
	if err != nil {
		return "", err
	}
	if pass != again {
		return "", fmt.Errorf("passwords do not match")
	}
	return pass, nil
}

// promptYesNo asks a Y/n question on the TTY. Empty input uses defaultYes.
func promptYesNo(prompt string, defaultYes bool) (bool, error) {
	fd := int(syscall.Stdin)
	if !term.IsTerminal(fd) {
		return false, fmt.Errorf("refusing to prompt: stdin is not a TTY")
	}
	suffix := " [Y/n] "
	if !defaultYes {
		suffix = " [y/N] "
	}
	fmt.Fprint(os.Stderr, prompt+suffix)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return false, err
	}
	s := strings.TrimSpace(strings.ToLower(line))
	if s == "" {
		return defaultYes, nil
	}
	switch s {
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, fmt.Errorf("please answer y or n")
	}
}

func promptSecret(prompt string) (string, error) {
	fd := int(syscall.Stdin)
	if !term.IsTerminal(fd) {
		return "", fmt.Errorf("refusing to read secret: stdin is not a TTY")
	}
	fmt.Fprint(os.Stderr, prompt)
	b, err := term.ReadPassword(fd)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}
	s := string(b)
	if s == "" {
		return "", fmt.Errorf("empty password")
	}
	return s, nil
}
