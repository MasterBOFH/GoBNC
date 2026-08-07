# GoBNC

Single-user IRC bouncer in Go.

## Build

```bash
make build
```

## Configure

Copy `gobnc.json.example` to `gobnc.json`. Generate TLS certs for the listener.

With `"log_level": "debug"`, the console shows colored, columnized logs including raw IRC (`<<` in / `>>` out) for uplink and downlink; set `"log_file"` for JSON logs. `PASS` / `AUTHENTICATE` secrets are redacted.

```bash
./bin/gobnc auth set-password          # asks to generate a random password (or enter one)
./bin/gobnc auth add-fingerprint <sha256-hex>
./bin/gobnc network add libera irc.libera.chat 6697 yournick --sasl-user=you --sasl-pass
./bin/gobnc serve -config gobnc.json           # backgrounds by default
./bin/gobnc serve -config gobnc.json -debug    # foreground (developer mode)
./bin/gobnc stop -config gobnc.json
```

`serve` re-execs into the background by default and writes a pid file (`pid_file`, default under `$XDG_RUNTIME_DIR/gobnc` or `~/.gobnc`). Use `-debug`/`-d` or `-foreground`/`-f` to stay attached. Under systemd or FreeBSD `rc.d`, always use `-foreground` (see `packaging/`). Daemon mode defaults `log_file` to the state dir when unset.
`--sasl-pass` (no value) prompts for the SASL password. Do not pass passwords on the command line.

You can also run `network add` / `network delete` while the daemon is already running: the CLI writes SQLite and notifies the process over `control_socket` so the uplink starts or stops immediately. When unset (or the legacy value `gobnc.sock`), the socket defaults to `$XDG_RUNTIME_DIR/gobnc/gobnc.sock`, or `~/.gobnc/gobnc.sock` if `XDG_RUNTIME_DIR` is unset. The daemon creates the parent directory mode `0700` and the socket mode `0600`, and accepts connections only from the same Unix UID.

Update an existing network without dropping the current uplink (`network mod`); new settings apply on the next reconnect:

```bash
./bin/gobnc network mod libera --host=irc.libera.chat --port=6667 --tls=false
```

Flags: `--host=`, `--port=`, `--nick=`, `--tls=true|false`, `--user=` / `--username=`, `--realname=`, `--sasl-user=`, `--sasl-pass`

Connect with TLS. Select a network via `PASS` as `network/password` (e.g. `PASS libera/s3cret`). The password may contain `/`; the network name is everything before the first `/`. For client-cert auth without a password, use `PASS libera/` or `PASS libera`. Nick is your normal IRC nick.

Channels are remembered when you `JOIN` (including channel keys) and forgotten when you `PART`; they are auto-rejoined on uplink reconnect.

## Rehash (SIGHUP / `gobnc rehash`)

Send `SIGHUP` to the running `serve` process, or run `./bin/gobnc rehash`, to reload `gobnc.json` and refresh network rows from SQLite without dropping connected clients or reconnecting uplinks. Listener TLS certificates are hot-swapped: existing sessions keep their handshake; new connections use the reloaded cert/key.

Ignored on rehash (require a full restart): `listen_addr`, `db_path`, `control_socket`, `log_file`, `log_level`.

## Service units

Release tags build installers automatically:

- Linux: `.deb` (amd64/arm64) with systemd unit
- macOS: `.pkg` (amd64/arm64) with LaunchDaemon
- FreeBSD: `.tar.gz` with binary + `rc.d` script (native pkgng needs a FreeBSD builder)

See `packaging/` for local build scripts and unit files. Supervised installs run `gobnc serve … -foreground`.

## Security notes

- Network passwords, SASL credentials, and channel keys are stored **plaintext** in SQLite so the bouncer can reconnect unattended. GoBNC enforces mode `0600` on the DB and on `log_file` when set.
- On shared hosts, keep `db_path`, `log_file`, and any explicit `control_socket` under a private directory you own. Avoid placing the control socket in a shared-writable path (CWD on a shared machine, `/tmp`, etc.).
- Client IRC lines are capped at 4608 bytes (IRCv3 client tags + 512-byte message); oversize input gets `ERR_INPUTTOOLONG` (`417`). Uplink lines are capped at 8703 bytes and dropped if longer. Concurrent clients default to `max_clients` 32; password checks are concurrency-limited.
- Tunables (see comments in `gobnc.json.example`): `max_flood_queue` (default 16384, `0` = unlimited), `legacy_playback_max` (default 50000, `0` = unlimited attach backlog), `chathistory_max` (default 100; ISUPPORT `CHATHISTORY=N` / per-query cap), `history_retention_days` (default `0` = no prune).
- Legacy attach playback uses a **shared** per-network/per-target cursor by design: one client's attach advances the watermark for other devices on the same network.

## Tests

```bash
make test
make test-race
make test-integration   # high-chatter multi-client + CHATHISTORY playback
make test-ircd          # parser interop vs ircu2, Unreal, InspIRCd, Ergo, … (Docker)
```
