# GoBNC

Single-user IRC bouncer in Go.

## 1. Installing GoBNC

```bash
make build
cp gobnc.json.example gobnc.json
make cert                        # prompts for hostname; or: make cert HOST=bnc.example.com
./bin/gobnc auth set-password    # bouncer login password (not NickServ)
./bin/gobnc network add libera irc.libera.chat 6697 yournick --sasl-user=you --sasl-pass
# CERTFP only: set tls_client_cert in gobnc.json, then reconnect (no --sasl)
# SASL EXTERNAL: --sasl=true (or --sasl-user=acct) with that cert and no --sasl-pass
./bin/gobnc serve -config gobnc.json
```

`make cert` writes under `certs/`:

| File | Role |
| --- | --- |
| `server.crt` / `server.key` | Presented to your IRC client (bouncer listener) |
| `client.crt` / `client.key` / `client.pem` | Presented by GoBNC when connecting to an IRC network (CERTFP / SASL EXTERNAL) |

It prints the **server** SHA-256 (pin in your IRC client) and **client** SHA-512 (NickServ CERTFP). Set `tls_client_cert` / `tls_client_key` in `gobnc.json` to use that network client cert globally, or override per network with `--tls-cert=` / `--tls-key=`. Set `bind_host` (or `--bind-host=`) to source uplink connections from a specific local address.

Further options: `gobnc.json` (commented in `gobnc.json.example`).

`serve` backgrounds by default (pid file under `$XDG_RUNTIME_DIR/gobnc` or `~/.gobnc`). Use `-foreground` / `-f` under systemd or `rc.d` (see `packaging/`).

## 2. Using GoBNC

Connect with TLS to `listen_addr` (default `127.0.0.1:6697`).

### Login (`PASS`)

`PASS` is `network/` plus either the bouncer password or nothing (cert login). The password is from `auth set-password`, not NickServ/SASL.

| `PASS` | Meaning |
| --- | --- |
| `libera/s3cret` | Network `libera`, password `s3cret` |
| `libera/a/b` | Network `libera`, password `a/b` (only the first `/` splits) |
| `libera/` or `libera` | Network `libera`, client-cert login |

**Password login:** set the IRC client’s server password to `network/your-bouncer-password`. For a self-signed listener, disable TLS certificate verification or pin the **server** SHA-256 from `make cert`.

**Cert login:** present any TLS client certificate from your IRC client and register its SHA-256:

```bash
./bin/gobnc auth add-fingerprint <sha256-hex> [label]
./bin/gobnc auth list-fingerprints
./bin/gobnc auth delete-fingerprint <#N|sha256-hex|prefix>
# e.g. openssl x509 -in your-client.crt -outform DER | openssl dgst -sha256 -hex
```

`label` is an optional note stored with the fingerprint (re-run `add-fingerprint` with the same hash to change it). `list-fingerprints` prints a 1-based index used by `delete-fingerprint #2` (or `2`).

Then connect with `PASS libera/` (or `libera`) and that client cert enabled.

### CERTFP / SASL (to the IRC network)

`tls_client_cert` / `tls_client_key` (or per-network `--tls-cert=` / `--tls-key=`) make GoBNC present a client certificate when connecting. That alone is enough for NickServ **CERTFP** (`CERT ADD` with the SHA-512 from `make cert`). It does **not** start SASL.

Enable bouncer SASL with `--sasl=true`:

- `--sasl-user=` + `--sasl-pass` → SCRAM-SHA-256 or PLAIN (whichever the server offers)
- no password + client cert → **EXTERNAL** (optional `--sasl-user=` is the authorization identity)
- `network add` with `--sasl-user=` implies `--sasl=true` if you omit `--sasl=`

Empty network cert paths inherit the global JSON paths; `none` or `-` disables the cert for one network.

Channels you `JOIN` (including keys) are remembered and auto-rejoined on uplink reconnect; `PART` forgets them.

### Administer

Administer the running bouncer with the CLI or the IRC `BNC` command (`/quote BNC …`, or a client alias such as `/bnc`):

```bash
./bin/gobnc status
./bin/gobnc network list
./bin/gobnc network add <name> <host> <port> [nick] \
  [--nick=] [--tls=true|false] [--tls-noverify=true|false] \
  [--tls-cert=] [--tls-key=] \
  [--user=] [--realname=] \
  [--sasl=true|false] [--sasl-user=] [--sasl-pass] \
  [--flood-burst=] [--flood-rate=] \
  [--alt-nick=] [--nick-recovery=true|false]
./bin/gobnc network mod <name> \
  [--host=] [--port=] [--nick=] [--tls=true|false] [--tls-noverify=true|false] \
  [--tls-cert=] [--tls-key=] \
  [--user=] [--realname=] \
  [--sasl=true|false] [--sasl-user=] [--sasl-pass] \
  [--flood-burst=] [--flood-rate=] \
  [--alt-nick=] [--nick-recovery=true|false]
./bin/gobnc network delete <name>
./bin/gobnc network reconnect <name>
./bin/gobnc network disconnect <name>
./bin/gobnc rehash
./bin/gobnc stop
```

```
BNC help
BNC status
BNC network list
BNC network add …
BNC network mod …
BNC reconnect
BNC disconnect
BNC network reconnect libera
BNC network disconnect libera
BNC rehash
```

`BNC` covers the same management commands as the CLI except `serve`, `auth`, and `stop`. From IRC, `reconnect` / `disconnect` (and `network reconnect` / `network disconnect` without a name) target the network of the current client connection. For SASL over `BNC`, use `--sasl-pass=secret` (CLI prompts on a TTY for bare `--sasl-pass`).

Nick / identity defaults for `network add` when omitted: `default_nick` / `default_username` / `default_realname` / `default_alt_nick` in `gobnc.json`. `--tls-cert=` / `--tls-key=` override the global network client cert (`none` / `-` disables). `--bind-host=` overrides global `bind_host` (`none` / `-` uses the OS default).

`network mod` updates config without dropping the connection to the IRC server (host/TLS/SASL/cert/bind_host apply on the next reconnect). `network reconnect` forces a reconnect now (or starts the network if it was disconnected). `network disconnect` stops the uplink without deleting the network. `rehash` / `SIGHUP` reloads `gobnc.json` (including global `tls_client_cert`/`tls_client_key`/`bind_host`, `log_level`, `log_file`, and `listen_addr`) and network rows without dropping existing clients. When `listen_addr` changes, new connections use the new address; already-connected clients stay on the old socket until they disconnect. Restart required for: `db_path`, `control_socket`.

## Packaging

Release tags build `.deb` / macOS `.pkg` / FreeBSD `.tar.gz` — see `packaging/`.

## Security

- Network passwords, SASL credentials, and channel keys are stored **plaintext** in SQLite (mode `0600`). Keep `db_path`, `log_file`, and any explicit `control_socket` under a private directory.
- Control socket defaults under `$XDG_RUNTIME_DIR/gobnc` or `~/.gobnc` (dir `0700`, socket `0600`, same-UID only).
- Line limits: client 4608 bytes (`417` if longer); uplink 8703 (dropped). Default `max_clients` 32.
- Tunables in `gobnc.json.example`: `max_flood_queue`, `legacy_playback_max`, `chathistory_max`, `history_retention_days`. Legacy attach playback uses a **shared** per-network/per-target cursor.

## Debug

`"log_level": "debug"` enables colored console IRC traces (`<<` / `>>`). `"log_file"` writes JSON. `PASS` / `AUTHENTICATE` secrets are redacted. `serve -debug` / `-d` stays in the foreground and forces a debug console; the log file still uses `log_level` from the config.

## Tests

```bash
make test
make test-race
make test-integration
make test-ircd          # parser interop vs major ircds (Docker)
```
