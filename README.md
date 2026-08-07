# GoBNC

Single-user IRC bouncer in Go.

## 1. Installing GoBNC

```bash
make build
cp gobnc.json.example gobnc.json
make cert                        # prompts for hostname; or: make cert HOST=bnc.example.com
./bin/gobnc auth set-password    # bouncer login password (not NickServ)
./bin/gobnc network add libera irc.libera.chat 6697 yournick --sasl-user=you --sasl-pass
./bin/gobnc serve -config gobnc.json
```

`make cert` writes under `certs/`:

| File | Role |
| --- | --- |
| `server.crt` / `server.key` | Presented to your IRC client (bouncer listener) |
| `client.crt` / `client.key` / `client.pem` | Presented by GoBNC when connecting to an IRC network (CERTFP / SASL EXTERNAL) |

It prints the **server** SHA-256 (pin in your IRC client) and **client** SHA-512 (NickServ CERTFP). Set `tls_client_cert` / `tls_client_key` in `gobnc.json` to use that network client cert globally, or override per network with `--tls-cert=` / `--tls-key=`.

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
./bin/gobnc auth add-fingerprint <sha256-hex>
# e.g. openssl x509 -in your-client.crt -outform DER | openssl dgst -sha256 -hex
```

Then connect with `PASS libera/` (or `libera`) and that client cert enabled.

### CERTFP / SASL EXTERNAL (to the IRC network)

With `tls_client_cert` / `tls_client_key` set (or per-network `--tls-cert=` / `--tls-key=`), GoBNC presents that certificate when connecting to the IRC server. Register the **SHA-512** fingerprint with NickServ (`CERT ADD …` from `make cert` output). Empty network paths inherit the global JSON paths; `none` or `-` disables the cert for one network.

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
  [--sasl-user=] [--sasl-pass] \
  [--flood-burst=] [--flood-rate=] \
  [--alt-nick=] [--nick-recovery=true|false]
./bin/gobnc network mod <name> \
  [--host=] [--port=] [--nick=] [--tls=true|false] [--tls-noverify=true|false] \
  [--tls-cert=] [--tls-key=] \
  [--user=] [--realname=] \
  [--sasl-user=] [--sasl-pass] \
  [--flood-burst=] [--flood-rate=] \
  [--alt-nick=] [--nick-recovery=true|false]
./bin/gobnc network delete <name>
./bin/gobnc network reconnect <name>
./bin/gobnc rehash
./bin/gobnc stop
```

```
BNC help
BNC status
BNC network list
BNC network add …
BNC network mod …
BNC network reconnect libera
BNC rehash
```

`BNC` covers the same management commands as the CLI except `serve`, `auth`, and `stop`. For SASL over `BNC`, use `--sasl-pass=secret` (CLI prompts on a TTY for bare `--sasl-pass`).

Nick / identity defaults for `network add` when omitted: `default_nick` / `default_username` / `default_realname` / `default_alt_nick` in `gobnc.json`. `--tls-cert=` / `--tls-key=` override the global network client cert (`none` / `-` disables). SASL mechanism is chosen automatically (SCRAM-SHA-256 → PLAIN with user+pass; EXTERNAL with a client cert).

`network mod` updates config without dropping the connection to the IRC server (host/TLS/SASL/cert apply on the next reconnect). `network reconnect` forces a reconnect now. `rehash` / `SIGHUP` reloads `gobnc.json` (including global `tls_client_cert`/`tls_client_key`) and network rows without dropping clients. Restart required for: `listen_addr`, `db_path`, `control_socket`, `log_file`, `log_level`.

## Packaging

Release tags build `.deb` / macOS `.pkg` / FreeBSD `.tar.gz` — see `packaging/`.

## Security

- Network passwords, SASL credentials, and channel keys are stored **plaintext** in SQLite (mode `0600`). Keep `db_path`, `log_file`, and any explicit `control_socket` under a private directory.
- Control socket defaults under `$XDG_RUNTIME_DIR/gobnc` or `~/.gobnc` (dir `0700`, socket `0600`, same-UID only).
- Line limits: client 4608 bytes (`417` if longer); uplink 8703 (dropped). Default `max_clients` 32.
- Tunables in `gobnc.json.example`: `max_flood_queue`, `legacy_playback_max`, `chathistory_max`, `history_retention_days`. Legacy attach playback uses a **shared** per-network/per-target cursor.

## Debug

`"log_level": "debug"` enables colored console IRC traces (`<<` / `>>`). `"log_file"` writes JSON. `PASS` / `AUTHENTICATE` secrets are redacted. `serve -debug` / `-d` stays in the foreground with debug logging.

## Tests

```bash
make test
make test-race
make test-integration
make test-ircd          # parser interop vs major ircds (Docker)
```
