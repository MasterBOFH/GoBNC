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

`make cert` writes under `certs/` (paths match `gobnc.json.example`):

| File | Role |
| --- | --- |
| `server.crt` / `server.key` | Listener cert |
| `client.crt` / `client.key` / `client.pem` | Optional client identity for cert login |

It prints **server** and **client** SHA-256 fingerprints.

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

**Cert login:**

1. Configure the IRC client to present a TLS client certificate (e.g. `certs/client.pem`).
2. Register that cert’s SHA-256 (the **client** fingerprint from `make cert`):

```bash
./bin/gobnc auth add-fingerprint <client-sha256>
# or:
openssl x509 -in certs/client.crt -outform DER | openssl dgst -sha256 -hex
```

3. Connect with `PASS libera/` (or `libera`) and the client cert enabled.

Channels you `JOIN` (including keys) are remembered and auto-rejoined on uplink reconnect; `PART` forgets them.

### Administer

Administer the running bouncer with the CLI or the IRC `BNC` command (`/quote BNC …`, or a client alias such as `/bnc`):

```bash
./bin/gobnc status
./bin/gobnc network list|add|mod|delete|reconnect …
./bin/gobnc rehash
./bin/gobnc stop
```

```
BNC help
BNC status
BNC network list
BNC network reconnect libera
BNC rehash
```

`BNC` covers the same management commands as the CLI except `serve`, `auth`, and `stop`. For SASL over `BNC`, use `--sasl-pass=secret`.

`network mod` updates config without dropping the uplink (host/TLS/SASL on next reconnect). `network reconnect` forces an uplink reconnect now. `rehash` / `SIGHUP` reloads `gobnc.json` and network rows without dropping clients. Restart required for: `listen_addr`, `db_path`, `control_socket`, `log_file`, `log_level`.

Identity defaults for `network add`: `default_nick` / `default_username` / `default_realname` / `default_alt_nick` in `gobnc.json`. `--sasl-pass` on the CLI prompts on a TTY.

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
