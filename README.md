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
./bin/gobnc auth set-password 'your-password'
./bin/gobnc auth add-fingerprint <sha256-hex>
./bin/gobnc network add libera irc.libera.chat 6697 yournick --sasl-user=you --sasl-pass=secret
./bin/gobnc serve -config gobnc.json
```

You can also run `network add` / `network delete` while the daemon is already running: the CLI writes SQLite and notifies the process over `control_socket` (default `gobnc.sock`) so the uplink starts or stops immediately.

Update an existing network without dropping the current uplink (`network mod`); new settings apply on the next reconnect:

```bash
./bin/gobnc network mod libera --host=irc.libera.chat --port=6667 --tls=false
```

Flags: `--host=`, `--port=`, `--nick=`, `--tls=true|false`, `--user=` / `--username=`, `--realname=`, `--sasl-user=`, `--sasl-pass=`

Connect with TLS. Select a network via `PASS` as `network/password` (e.g. `PASS libera/s3cret`). The password may contain `/`; the network name is everything before the first `/`. For client-cert auth without a password, use `PASS libera/` or `PASS libera`. Nick is your normal IRC nick.

Channels are remembered when you `JOIN` (including channel keys) and forgotten when you `PART`; they are auto-rejoined on uplink reconnect.

## Tests

```bash
make test
make test-race
make test-integration   # high-chatter multi-client + CHATHISTORY playback
make test-ircd          # parser interop vs ircu2, Unreal, InspIRCd, Ergo, … (Docker)
```
