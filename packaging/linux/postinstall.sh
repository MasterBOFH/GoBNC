#!/bin/sh
set -e
if ! getent group gobnc >/dev/null 2>&1; then
	groupadd --system gobnc
fi
if ! getent passwd gobnc >/dev/null 2>&1; then
	useradd --system --gid gobnc --home-dir /var/lib/gobnc --shell /usr/sbin/nologin gobnc
fi
install -d -o gobnc -g gobnc -m 0750 /var/lib/gobnc /var/log/gobnc /run/gobnc /etc/gobnc
# First install only: seed working config from the packaged example.
if [ ! -f /etc/gobnc/gobnc.json ] && [ -f /etc/gobnc/gobnc.json.example ]; then
	cp /etc/gobnc/gobnc.json.example /etc/gobnc/gobnc.json
	chown root:gobnc /etc/gobnc/gobnc.json
	chmod 640 /etc/gobnc/gobnc.json
fi
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload >/dev/null 2>&1 || true
fi
echo "GoBNC installed. Edit /etc/gobnc/gobnc.json (from gobnc.json.example), set TLS paths and a password, then: systemctl enable --now gobnc"
