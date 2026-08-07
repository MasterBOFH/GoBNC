#!/usr/bin/env bash
# Generate self-signed server + client leaf certificates for GoBNC.
# Usage:
#   ./scripts/gen-certs.sh [hostname]
#   make cert HOST=bnc.example.com
#   make cert   # prompts for hostname when stdin is a TTY
set -euo pipefail

CERT_DIR="${CERT_DIR:-certs}"
CERT_DAYS="${CERT_DAYS:-3650}"
HOST="${1:-${HOST:-}}"

if [[ -z "$HOST" ]]; then
	if [[ -t 0 ]]; then
		read -r -p "Hostname (or IP) for the TLS listener [localhost]: " HOST
		HOST="${HOST:-localhost}"
	else
		HOST=localhost
	fi
fi

# Build SAN: DNS or IP for HOST, plus localhost loopback for local testing when HOST is not already local.
if [[ "$HOST" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || [[ "$HOST" == *:* ]]; then
	SAN="IP:${HOST}"
else
	SAN="DNS:${HOST}"
fi
if [[ -n "${CERT_SAN:-}" ]]; then
	SAN="$CERT_SAN"
elif [[ "$HOST" != "localhost" && "$HOST" != "127.0.0.1" ]]; then
	SAN="${SAN},DNS:localhost,IP:127.0.0.1"
fi

mkdir -p "$CERT_DIR"

gen_leaf() {
	local name="$1" eku="$2" ku="$3"
	local key="$CERT_DIR/${name}.key"
	local crt="$CERT_DIR/${name}.crt"
	openssl genpkey -algorithm ec -pkeyopt ec_paramgen_curve:P-256 -out "$key"
	openssl req -key "$key" -new -x509 -days "$CERT_DAYS" -sha256 \
		-subj "/CN=${HOST}" \
		-addext "subjectAltName=${SAN}" \
		-addext "basicConstraints=critical,CA:FALSE" \
		-addext "keyUsage=critical,${ku}" \
		-addext "extendedKeyUsage=${eku}" \
		-out "$crt"
	chmod 600 "$key"
	chmod 644 "$crt"
}

# Server: listener presented to IRC clients.
gen_leaf server serverAuth "digitalSignature,keyAgreement"
# Client: optional client-cert auth to the bouncer (register fingerprint below).
gen_leaf client clientAuth "digitalSignature"

# Combined PEM for clients that want cert+key in one file (e.g. WeeChat tls_cert).
cat "$CERT_DIR/client.crt" "$CERT_DIR/client.key" >"$CERT_DIR/client.pem"
chmod 600 "$CERT_DIR/client.pem"

fp_sha256() {
	openssl x509 -in "$1" -outform DER | openssl dgst -sha256 -hex | awk '{print $2}'
}

SERVER_FP="$(fp_sha256 "$CERT_DIR/server.crt")"
CLIENT_FP="$(fp_sha256 "$CERT_DIR/client.crt")"

echo "wrote ${CERT_DIR}/server.crt + server.key  (CN=${HOST} SAN=${SAN})"
echo "wrote ${CERT_DIR}/client.crt + client.key + client.pem  (clientAuth)"
echo "server sha256: ${SERVER_FP}"
echo "client sha256: ${CLIENT_FP}"
echo
echo "Register client cert for bouncer auth:"
echo "  ./bin/gobnc auth add-fingerprint ${CLIENT_FP}"
echo "WeeChat (example):"
echo "  /set irc.server.NAME.tls_cert ${CERT_DIR}/client.pem"
echo "  /set irc.server.NAME.tls_fingerprint ${SERVER_FP}"
echo "  # or: /set irc.server.NAME.tls_verify off"
echo "PASS for cert-only login: network/  or  network"
