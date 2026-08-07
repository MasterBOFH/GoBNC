#!/usr/bin/env bash
# Generate self-signed TLS certificates for GoBNC:
#   server.* — presented to your IRC client (listener)
#   client.* — presented by GoBNC when it connects to an IRC network (CERTFP / SASL EXTERNAL)
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

# Server: certificate the bouncer presents to IRC clients connecting to it.
gen_leaf server serverAuth "digitalSignature,keyAgreement"
# Client: certificate GoBNC presents when connecting to an IRC network
# (NickServ CERTFP / SASL EXTERNAL). Not used for logging into the bouncer.
gen_leaf client clientAuth "digitalSignature"

# Combined PEM (optional convenience).
cat "$CERT_DIR/client.crt" "$CERT_DIR/client.key" >"$CERT_DIR/client.pem"
chmod 600 "$CERT_DIR/client.pem"

fp_sha256() {
	openssl x509 -in "$1" -outform DER | openssl dgst -sha256 -hex | awk '{print $2}'
}
fp_sha512() {
	openssl x509 -in "$1" -outform DER | openssl dgst -sha512 -hex | awk '{print $2}'
}

SERVER_FP="$(fp_sha256 "$CERT_DIR/server.crt")"
CLIENT_SHA256="$(fp_sha256 "$CERT_DIR/client.crt")"
CLIENT_SHA512="$(fp_sha512 "$CERT_DIR/client.crt")"

echo "wrote ${CERT_DIR}/server.crt + server.key  (CN=${HOST} SAN=${SAN})"
echo "wrote ${CERT_DIR}/client.crt + client.key + client.pem  (IRC network client cert)"
echo "server sha256 (pin in your IRC client / tls_verify): ${SERVER_FP}"
echo "client sha512 (NickServ CERTFP): ${CLIENT_SHA512}"
echo "client sha256 (optional gobnc auth add-fingerprint): ${CLIENT_SHA256}"
echo
echo "Use the client cert when GoBNC connects to IRC (or leave empty and set per-network):"
echo "  \"tls_client_cert\": \"${CERT_DIR}/client.crt\","
echo "  \"tls_client_key\": \"${CERT_DIR}/client.key\","
echo "Then: ./bin/gobnc rehash   # or restart; network reconnect to redial"
echo "NickServ: CERT ADD ${CLIENT_SHA512}"
echo "Logging into the bouncer: password PASS, or any client cert + auth add-fingerprint."
