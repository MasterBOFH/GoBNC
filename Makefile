.PHONY: test test-race test-integration test-ircd build cert

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	go test -tags=integration -count=1 -timeout 120s ./...

# Comprehensive parser interop against major ircds (Docker Compose).
# Requires Docker; irccom images use linux/amd64 (QEMU on Apple Silicon).
test-ircd:
	docker compose -f docker/ircd/docker-compose.yml up -d --pull missing
	@echo "waiting for ircds..."
	@sleep 8
	go test -tags=ircd -count=1 -timeout 180s -parallel 4 ./internal/ircdtest/
	docker compose -f docker/ircd/docker-compose.yml down

# No -X version.stamp here: leaving it unset makes DisplayVersion fall
# back to its own composition (Version, currently "0.2.0-dev", plus the
# commit Go's toolchain already embeds automatically) — exactly
# "0.2.0-dev+<commit>[-dirty]", the same on every platform's make with no
# git-describe shell-out needed at all. Only the release workflow
# (.github/workflows/release.yml, entirely separate from this Makefile)
# sets Version and stamp to the bare tag, e.g. "0.2.0" with nothing
# appended — that's what makes a real release look clean and this look
# unmistakably like a dev build.
build:
	go build -o bin/gobnc ./cmd/gobnc
	go build -o bin/gobnc-keeper ./cmd/keeper

# Self-signed server + client leaf certs under certs/ (see scripts/gen-certs.sh).
#   make cert                         # prompt for hostname (TTY) or localhost
#   make cert HOST=bnc.example.com
#   make cert HOST=203.0.113.10
# Optional: CERT_DIR=certs CERT_DAYS=3650 CERT_SAN=DNS:bnc.example.com,IP:203.0.113.10
CERT_DIR ?= certs
CERT_DAYS ?= 3650

cert:
	CERT_DIR="$(CERT_DIR)" CERT_DAYS="$(CERT_DAYS)" CERT_SAN="$(CERT_SAN)" \
		./scripts/gen-certs.sh $(HOST)
