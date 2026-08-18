.PHONY: test test-race test-integration test-ircd build cert

# Last tag plus commits (v0.1.1-5-gabcdef1) or the tag itself on a
# release commit. Empty when git describe is unavailable; DisplayVersion
# then falls back to embedded VCS info or Version.
VERSION_PKG := github.com/MasterBOFH/GoBNC/internal/version
VERSION ?= $(patsubst v%,%,$(shell git describe --tags --always --dirty --match 'v*' 2>/dev/null))
LDFLAGS := -X $(VERSION_PKG).stamp=$(VERSION)

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

build:
	go build -ldflags "$(LDFLAGS)" -o bin/gobnc ./cmd/gobnc
	go build -ldflags "$(LDFLAGS)" -o bin/gobnc-keeper ./cmd/keeper

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
