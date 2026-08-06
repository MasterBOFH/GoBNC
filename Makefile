.PHONY: test test-race test-integration test-ircd build

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
	go build -o bin/gobnc ./cmd/gobnc
