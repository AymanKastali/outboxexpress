.RECIPEPREFIX := >
.DEFAULT_GOAL := build

export GOTOOLCHAIN := auto

COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: build test test-integration lint fmt db-up db-down db-logs migrate run-api

build:
> go build ./...

test:
> go test ./...

test-integration:
> go test -tags=integration -count=1 -timeout=10m ./...

lint:
> go vet ./...
> test -z "$$(gofmt -l . )" || (gofmt -l . && echo "gofmt: files need formatting" && exit 1)

fmt:
> gofmt -w .

db-up:
> $(COMPOSE) up -d --wait postgres

db-down:
> $(COMPOSE) down -v

db-logs:
> $(COMPOSE) logs -f postgres

migrate:
> go run ./cmd/migrate -context all -action up
