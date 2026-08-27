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
# -checks=all turns on the ST* style family too, which vet does not cover: it is
# what requires every package to carry a package comment. The tool is pinned in
# go.mod, so this runs the same version everywhere.
> go tool staticcheck -checks=all ./...
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

run-api:
> go run ./cmd/api
