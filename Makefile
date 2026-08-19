.DEFAULT_GOAL := help
COMPOSE := docker compose

# The host port only. Inside the network the API is always on 8000. Override when
# something already owns 8000: `make up HOST_PORT=8080`.
HOST_PORT ?= 8000
export OUTBOX_HOST_PORT := $(HOST_PORT)

.PHONY: help up down logs ps smoke test lint clean

help:  ## List the targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk -F':.*?## ' '{printf "  \033[36m%-7s\033[0m %s\n", $$1, $$2}'

up:  ## Build and start Postgres and the migrated API
	$(COMPOSE) up --build --wait

down:  ## Stop the stack, keeping the database volume
	$(COMPOSE) down

logs:  ## Follow the API log
	$(COMPOSE) logs -f api

ps:  ## Show service state and health
	$(COMPOSE) ps

smoke:  ## Prove the running stack end to end
	BASE_URL=http://127.0.0.1:$(HOST_PORT) ./scripts/smoke.sh

test:  ## Run the suite (testcontainers, independent of the compose stack)
	uv run pytest

lint:  ## Lint and type-check
	uv run ruff check . && uv run pyright

clean:  ## Stop the stack and delete the database volume
	$(COMPOSE) down --volumes
