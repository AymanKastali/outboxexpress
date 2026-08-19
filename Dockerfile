# syntax=docker/dockerfile:1

# --- build: resolve the locked dependency set into a self-contained venv ---
FROM python:3.14-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.10.0 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Dependencies before source: they change far less often than a route does, so
# editing the API does not re-resolve the lock file. The lock and manifest are
# bind-mounted rather than copied so they leave no layer behind.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY pyproject.toml uv.lock ./
COPY src ./src

# --no-editable installs the package into the venv rather than linking back to
# /app/src, so the runtime stage can take the venv alone and owns no source tree.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# --- run: the interpreter and the venv, without the tooling that produced them ---
FROM python:3.14-slim-bookworm

# The container writes nothing, so it has no reason to own the filesystem it runs on.
RUN useradd --create-home --uid 1000 outbox

# `.venv/bin` first, so `outbox-api` and `alembic` are bare commands -- no `uv run`
# in the runtime image, which has no uv.
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY --from=builder --chown=outbox:outbox /app/.venv /app/.venv

# Alembic reads these off disk at runtime. They are data the venv cannot carry,
# which is why they are copied here and not in the builder.
COPY --chown=outbox:outbox alembic.ini ./
COPY --chown=outbox:outbox migrations ./migrations

USER outbox
EXPOSE 8000
CMD ["outbox-api"]
