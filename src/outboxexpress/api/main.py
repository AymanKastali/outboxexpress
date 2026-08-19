"""Application assembly and the ``outbox-api`` entrypoint."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from outboxexpress.shared.config import get_settings
from outboxexpress.shared.db import create_async_engine_from_settings, create_sessionmaker
from outboxexpress.shared.logging import configure_logging

from .errors import register_error_handlers
from .routes import router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # Process startup, not app construction: `create_app` stays free of side
    # effects, so building an app in a test does not reconfigure global logging.
    configure_logging()
    # One engine per process, built after the event loop exists: an async pool
    # binds to the loop that created it.
    engine = create_async_engine_from_settings()
    app.state.sessionmaker = create_sessionmaker(engine)
    try:
        yield
    finally:
        await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Flash Order Notifier", version="0.1.0", lifespan=lifespan)
    app.include_router(router)
    register_error_handlers(app)
    return app


app = create_app()


def run() -> None:
    settings = get_settings()
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
