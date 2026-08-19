from logging.config import fileConfig

from alembic import context

from outboxexpress.shared.config import get_settings
from outboxexpress.shared.db import create_sync_engine_from_settings
from outboxexpress.shared.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=get_settings().database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    # Built from settings rather than alembic.ini: one source for the URL, and no
    # ConfigParser interpolation to trip over a '%' in a password.
    connectable = create_sync_engine_from_settings()
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
