"""
Alembic environment configuration.

Reads database credentials from the same .env file the rest of the project
uses (via db_config.get_db_config()), so there is a single source of truth
for connection details.

Supports two modes:
  - offline  : generates SQL without a live DB connection
  - online   : connects to the DB and runs migrations directly
"""

import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context

# Make sure the project root is on the path so db_config is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db_config import get_db_config  # noqa: E402

# ---------------------------------------------------------------------------
# Alembic Config object — gives access to alembic.ini values
# ---------------------------------------------------------------------------
config = context.config

# Set up Python logging from the alembic.ini [loggers] section
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ---------------------------------------------------------------------------
# Build the SQLAlchemy URL from the project's existing db_config
# ---------------------------------------------------------------------------
def get_url() -> str:
    cfg = get_db_config()
    return (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    )


# target_metadata is used for --autogenerate support.
# Import your SQLAlchemy models here if you have them; leave None otherwise.
target_metadata = None


# ---------------------------------------------------------------------------
# Offline mode — generate SQL script without DB connection
# ---------------------------------------------------------------------------
def run_migrations_offline() -> None:
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Online mode — connect to DB and run migrations directly
# ---------------------------------------------------------------------------
def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,  # no pooling in migration scripts
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,           # detect column type changes
            compare_server_default=True, # detect default value changes
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()