import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Add paths for coldchain_common and api
API_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = API_ROOT.parents[1]
sys.path.insert(0, str(WORKSPACE_ROOT / "libs" / "coldchain_common" / "src"))
sys.path.insert(0, str(API_ROOT / "src"))

from coldchain_common.db_models import Base

# Alembic Config object
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set model metadata for autogenerate
target_metadata = Base.metadata

# Allow environment variable override of database URL
postgres_dsn = os.getenv("POSTGRES_DSN")
if postgres_dsn:
    # Ensure psycopg (v3) dialect driver prefix
    if postgres_dsn.startswith("postgresql://"):
        postgres_dsn = postgres_dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    elif postgres_dsn.startswith("postgres://"):
        postgres_dsn = postgres_dsn.replace("postgres://", "postgresql+psycopg://", 1)
    config.set_main_option("sqlalchemy.url", postgres_dsn)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
