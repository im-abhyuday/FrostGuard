# coldchain_common

Shared library containing canonical Pydantic models, domain enums, and SQLAlchemy 2.0 ORM database models for ColdChainIQ / FrostGuard.

Installed in editable mode across `services/simulator`, `services/consumer`, and `services/api` to eliminate schema drift and guarantee compile-time type safety.
