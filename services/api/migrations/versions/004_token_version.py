"""Add token_version column to users for instant multi-device revocation

Revision ID: 004_token_version
Revises: 003_case_insensitive_email_and_role_enum
Create Date: 2026-10-30 21:40:00.000000

Adds an integer token_version column (default 1, not null) to the users table.
Incremented whenever a user changes password, has their role updated, is deactivated,
or executes a logout-all. Active access tokens with an outdated tv claim are immediately
rejected by the authentication layer even during Redis outages.
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "004_token_version"
down_revision: Union[str, None] = "003_email_ci_and_role_enum"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("token_version", sa.Integer(), server_default="1", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("users", "token_version")
