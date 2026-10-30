"""Case-insensitive email uniqueness index and PostgreSQL native user_role_enum

Revision ID: 003_email_ci_and_role_enum
Revises: 002_auth_and_rbac
Create Date: 2026-10-30 21:30:00.000000

Rationale for dropping uq_users_email:
The functional unique index uq_users_email_ci ON users (lower(email)) strictly supersedes
uq_users_email. Any query matching email directly or case-insensitively utilizes the functional
index. Retaining uq_users_email would incur redundant index write overhead during user
insertions and updates while failing to prevent mixed-case duplicates.
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "003_email_ci_and_role_enum"
down_revision: Union[str, None] = "002_auth_and_rbac"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()

    # 1. Pre-check: fail if case-insensitive duplicate emails exist
    dupes = bind.execute(sa.text("""
        SELECT lower(email), count(*) 
        FROM users 
        GROUP BY lower(email) 
        HAVING count(*) > 1
    """)).fetchall()
    if dupes:
        raise RuntimeError(
            f"Cannot apply migration 003: case-insensitive duplicate emails exist: {dupes}"
        )

    # 2. Canonicalize existing emails to lowercase
    op.execute("UPDATE users SET email = lower(email)")

    # 3. Drop redundant old unique constraint and create functional unique index
    if bind.dialect.name == "postgresql":
        op.drop_constraint("uq_users_email", "users", type_="unique")
    
    op.create_index(
        "uq_users_email_ci",
        "users",
        [sa.text("lower(email)")],
        unique=True,
    )

    # 4. Native PostgreSQL Enum for user roles
    if bind.dialect.name == "postgresql":
        user_role_enum = postgresql.ENUM("driver", "manager", "auditor", name="user_role_enum")
        user_role_enum.create(bind, checkfirst=True)

        op.execute("ALTER TABLE users ALTER COLUMN role DROP DEFAULT")
        op.execute("ALTER TABLE users ALTER COLUMN role TYPE user_role_enum USING role::user_role_enum")
        op.execute("ALTER TABLE users ALTER COLUMN role SET DEFAULT 'driver'::user_role_enum")


def downgrade() -> None:
    bind = op.get_bind()

    if bind.dialect.name == "postgresql":
        # Revert users.role back to VARCHAR(32) with default 'driver'
        op.execute("ALTER TABLE users ALTER COLUMN role DROP DEFAULT")
        op.execute("ALTER TABLE users ALTER COLUMN role TYPE VARCHAR(32) USING role::text")
        op.execute("ALTER TABLE users ALTER COLUMN role SET DEFAULT 'driver'")

        # Drop enum type
        user_role_enum = postgresql.ENUM("driver", "manager", "auditor", name="user_role_enum")
        user_role_enum.drop(bind, checkfirst=True)

    # Drop functional index and restore original unique constraint
    op.drop_index("uq_users_email_ci", table_name="users")
    if bind.dialect.name == "postgresql":
        op.create_unique_constraint("uq_users_email", "users", ["email"])
