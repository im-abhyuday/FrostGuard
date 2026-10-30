"""Initial schema for ColdChainIQ: shipments, devices, readings, alerts

Revision ID: 001_initial_schema
Revises: 
Create Date: 2026-10-30 18:25:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Shipments Table
    op.create_table(
        "shipments",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("cargo_type", sa.String(length=32), nullable=False),
        sa.Column("origin", sa.String(length=128), nullable=True),
        sa.Column("destination", sa.String(length=128), nullable=True),
        sa.Column("route_name", sa.String(length=128), nullable=True),
        sa.Column("setpoint_c", sa.Float(), nullable=False),
        sa.Column("safe_min_c", sa.Float(), nullable=False),
        sa.Column("safe_max_c", sa.Float(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), server_default="IN_TRANSIT", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    # 2. Devices Table
    op.create_table(
        "devices",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("shipment_id", sa.String(length=64), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["shipment_id"], ["shipments.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )

    # 3. Readings Table
    op.create_table(
        "readings",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.String(length=64), nullable=False),
        sa.Column("shipment_id", sa.String(length=64), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("temperature_c", sa.Float(), nullable=False),
        sa.Column("humidity_pct", sa.Float(), nullable=False),
        sa.Column("door_open", sa.Boolean(), nullable=False),
        sa.Column("compressor_on", sa.Boolean(), nullable=False),
        sa.Column("compressor_status", sa.String(length=32), nullable=False),
        sa.Column("ambient_temp_c", sa.Float(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("speed_kmph", sa.Float(), nullable=False),
        sa.Column("setpoint_c", sa.Float(), nullable=False),
        sa.Column("battery_pct", sa.Float(), nullable=False),
        sa.Column("cargo_type", sa.String(length=32), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["shipment_id"], ["shipments.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "ts", name="uq_readings_device_ts"),
    )

    op.create_index("ix_readings_shipment_ts_desc", "readings", ["shipment_id", sa.text("ts DESC")])
    op.create_index("ix_readings_device_ts_desc", "readings", ["device_id", sa.text("ts DESC")])
    op.create_index("ix_readings_ts_brin", "readings", ["ts"], postgresql_using="brin")

    # 4. Alerts Table
    op.create_table(
        "alerts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("shipment_id", sa.String(length=64), nullable=True),
        sa.Column("device_id", sa.String(length=64), nullable=True),
        sa.Column("severity", sa.String(length=32), nullable=False),
        sa.Column("risk_score", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="OPEN", nullable=False),
        sa.Column("acknowledged_by", sa.String(length=128), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reasons", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["shipment_id"], ["shipments.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("alerts")
    op.drop_index("ix_readings_ts_brin", table_name="readings", postgresql_using="brin")
    op.drop_index("ix_readings_device_ts_desc", table_name="readings")
    op.drop_index("ix_readings_shipment_ts_desc", table_name="readings")
    op.drop_table("readings")
    op.drop_table("devices")
    op.drop_table("shipments")
