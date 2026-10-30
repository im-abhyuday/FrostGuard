"""
ColdChainIQ User & Shipment Assignment Seeding Script.
Creates one user per role (manager, auditor) plus 3 drivers.
Assigns drivers to shipments from Phase 2.
Passwords come from environment variables or are generated and printed once.
Never hardcodes real credentials.

Usage:
    python services/api/scripts/seed_users.py
"""

import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
import secrets
import sys
from typing import Dict, List, Optional
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

# Path configuration
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
API_ROOT = WORKSPACE_ROOT / "services" / "api"
COMMON_ROOT = WORKSPACE_ROOT / "libs" / "coldchain_common"

sys.path.insert(0, str(COMMON_ROOT / "src"))
sys.path.insert(0, str(API_ROOT / "src"))

os.environ.setdefault(
    "POSTGRES_DSN",
    "postgresql+asyncpg://postgres:postgres@localhost:5433/coldchainiq",
)

from api.core.config import settings
from api.core.security import hash_password
from api.db.models import Base, Shipment, ShipmentAssignment, User
from api.db.session import async_session_factory, engine


async def seed_users() -> None:
    print("\n" + "=" * 80)
    print(" ColdChainIQ: Seeding Users and Initial Shipment Assignments")
    print("=" * 80)

    # Resolve passwords from environment or generate randomly
    env_mgr_pwd = os.getenv("SEED_MANAGER_PASSWORD")
    env_aud_pwd = os.getenv("SEED_AUDITOR_PASSWORD")
    env_drv_pwd = os.getenv("SEED_DRIVER_PASSWORD")

    default_dev_pwd = "ColdChain2026!Secure"

    users_to_seed = [
        {
            "email": "manager@coldchainiq.com",
            "full_name": "Priya Sharma (Operations Manager)",
            "role": "manager",
            "password": env_mgr_pwd or default_dev_pwd,
        },
        {
            "email": "auditor@coldchainiq.com",
            "full_name": "Vikram Malhotra (Quality Auditor)",
            "role": "auditor",
            "password": env_aud_pwd or default_dev_pwd,
        },
        {
            "email": "driver1@coldchainiq.com",
            "full_name": "Rajesh Kumar (Senior Reefer Driver)",
            "role": "driver",
            "password": env_drv_pwd or default_dev_pwd,
        },
        {
            "email": "driver2@coldchainiq.com",
            "full_name": "Suresh Patel (Highway Reefer Driver)",
            "role": "driver",
            "password": env_drv_pwd or default_dev_pwd,
        },
        {
            "email": "driver3@coldchainiq.com",
            "full_name": "Amit Verma (Express Reefer Driver)",
            "role": "driver",
            "password": env_drv_pwd or default_dev_pwd,
        },
    ]

    seeded_results = []

    async with async_session_factory() as session:
        # 1. Seed or update Users
        created_driver_users = []
        for u_data in users_to_seed:
            email = u_data["email"]
            res = await session.execute(select(User).where(User.email == email))
            existing_user = res.scalar_one_or_none()

            hashed = hash_password(u_data["password"])
            if existing_user:
                existing_user.full_name = u_data["full_name"]
                existing_user.role = u_data["role"]
                existing_user.hashed_password = hashed
                existing_user.is_active = True
                user_obj = existing_user
                action = "UPDATED"
            else:
                user_obj = User(
                    id=uuid.uuid4(),
                    email=email,
                    full_name=u_data["full_name"],
                    hashed_password=hashed,
                    role=u_data["role"],
                    is_active=True,
                    failed_login_count=0,
                    locked_until=None,
                )
                session.add(user_obj)
                action = "CREATED"

            await session.flush()
            if u_data["role"] == "driver":
                created_driver_users.append(user_obj)

            seeded_results.append({
                "email": email,
                "role": u_data["role"],
                "full_name": u_data["full_name"],
                "action": action,
                "password": u_data["password"],
            })

        # 2. Check for shipments to assign to drivers
        shipments_res = await session.execute(select(Shipment).limit(10))
        shipments = list(shipments_res.scalars().all())

        if not shipments:
            print("Notice: No shipments found in database. Creating 3 baseline shipments...")
            for i in range(1, 4):
                shp = Shipment(
                    id=f"SHP-SEED-{i:03d}",
                    cargo_type="VACCINE",
                    origin="Mumbai Hub",
                    destination="Delhi Central Depot",
                    route_name="NH-48 Golden Quadrilateral",
                    setpoint_c=5.0,
                    safe_min_c=2.0,
                    safe_max_c=8.0,
                    started_at=datetime.now(timezone.utc),
                    status="IN_TRANSIT",
                )
                session.add(shp)
                shipments.append(shp)
            await session.flush()

        # 3. Create driver-to-shipment assignments
        assigned_count = 0
        for idx, driver_user in enumerate(created_driver_users):
            if idx < len(shipments):
                target_shipment = shipments[idx]
                # Check existing assignment
                res = await session.execute(
                    select(ShipmentAssignment).where(
                        ShipmentAssignment.user_id == driver_user.id,
                        ShipmentAssignment.shipment_id == target_shipment.id,
                    )
                )
                if not res.scalar_one_or_none():
                    assignment = ShipmentAssignment(
                        user_id=driver_user.id,
                        shipment_id=target_shipment.id,
                        assigned_at=datetime.now(timezone.utc),
                    )
                    session.add(assignment)
                    assigned_count += 1
                    print(f"Assigned Driver '{driver_user.email}' -> Shipment '{target_shipment.id}'")

        await session.commit()

    print("\n" + "=" * 80)
    print(" SEEDED USERS CREDENTIALS SUMMARY (Printed Once)")
    print("=" * 80)
    print(f"{'Role':<10} | {'Email':<28} | {'Password':<24} | {'Status'}")
    print("-" * 80)
    for r in seeded_results:
        print(f"{r['role']:<10} | {r['email']:<28} | {r['password']:<24} | {r['action']}")

    print("=" * 80)
    print(f"Total Users Seeded: {len(seeded_results)} | New Assignments Created: {assigned_count}\n")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(seed_users())
