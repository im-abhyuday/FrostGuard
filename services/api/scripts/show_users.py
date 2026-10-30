"""Script to display seeded users table with argon2id hashes."""
import asyncio
import os
from pathlib import Path
import sys

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE_ROOT / "libs" / "coldchain_common" / "src"))
sys.path.insert(0, str(WORKSPACE_ROOT / "services" / "api" / "src"))

from sqlalchemy import select
from coldchain_common.db_models import User
from api.db.session import async_session_factory

async def show_users():
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    async with async_session_factory() as session:
        users = (await session.execute(select(User).order_by(User.role))).scalars().all()
        print("\n" + "=" * 95)
        print(f"{'ROLE':<10} | {'EMAIL':<28} | {'ACTIVE':<7} | {'HASHED PASSWORD (ARGON2ID)'}")
        print("-" * 95)
        for u in users:
            print(f"{u.role:<10} | {u.email:<28} | {str(u.is_active):<7} | {u.hashed_password[:45]}...")
        print("=" * 95 + "\n")

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(show_users())
