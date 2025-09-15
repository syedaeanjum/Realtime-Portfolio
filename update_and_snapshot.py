import asyncio
import importlib
from time import time

from app.db import engine, Base, SessionLocal
from app.pnl import compute_pnl
from app.models import PortfolioSnapshot

INTERVAL_SECS = 60  # run every 60s
CASH = 1000.0       # starting cash (adjust)

async def ensure_schema():
    # make sure all tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def write_snapshot(session, snap):
    # insert one portfolio snapshot
    row = PortfolioSnapshot(
        ts=int(time() * 1000),
        equity=float(snap["equity"]),
        cash=float(snap["cash"]),
        unrealized=float(snap["unrealized"]),
        exposure=float(snap["exposure"]),
    )
    session.add(row)
    await session.commit()

async def run_once():
    # step 1: update bars (one pass)
    updater = importlib.import_module("update_binance")
    if not hasattr(updater, "run_once"):
        raise RuntimeError("update_binance.run_once not found. Set RUN_FOREVER=False and ensure run_once() exists.")
    await updater.run_once()

    # step 2: compute PnL and store a snapshot
    async with SessionLocal() as s:
        snap = await compute_pnl(s, cash=CASH)
        await write_snapshot(s, snap)
        print(f"cycle done: equity={snap['equity']:.2f} unreal={snap['unrealized']:.2f} exposure={snap['exposure']:.2f}")

async def main():
    await ensure_schema()
    while True:
        try:
            await run_once()
        except Exception as e:
            print(f"cycle error: {e!r}")
        await asyncio.sleep(INTERVAL_SECS)

if __name__ == "__main__":
    asyncio.run(main())
