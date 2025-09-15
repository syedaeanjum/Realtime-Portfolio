import asyncio
from time import time

from sqlalchemy import select, desc
from app.db import engine, Base, SessionLocal
from app.models import PortfolioSnapshot
from app.pnl import compute_pnl
from app.metrics import max_drawdown

# how many past snapshots to include when computing drawdown
DD_LOOKBACK = 5000  # tune as you like

async def ensure_schema():
    # make sure all tables (incl. new snapshots) exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def write_snapshot(session, snap):
    # insert one snapshot row
    row = PortfolioSnapshot(
        ts=int(time() * 1000),
        equity=float(snap["equity"]),
        cash=float(snap["cash"]),
        unrealized=float(snap["unrealized"]),
        exposure=float(snap["exposure"]),
    )
    session.add(row)
    await session.commit()
    return row

async def recent_snapshots(session, limit: int):
    # latest N snapshots, newest first
    q = select(PortfolioSnapshot).order_by(desc(PortfolioSnapshot.ts)).limit(limit)
    rows = (await session.execute(q)).scalars().all()
    # return oldest->newest for drawdown function
    rows.reverse()
    return [(r.ts, r.equity) for r in rows]

async def main():
    await ensure_schema()
    async with SessionLocal() as s:
        # compute PnL using latest bar closes (from app/pnl.py)
        snap = await compute_pnl(s, cash=1000.0)  # adjust cash as needed

        # write snapshot
        row = await write_snapshot(s, snap)
        print(f"Snapshot written @ {row.ts} | equity={snap['equity']:.2f} cash={snap['cash']:.2f} "
            f"unreal={snap['unrealized']:.2f} exposure={snap['exposure']:.2f}")

        # load history and compute max drawdown
        pts = await recent_snapshots(s, DD_LOOKBACK)
        if len(pts) >= 2:
            dd_abs, dd_pct, peak_ts, trough_ts = max_drawdown(pts)
            print(f"Max drawdown: {dd_abs:.2f} ({dd_pct*100:.2f}%) "
                f"from {peak_ts} to {trough_ts}")
        else:
            print("Not enough snapshots yet for drawdown.")

if __name__ == "__main__":
    asyncio.run(main())
