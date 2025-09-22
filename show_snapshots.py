import asyncio
from typing import List, Tuple
from datetime import datetime

from sqlalchemy import select, desc

from app.db import SessionLocal, engine, Base
from app.models import PortfolioSnapshot
from app.metrics import max_drawdown

LIMIT = 20  # how many latest snapshots to show

def ts_to_str(ms: int) -> str:
    # ms -> human time
    return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

async def ensure_schema():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def load_recent(limit: int) -> List[PortfolioSnapshot]:
    async with SessionLocal() as s:
        q = select(PortfolioSnapshot).order_by(desc(PortfolioSnapshot.ts)).limit(limit)
        rows = (await s.execute(q)).scalars().all()
        rows.reverse()  # oldest -> newest for nicer reading / MDD calc
        return rows

def print_table(rows: List[PortfolioSnapshot]):
    if not rows:
        print("No snapshots yet."); return
    print(f"{'time (UTC)':19s}  {'equity':>10s}  {'cash':>10s}  {'unreal':>10s}  {'exposure':>10s}")
    print("-" * 70)
    for r in rows:
        print(f"{ts_to_str(r.ts):19s}  {r.equity:10.2f}  {r.cash:10.2f}  {r.unrealized:10.2f}  {r.exposure:10.2f}")

def print_mdd(rows: List[PortfolioSnapshot]):
    pts: List[Tuple[int, float]] = [(r.ts, float(r.equity)) for r in rows]
    if len(pts) < 2:
        print("\nMax drawdown: need at least 2 snapshots.")
        return
    dd_abs, dd_pct, peak_ts, trough_ts = max_drawdown(pts)
    print(f"\nMax drawdown: {dd_abs:.2f} ({dd_pct*100:.2f}%) "
          f"from {ts_to_str(peak_ts)} to {ts_to_str(trough_ts)}")

async def main():
    await ensure_schema()
    rows = await load_recent(LIMIT)
    print_table(rows)
    print_mdd(rows)

if __name__ == "__main__":
    asyncio.run(main())
