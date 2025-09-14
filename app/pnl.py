from typing import Dict
from sqlalchemy import select, desc
from app.models import Bar, Position

async def latest_prices(session) -> Dict[int, float]:
    # map symbol_id -> latest close (1m)
    q = select(Bar.symbol_id, Bar.close).where(Bar.timeframe == "1m") \
        .order_by(Bar.symbol_id, desc(Bar.ts))
    rows = (await session.execute(q)).all()
    out: Dict[int, float] = {}
    for sid, px in rows:
        if sid not in out:  # first (latest) per symbol_id
            out[sid] = float(px)
    return out

async def compute_pnl(session, cash: float = 0.0):
    # get positions
    pos_rows = (await session.execute(select(Position))).scalars().all()
    if not pos_rows:
        return {"equity": cash, "cash": cash, "unrealized": 0.0, "exposure": 0.0, "by_symbol": {}}

    prices = await latest_prices(session)
    unreal = 0.0
    exposure = 0.0
    by_symbol = {}

    for p in pos_rows:
        px = prices.get(p.symbol_id)
        if px is None:
            continue
        # unrealized P&L = (mark - avg) * qty
        upnl = (px - p.avg_price) * p.qty
        mkt_value = px * p.qty
        unreal += upnl
        exposure += abs(mkt_value)
        by_symbol[p.symbol_id] = {"px": px, "qty": p.qty, "avg": p.avg_price, "uPnL": upnl, "mv": mkt_value}

    equity = cash + unreal
    return {"equity": equity, "cash": cash, "unrealized": unreal, "exposure": exposure, "by_symbol": by_symbol}
