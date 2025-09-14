import asyncio
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db import SessionLocal, engine, Base
from app.models import Symbol, Bar
from app.clients.binance import get_klines

SYMBOL = "BTCUSDT"   # If binance.us rejects USDT, try: "BTCUSD"
INTERVAL = "1m"
LIMIT = 500  # Binance often allows up to 1000

def rows_from_klines(symbol_id: int, interval: str, klines: Iterable[list]):
    # Binance kline layout:
    # 0 openTime(ms), 1 open, 2 high, 3 low, 4 close, 5 volume, 6 closeTime(ms), ...
    for k in klines:
        yield {
            "symbol_id": symbol_id,
            "ts": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "timeframe": interval,
        }

async def ensure_schema():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def upsert_bars(session, rows):
    data = list(rows)
    if not data:
        return
    stmt = sqlite_insert(Bar).values(data)
    # SQLite: specify conflict target columns (not constraint name)
    stmt = stmt.on_conflict_do_nothing(index_elements=["symbol_id", "ts", "timeframe"])
    await session.execute(stmt)
    await session.commit()

async def main():
    await ensure_schema()

    async with SessionLocal() as s:
        # ensure symbol exists
        res = await s.execute(select(Symbol).where(Symbol.symbol == SYMBOL))
        sym = res.scalar_one_or_none()
        if sym is None:
            sym = Symbol(symbol=SYMBOL, asset_class="crypto")
            s.add(sym)
            await s.commit()
            await s.refresh(sym)

        # fetch klines
        klines = await get_klines(SYMBOL, INTERVAL, LIMIT)

        # upsert bars
        await upsert_bars(s, rows_from_klines(sym.id, INTERVAL, klines))

        # quick count
        count_res = await s.execute(
            select(Bar).where(Bar.symbol_id == sym.id, Bar.timeframe == INTERVAL)
        )
        bars = count_res.scalars().all()
        print(f"Bars stored for {SYMBOL} ({INTERVAL}): {len(bars)}")

if __name__ == "__main__":
    asyncio.run(main())
