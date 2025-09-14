import asyncio
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db import SessionLocal, engine, Base
from app.models import Symbol, Bar
from app.clients.binance import get_klines

# crypto pairs to fetch
SYMBOLS = ["BTCUSDT", "ETHUSDT", "DOGEUSDT"]
INTERVAL = "1m"   # 1-minute candles
LIMIT = 900       # max candles to pull at once

def alias_list(symbol: str) -> list:
    # fallback: try USD if USDT fails
    alts = [symbol]
    if symbol.endswith("USDT"):
        alts.append(symbol.replace("USDT", "USD"))
    return alts

def rows_from_klines(symbol_id: int, interval: str, klines: Iterable[list]):
    # convert Binance kline array → dict for Bar model
    # layout: [openTime, open, high, low, close, vol, closeTime, ...]
    for k in klines:
        yield {
            "symbol_id": symbol_id,
            "ts": int(k[0]),           # open time (ms)
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "timeframe": interval,
        }

async def ensure_schema():
    # make sure tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def upsert_bars(session, rows):
    # insert rows, skip duplicates on (symbol_id, ts, timeframe)
    data = list(rows)
    if not data:
        return
    stmt = sqlite_insert(Bar).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=["symbol_id", "ts", "timeframe"])
    await session.execute(stmt)
    await session.commit()

async def fetch_one_symbol(logical_symbol: str):
    # handle one symbol (BTCUSDT etc.), with its own DB session
    async with SessionLocal() as session:
        last_err = None
        for sym in alias_list(logical_symbol):
            try:
                # get or create Symbol record
                res = await session.execute(select(Symbol).where(Symbol.symbol == sym))
                db_sym = res.scalar_one_or_none()
                if db_sym is None:
                    db_sym = Symbol(symbol=sym, asset_class="crypto")
                    session.add(db_sym)
                    await session.commit()
                    await session.refresh(db_sym)

                # fetch klines from Binance
                klines = await get_klines(sym, INTERVAL, LIMIT)

                # save to DB
                await upsert_bars(session, rows_from_klines(db_sym.id, INTERVAL, klines))

                # check how many bars stored
                count_res = await session.execute(
                    select(Bar).where(Bar.symbol_id == db_sym.id, Bar.timeframe == INTERVAL)
                )
                bars = count_res.scalars().all()
                print(f"Bars stored for {sym} ({INTERVAL}): {len(bars)}")
                return
            except Exception as e:
                last_err = e
                # if failed, try fallback alias
                continue
        print(f"Failed to fetch any of: {', '.join(alias_list(logical_symbol))}  -> {last_err!r}")

async def main():
    await ensure_schema()
    # loop through symbols one by one
    for sym in SYMBOLS:
        await fetch_one_symbol(sym)

if __name__ == "__main__":
    asyncio.run(main())
