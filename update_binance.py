import asyncio
from typing import Iterable, List

from sqlalchemy import select, func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db import SessionLocal, engine, Base
from app.models import Symbol, Bar
from app.clients.binance import get_klines_from

# --- config ---
SYMBOLS = ["BTCUSDT", "ETHUSDT", "DOGEUSDT"]  # pairs to update
INTERVAL = "1m"                                # candle size
BATCH_LIMIT = 900                              # candles per API call (<= 1000)
RUN_FOREVER = False                             # loop every 60s if True
SLEEP_SECS = 60                                # delay between rounds

def alias_list(s: str) -> List[str]:
    # fallback alias (USDT -> USD)
    return [s, s.replace("USDT", "USD")] if s.endswith("USDT") else [s]

def rows_from_klines(symbol_id: int, interval: str, klines: Iterable[list]):
    # convert kline arrays -> dicts for Bar model
    # layout: [openTime, open, high, low, close, volume, closeTime, ...]
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
    # create tables if missing
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def last_ts_ms(session, symbol_id: int, interval: str) -> int:
    # last saved bar timestamp (ms)
    q = select(func.max(Bar.ts)).where(Bar.symbol_id == symbol_id, Bar.timeframe == interval)
    res = await session.execute(q)
    m = res.scalar_one()
    return int(m) if m is not None else 0

async def upsert_bars(session, rows):
    # insert rows, skip duplicates by (symbol_id, ts, timeframe)
    data = list(rows)
    if not data:
        return 0
    stmt = sqlite_insert(Bar).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=["symbol_id", "ts", "timeframe"])
    await session.execute(stmt)
    await session.commit()
    return len(data)

async def update_one_symbol(logical_symbol: str):
    # one-symbol update (own DB session)
    async with SessionLocal() as s:
        # find or create symbol row
        db_sym = None
        for sym in alias_list(logical_symbol):
            res = await s.execute(select(Symbol).where(Symbol.symbol == sym))
            db_sym = res.scalar_one_or_none()
            if db_sym:
                break
        if db_sym is None:
            db_sym = Symbol(symbol=logical_symbol, asset_class="crypto")
            s.add(db_sym)
            await s.commit()
            await s.refresh(db_sym)

        # starting point
        start = await last_ts_ms(s, db_sym.id, INTERVAL)
        if start == 0:
            from time import time
            # bootstrap ~12h if no history yet
            start = int((time() - 12 * 3600) * 1000)

        total = 0
        cursor = start
        while True:
            kl = []
            # try USDT first, then USD
            for sym in alias_list(db_sym.symbol):
                try:
                    kl = await get_klines_from(sym, INTERVAL, start_ms=cursor, limit=BATCH_LIMIT)
                    if kl:
                        break
                except Exception:
                    continue
            if not kl:
                break

            # save rows and advance
            inserted = await upsert_bars(s, rows_from_klines(db_sym.id, INTERVAL, kl))
            total += inserted
            last_open = int(kl[-1][0])
            cursor = last_open + 1

            # stop if fewer than requested or no progress
            if inserted == 0 or len(kl) < BATCH_LIMIT:
                break

        print(f"Updated {db_sym.symbol} ({INTERVAL}): +{total} rows")

async def run_once():
    # one full pass over all symbols
    await ensure_schema()
    for sym in SYMBOLS:
        try:
            await update_one_symbol(sym)
        except Exception as e:
            # keep going even if one symbol fails
            print(f"Update error for {sym}: {e!r}")

async def main():
    # run once or in a loop
    if not RUN_FOREVER:
        await run_once()
        return

    while True:
        await run_once()
        await asyncio.sleep(SLEEP_SECS)

if __name__ == "__main__":
    asyncio.run(main())
