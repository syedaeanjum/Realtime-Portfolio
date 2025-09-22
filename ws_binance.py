import asyncio
import json
import websockets

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db import engine, Base, SessionLocal
from app.models import Symbol, Bar
from app.config import SYMBOLS, INTERVAL
from app.logging import logger  # loguru (rotating file logs)

# ---- helpers -----------------------------------------------------

def stream_name(sym: str, interval: str) -> str:
    # e.g. "btcusdt@kline_1m"
    return f"{sym.lower()}@kline_{interval}"

def build_ws_url(symbols, interval) -> str:
    # multi-stream URL for binance.us
    streams = "/".join(stream_name(s, interval) for s in symbols)
    return f"wss://stream.binance.us:9443/stream?streams={streams}"

async def ensure_schema():
    # create tables if missing
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def ensure_symbols(session, symbols):
    # make sure each text symbol has a row; return map symbol->id
    out = {}
    for s in symbols:
        row = (await session.execute(select(Symbol).where(Symbol.symbol == s))).scalar_one_or_none()
        if row is None:
            row = Symbol(symbol=s, asset_class="crypto")
            session.add(row)
            await session.commit()
            await session.refresh(row)
        out[s] = row.id
    return out

async def upsert_closed_kline(session, symbol_id: int, k: dict):
    # only write when candle is CLOSED (k["x"] == True)
    if not k.get("x"):
        return 0
    data = {
        "symbol_id": symbol_id,
        "ts": int(k["t"]),           # open time (ms)
        "open": float(k["o"]),
        "high": float(k["h"]),
        "low":  float(k["l"]),
        "close":float(k["c"]),
        "volume":float(k["v"]),
        "timeframe": k["i"],         # interval string, e.g. "1m"
    }
    stmt = sqlite_insert(Bar).values([data]).on_conflict_do_nothing(
        index_elements=["symbol_id", "ts", "timeframe"]
    )
    await session.execute(stmt)
    await session.commit()
    return 1

# ---- main WS loop ------------------------------------------------

async def run_ws():
    await ensure_schema()
    async with SessionLocal() as session:
        sym_id = await ensure_symbols(session, SYMBOLS)  # map "BTCUSDT" -> 1, etc.

    url = build_ws_url(SYMBOLS, INTERVAL)
    logger.info(f"Connecting to {url}")

    # reconnect loop with simple backoff
    backoff = 1
    while True:
        try:
            async with SessionLocal() as session:
                async with websockets.connect(
                    url,
                    ping_interval=20,  # keepalive
                    ping_timeout=20,
                    max_size=2**22,    # allow large frames
                ) as ws:
                    logger.info("WebSocket connected")
                    backoff = 1  # reset backoff on success
                    while True:
                        msg = await ws.recv()
                        obj = json.loads(msg)

                        # WS payload shape: {"stream": "...", "data": {...}}
                        d = obj.get("data") or {}
                        k = d.get("k") or {}
                        sym = d.get("s")
                        if not sym or not k:
                            continue

                        # write bar on candle close
                        sid = sym_id.get(sym)
                        if sid is None:
                            # in case a new symbol appears (unlikely if fixed SYMBOLS)
                            async with SessionLocal() as s:
                                tmp = await ensure_symbols(s, [sym])
                                sid = tmp[sym]
                                sym_id[sym] = sid

                        wrote = await upsert_closed_kline(session, sid, k)
                        if wrote:
                            logger.info(f"bar closed: {sym} {k['i']} ts={k['t']} close={k['c']}")
        except asyncio.CancelledError:
            raise
        except KeyboardInterrupt:
            logger.info("WebSocket interrupted by user")
            break
        except Exception as e:
            logger.exception(f"WebSocket error: {e!r}; reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # cap backoff at 60s

async def main():
    await run_ws()

if __name__ == "__main__":
    asyncio.run(main())
