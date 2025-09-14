import asyncio
from sqlalchemy import select
from app.db import SessionLocal, engine, Base
from app.models import Symbol, Position
from app.pnl import compute_pnl

# set True once if you want to seed demo positions
SEED = True  

async def main():
    # make sure tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with SessionLocal() as s:
        if SEED:
            # fetch all symbols
            syms = (await s.execute(select(Symbol))).scalars().all()
            by_sym = {x.symbol: x for x in syms}

            # demo positions
            for ticker, qty, avg in [
                ("BTCUSDT", 0.01, 50000.0),
                ("ETHUSDT", 0.2, 2500.0),
                ("DOGEUSDT", 100, 0.1),
            ]:
                if ticker in by_sym:
                    sid = by_sym[ticker].id
                    # clear existing
                    await s.execute(Position.__table__.delete().where(Position.symbol_id == sid))
                    s.add(Position(symbol_id=sid, qty=qty, avg_price=avg))
            await s.commit()

        snap = await compute_pnl(s, cash=1000.0)
        print("Equity:", round(snap["equity"], 2))
        print("Cash:", round(snap["cash"], 2))
        print("Unrealized:", round(snap["unrealized"], 2))
        print("Exposure:", round(snap["exposure"], 2))
        print("By symbol:", snap["by_symbol"])

if __name__ == "__main__":
    asyncio.run(main())
