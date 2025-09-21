import asyncio
from app.db import SessionLocal, engine, Base
from app.pnl import compute_pnl

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with SessionLocal() as s:
        snap = await compute_pnl(s, cash=1000.0)  # can adjust cash
        print("Equity:", round(snap["equity"], 2))
        print("Cash:", round(snap["cash"], 2))
        print("Unrealized:", round(snap["unrealized"], 2))
        print("Exposure:", round(snap["exposure"], 2))
        print("By symbol:", snap["by_symbol"])

if __name__ == "__main__":
    asyncio.run(main())
