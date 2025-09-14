import asyncio
from sqlalchemy import select

from app.db import engine, Base, SessionLocal
from app.models import Symbol

async def main():
    # create tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created ✔")

    # insert symbol if not already in table
    async with SessionLocal() as session:
        res = await session.execute(select(Symbol).where(Symbol.symbol == "BTCUSDT"))
        row = res.scalar_one_or_none()
        if row is None:
            session.add(Symbol(symbol="BTCUSDT", asset_class="crypto"))
            await session.commit()
            print("Inserted: BTCUSDT")

    # read back all symbols
    async with SessionLocal() as session:
        res = await session.execute(select(Symbol))
        symbols = res.scalars().all()
        print("Symbols:", [(s.id, s.symbol, s.asset_class) for s in symbols])

if __name__ == "__main__":
    asyncio.run(main())
