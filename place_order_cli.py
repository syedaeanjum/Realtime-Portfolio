import asyncio

from app.db import SessionLocal, engine, Base
from app.orders import apply_order

# helper: ask for input, convert to given type (int/float/etc)
def ask(prompt: str, cast):
    while True:
        val = input(prompt).strip()
        try:
            return cast(val)
        except Exception:
            print("Invalid input, try again.")

# helper: ask for choice (e.g. buy/sell) until user types a valid one
def ask_choice(prompt: str, choices):
    choices_lower = [c.lower() for c in choices]
    while True:
        val = input(prompt).strip().lower()
        if val in choices_lower:
            return val
        print(f"Please enter one of: {', '.join(choices)}")

# make sure DB + tables exist
async def ensure_schema():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# prompt user for one order and apply it
async def place_one_order():
    print("\n--- New Order ---")
    symbol = input("Symbol (e.g., BTCUSDT): ").strip().upper()
    side   = ask_choice("Side (buy/sell): ", ["buy", "sell"])
    qty    = ask("Quantity (e.g., 0.01): ", float)
    price  = ask("Price (e.g., 50000): ", float)
    fee    = ask("Fee (default 0): ", float)

    async with SessionLocal() as s:
        # call the order engine
        realized, new_qty, new_avg, cash_delta = await apply_order(
            s, symbol, side, qty, price, fee=fee
        )
        # print results
        print(f"\n✔ Order applied for {symbol}")
        print(f"   side={side} qty={qty} price={price} fee={fee}")
        print(f"   realized={realized:.6f} new_qty={new_qty:.6f} new_avg={new_avg:.6f} cash_delta={cash_delta:.6f}\n")

# loop until user quits
async def main():
    await ensure_schema()
    print("Interactive Order CLI (Ctrl+C to quit)\n")
    while True:
        try:
            await place_one_order()
            again = input("Place another? (y/n): ").strip().lower()
            if again not in ("y", "yes"):
                break
        except KeyboardInterrupt:
            print("\nBye!")  # graceful exit
            break
        except Exception as e:
            print(f"Error: {e!r}\n")

if __name__ == "__main__":
    asyncio.run(main())
