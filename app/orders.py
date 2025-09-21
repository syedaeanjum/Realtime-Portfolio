from typing import Tuple
from sqlalchemy import select
from app.models import Symbol, Position

# get or create a Symbol row
async def _get_or_create_symbol(session, ticker: str) -> Symbol:
    sym = (await session.execute(select(Symbol).where(Symbol.symbol == ticker))).scalar_one_or_none()
    if sym is None:
        sym = Symbol(symbol=ticker, asset_class="crypto")
        session.add(sym)
        await session.commit()
        await session.refresh(sym)
    return sym

# get or create a Position row
async def _get_or_create_position(session, symbol_id: int) -> Position:
    pos = (await session.execute(select(Position).where(Position.symbol_id == symbol_id))).scalar_one_or_none()
    if pos is None:
        pos = Position(symbol_id=symbol_id, qty=0.0, avg_price=0.0)
        session.add(pos)
        await session.commit()
        await session.refresh(pos)
    return pos

# -------------------------------------------------------------
# apply_order: updates Position like a real trade
# returns (realized_pnl, new_qty, new_avg, cash_delta)
# - side: "buy" or "sell"
# - qty:  > 0 (units)
# - price: > 0 (fill price)
# - fee: trading fee in cash units (deducted)
# conventions:
#   Position.qty > 0 → long, < 0 → short
#   avg_price is entry ref
# cash_delta:
#   buy  => -qty*price - fee
#   sell => +qty*price - fee
# realized PnL:
#   closing part only (kept qty stays at avg_price)
# -------------------------------------------------------------
async def apply_order(session, ticker: str, side: str, qty: float, price: float, fee: float = 0.0) -> Tuple[float, float, float, float]:
    assert side in ("buy", "sell"), "side must be 'buy' or 'sell'"
    assert qty > 0 and price > 0, "qty and price must be positive"

    sym = await _get_or_create_symbol(session, ticker)
    pos = await _get_or_create_position(session, sym.id)

    old_qty = float(pos.qty)        # signed
    old_avg = float(pos.avg_price)  # entry ref
    realized = 0.0

    # signed trade qty for position math
    trade_signed = qty if side == "buy" else -qty
    new_qty = old_qty + trade_signed

    # cash effect (positive = receive, negative = pay)
    cash_delta = (-qty * price if side == "buy" else qty * price) - fee

    if old_qty == 0.0:
        # opening fresh position
        new_avg = price

    elif old_qty > 0 and new_qty >= 0:
        # staying long or reducing long
        if side == "buy":
            # add to long → weighted avg
            new_avg = (old_avg * old_qty + price * qty + fee) / (old_qty + qty)
        else:
            # sell from a long → realize on closed part
            closed = min(qty, old_qty)
            realized += (price - old_avg) * closed
            new_avg = 0.0 if new_qty == 0 else old_avg

    elif old_qty < 0 and new_qty <= 0:
        # staying short or reducing short
        if side == "sell":
            # add to short → weighted avg by absolute sizes
            old_abs = abs(old_qty)
            new_abs = old_abs + qty
            new_avg = (old_avg * old_abs + price * qty + fee) / new_abs
        else:
            # buy to cover → realize on covered part
            closed = min(qty, abs(old_qty))
            realized += (old_avg - price) * closed  # short pnl
            new_avg = 0.0 if new_qty == 0 else old_avg

    else:
        # crossing through zero (flip)
        if old_qty > 0 and new_qty < 0:
            # close long fully, remainder opens short at trade price
            realized += (price - old_avg) * old_qty
            new_avg = price
        elif old_qty < 0 and new_qty > 0:
            # close short fully, remainder opens long at trade price
            realized += (old_avg - price) * abs(old_qty)
            new_avg = price
        else:
            new_avg = price  # safety

    # optionally charge fee to realized pnl too (besides cash)
    realized -= fee

    # persist
    pos.qty = new_qty
    pos.avg_price = new_avg
    await session.commit()
    await session.refresh(pos)

    return realized, new_qty, new_avg, cash_delta
