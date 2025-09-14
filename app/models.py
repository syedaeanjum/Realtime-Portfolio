from sqlalchemy import Integer, Float, BigInteger, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base

class Symbol(Base):
    __tablename__ = "symbols"   # name of the table

    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # primary key, unique row id
    symbol: Mapped[str] = mapped_column(String(50), unique=True, index=True)  # ticker, must be unique
    asset_class: Mapped[str] = mapped_column(String(20))  # type of asset: crypto, equity, fx


class Bar(Base):
    __tablename__ = "bars"  # OHLCV candles

    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # unique row id
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), index=True)  # link to Symbol
    ts: Mapped[int] = mapped_column(BigInteger, index=True)  # open time (ms since epoch)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low:  Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    timeframe: Mapped[str] = mapped_column(String(10))  # e.g. "1m"

    __table_args__ = (
        UniqueConstraint("symbol_id", "ts", "timeframe", name="uq_bar_symbol_ts_tf"),
    )
    
class Position(Base):
    __tablename__ = "positions"  # current holdings

    id: Mapped[int] = mapped_column(Integer, primary_key=True)      # row id
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"), index=True)
    qty: Mapped[float] = mapped_column(Float, default=0.0)          # units
    avg_price: Mapped[float] = mapped_column(Float, default=0.0)    # entry avg

    __table_args__ = (UniqueConstraint("symbol_id", name="uq_pos_symbol"),)    