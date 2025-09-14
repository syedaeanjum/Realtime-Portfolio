from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base

class Symbol(Base):
    __tablename__ = "symbols"   # name of the table

    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # primary key, unique row id
    symbol: Mapped[str] = mapped_column(String(50), unique=True, index=True)  # ticker, must be unique
    asset_class: Mapped[str] = mapped_column(String(20))  # type of asset: crypto, equity, fx
