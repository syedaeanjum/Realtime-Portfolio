from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from app import config  # holds DATABASE_URL

Base = declarative_base()  # base class for all models

# async database engine (uses DATABASE_URL from config.py)
engine = create_async_engine(config.DATABASE_URL, echo=False, future=True)

# session factory for making DB sessions
SessionLocal = async_sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
