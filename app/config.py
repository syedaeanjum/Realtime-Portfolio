import os

try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except Exception:
    pass

# DB url (SQLite by default)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./market.db")

# comma-separated symbols → list
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,DOGEUSDT").split(",") if s.strip()]

# candle size (e.g., "1m","5m","1h")
INTERVAL = os.getenv("INTERVAL", "1m")

# starting cash for P&L view
CASH = float(os.getenv("CASH", "1000"))
