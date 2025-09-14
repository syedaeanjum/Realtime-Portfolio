import os
# Minimal config: read DATABASE_URL from env or default to local SQLite
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./market.db")
