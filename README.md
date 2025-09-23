# Realtime Portfolio

Async Python project for **real-time portfolio tracking** with live market data.  
It ingests OHLC bars from Binance (via REST + WebSocket), simulates positions with buy/sell orders, computes P&L and risk metrics (equity, exposure, drawdown), and logs snapshots for analysis.  

This project is designed to mimic the core components of a trading system:  
- Connecting to live market data  
- Storing and updating financial time-series in a database  
- Managing portfolio state through simulated orders  
- Calculating key performance and risk metrics in real-time  

It demonstrates API integration, database design, state management, and financial computation. These are essential skills for building scalable financial applications and real-time data pipelines.

---

## What It Does

- **Data ingestion**  
  - Pulls historical OHLCV bars from Binance REST API (backfill).  
  - Streams live 1-minute candles from Binance WebSocket (real-time updates).  
  - Stores all bars in SQLite (default) or PostgreSQL.  

- **Portfolio management**  
  - Tracks symbols (`BTCUSDT`, `ETHUSDT`, `DOGEUSDT` by default).  
  - Supports simulated buy/sell orders via CLI.  
  - Updates positions with average cost, realized/unrealized PnL.  

- **Risk & performance metrics**  
  - Computes equity, cash balance, exposure, and unrealized PnL.  
  - Stores time-based snapshots of portfolio state.  
  - Calculates max drawdown across snapshot history.  

- **Utilities**  
  - Loguru-powered logging with rotating log files.  
  - CLI scripts to ingest data, place orders, view snapshots, and compute PnL.  
  - Modular design: easily extend to new symbols, APIs, or databases.  

---

## System Overview

```text
    +-----------+
    |  Binance  |
    | REST + WS |
    +-----+-----+
          |
 (bars, ticks, klines)
          |
          v
    +-----+------+
    |  Database  |
    |   SQLite   |
    +-----+------+
          |
   +------+------+
   |             |
   v             v
Positions     Snapshots
 (orders)    (equity curve)
   |             |
   +------+------+
          v
     +----+----+
     |  PnL /  |
     |  Risk   |
     +---------+
```
---

## Quickstart

### 1. Clone and setup

```
git clone git@github.com:syedaeanjum/Realtime-Portfolio.git
cd Realtime-Portfolio

# create and activate venv
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1

# install dependencies
pip install -r requirements.txt

```

### 2. Configure environment
Create a .env file at the project root:
```
DATABASE_URL=sqlite+aiosqlite:///./market.db
SYMBOLS=BTCUSDT,ETHUSDT,DOGEUSDT
INTERVAL=1m
CASH=1000
```

### 3. Ingest historical data
```
# one-time bulk backfill
python ingest_binance.py

# keep data fresh (REST loop)
python update_binance.py
```

### 4. Stream live bars (WebSocket)

```
# writes closed candles in real-time (new bar every minute)
python ws_binance.py
```

### 5. Portfolio & Risk

```
# compute current P&L
python pnl_now.py

# take and store a snapshot
python snapshot_now.py

# view last snapshots + max drawdown
python show_snapshots.py
```

### 6. Place simulated orders
```
# interactive order entry (prompts for symbol/side/qty/price/fee)
python place_order_cli.py
```

---

# Notes

- Binance US WebSocket is used (REST fallback if blocked).

- SQLite is default; swap to PostgreSQL by updating DATABASE_URL in .env.

- Orders are simulated only (not sent to any exchange).

- Project is modular and extendable (e.g., add Alpaca or Yahoo Finance APIs).

