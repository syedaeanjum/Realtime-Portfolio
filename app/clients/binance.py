import httpx
from typing import List, Optional, Dict, Any 

# base URLs to try (US first, fallback to .com)
BASES: List[str] = [
    "https://api.binance.us",
    "https://api.binance.com",
]

HEADERS: Dict[str, str] = {"User-Agent": "realtime-portfolio/0.1"}
TIMEOUT = 20  # seconds

# ---------------------------
# get_klines = recent candles
# ---------------------------
# interval: 1m, 3m, 5m, 15m, 1h, 4h, 1d, etc.

async def get_klines(symbol: str, interval: str = "1m", limit: int = 900):
    """
    Return list of klines:
    [ [openTime, open, high, low, close, volume, closeTime, ...], ... ]
    """
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    last_exc: Optional[Exception] = None

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
        for base in BASES:          # try US, then COM
            for _ in range(2):      # retry twice per base
                try:
                    r = await client.get(f"{base}/api/v3/klines", params=params)
                    r.raise_for_status()
                    return r.json()
                except httpx.HTTPStatusError as e:
                    last_exc = e
                    code = getattr(e.response, "status_code", None)
                    if code in (451, 403):  # legal block -> try next base
                        break
                except (httpx.ReadTimeout, httpx.ConnectError) as e:
                    last_exc = e  # retry once
                except Exception as e:
                    last_exc = e
                    break

    if last_exc:
        raise last_exc
    raise RuntimeError("Failed to fetch klines: unknown error")


# --------------------------------------
# get_klines_from = candles since start
# --------------------------------------
# start_ms = epoch time in ms (openTime >= start_ms)

async def get_klines_from(symbol: str, interval: str, start_ms: int, limit: int = 1000):
    """
    Return klines starting from start_ms (inclusive).
    Same layout as get_klines().
    """
    params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": start_ms}
    last_exc: Optional[Exception] = None

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
        for base in BASES:          # try both bases
            for _ in range(2):      # retry twice
                try:
                    r = await client.get(f"{base}/api/v3/klines", params=params)
                    r.raise_for_status()
                    return r.json()
                except httpx.HTTPStatusError as e:
                    last_exc = e
                    code = getattr(e.response, "status_code", None)
                    if code in (451, 403):  # block -> try next base
                        break
                except (httpx.ReadTimeout, httpx.ConnectError) as e:
                    last_exc = e
                except Exception as e:
                    last_exc = e
                    break

    if last_exc:
        raise last_exc
    return []  # empty if nothing new
