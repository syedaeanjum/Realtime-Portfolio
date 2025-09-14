import httpx
from typing import List, Optional  # 3.8-compatible typing

# try US first (binance.com may 451/403 in the US)
BASES: List[str] = [
    "https://api.binance.us",
    "https://api.binance.com",
]

# interval: "1m","3m","5m","15m","1h","4h","1d", etc.
async def get_klines(symbol: str, interval: str = "1m", limit: int = 500):
    """
    Return klines:
    [ [openTime, open, high, low, close, volume, closeTime, ...], ... ]
    """
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    last_exc: Optional[Exception] = None

    async with httpx.AsyncClient(timeout=20, headers={"User-Agent": "realtime-portfolio/0.1"}) as client:
        for base in BASES:
            for _ in range(2):  # small retry per base
                try:
                    r = await client.get(f"{base}/api/v3/klines", params=params)
                    r.raise_for_status()
                    return r.json()
                except httpx.HTTPStatusError as e:
                    last_exc = e
                    code = getattr(e.response, "status_code", None)
                    if code in (451, 403):  # legal/forbidden → try next base
                        break
                    # other HTTP errors → retry once, then move on
                except (httpx.ReadTimeout, httpx.ConnectError) as e:
                    last_exc = e  # retry once
                except Exception as e:
                    last_exc = e
                    break

    # exhausted all bases / retries
    if last_exc:
        raise last_exc
    raise RuntimeError("Failed to fetch klines: unknown error")
