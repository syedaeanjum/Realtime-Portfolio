from typing import Iterable, Tuple

# input: sequence of (timestamp_ms, equity)
# output: (max_drawdown_abs, max_drawdown_pct, peak_ts, trough_ts)
def max_drawdown(points: Iterable[Tuple[int, float]]):
    # single pass: track running peak and worst drop
    peak_equity = None
    peak_ts = None
    worst_dd_abs = 0.0
    worst_dd_pct = 0.0
    trough_ts = None

    for ts, eq in points:
        if peak_equity is None or eq > peak_equity:
            peak_equity = eq
            peak_ts = ts
            continue

        # drawdown from last peak
        dd_abs = peak_equity - eq
        if peak_equity and peak_equity != 0:
            dd_pct = dd_abs / peak_equity
        else:
            dd_pct = 0.0

        if dd_abs > worst_dd_abs:
            worst_dd_abs = dd_abs
            worst_dd_pct = dd_pct
            trough_ts = ts

    return worst_dd_abs, worst_dd_pct, peak_ts, trough_ts
