def a():
    if x:
        return 1
    return 2
#!/usr/bin/env python3
"""
Builds the daily HTF universe digest.

Runs inside a GitHub Action, where the network is unrestricted, and writes two
artifacts the screen reads back over raw.githubusercontent.com:

    data/universe.csv   one row per surviving candidate, with the metrics the
                        screen ranks on (RS, ADR, ATR, dollar volume, 52w
                        distance, sector, industry)
    data/bars.csv       6 months of daily OHLCV for those survivors, long form

Why this shape: the screen's container can curl raw.githubusercontent.com at
full fidelity but cannot reach Yahoo, Nasdaq, Stooq or any data API. Doing the
heavy scan here and shipping a compact digest is the only free architecture
that gives a true full-market scan.

Everything here is free and keyless: the ticker list comes from Nasdaq Trader's
public symbol directory, the bars from yfinance.
"""
from __future__ import annotations

import io
import os
import sys
import time
import urllib.request

import numpy as np
import pandas as pd
import yfinance as yf

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

# ---- pre-filters. Deliberately loose: this stage only decides what is worth
# ---- shipping to the screen, which does the real geometry work.
MIN_PRICE = 1.00
MIN_DOLLAR_VOL = 2_000_000      # 20-day average
MIN_ADR = 3.5                   # %, the screen's own floor is 4.0
MIN_RUN_90D = 25.0              # % - needs to have moved to have a pole
MAX_ROWS = 600                  # cap on what we ship

CHUNK = 150
SLEEP = 1.5


def nasdaq_symbol_directory() -> pd.DataFrame:
    """Free, keyless, authoritative US listed-symbol directory."""
    frames = []
    for fname, is_nasdaq in (("nasdaqlisted.txt", True), ("otherlisted.txt", False)):
        url = "https://www.nasdaqtrader.com/dynamic/SymDir/" + fname
        raw = urllib.request.urlopen(url, timeout=90).read().decode("utf-8", "replace")
        df = pd.read_csv(io.StringIO(raw), sep="|")
        df = df[~df.iloc[:, 0].astype(str).str.startswith("File Creation Time")]
        sym = "Symbol" if is_nasdaq else "ACT Symbol"
        df = df.rename(columns={sym: "ticker", "Security Name": "name"})
        if "ETF" in df.columns:
            df = df[df["ETF"] != "Y"]
        if "Test Issue" in df.columns:
            df = df[df["Test Issue"] != "Y"]
        frames.append(df[["ticker", "name"]])
    out = pd.concat(frames, ignore_index=True).dropna(subset=["ticker"])
    out["ticker"] = out["ticker"].astype(str).str.strip()
    # drop warrants, units, preferreds and anything with a non-plain symbol
    out = out[out["ticker"].str.fullmatch(r"[A-Z]{1,5}")]
    return out.drop_duplicates("ticker").reset_index(drop=True)


def download(tickers: list[str]) -> dict[str, pd.DataFrame]:
    """Batch-download 6 months of daily bars. Failures are skipped, not fatal."""
    got: dict[str, pd.DataFrame] = {}
    for i in range(0, len(tickers), CHUNK):
        batch = tickers[i:i + CHUNK]
        try:
            raw = yf.download(batch, period="9mo", interval="1d",
                              group_by="ticker", auto_adjust=True,
                              threads=True, progress=False)
        except Exception as exc:                       # noqa: BLE001
            print("  chunk %d failed: %s" % (i, exc), file=sys.stderr)
            time.sleep(SLEEP * 4)
            continue
        for t in batch:
            try:
                df = raw[t] if len(batch) > 1 else raw
            except (KeyError, TypeError):
                continue
            df = df.dropna()
            if len(df) >= 60:
                got[t] = df
        print("  %d/%d fetched, %d usable" % (min(i + CHUNK, len(tickers)),
                                              len(tickers), len(got)), flush=True)
        time.sleep(SLEEP)
    return got


def metrics(df: pd.DataFrame) -> dict | None:
    """The numbers the screen ranks on. Cheap enough to run over everything."""
    h, l = df["High"].to_numpy(), df["Low"].to_numpy()
    c, v = df["Close"].to_numpy(), df["Volume"].to_numpy()
    n = len(c)
    if n < 60 or c[-1] <= 0:
        return None

    px = float(c[-1])
    dv = float(np.mean(c[-20:] * v[-20:]))

    with np.errstate(divide="ignore", invalid="ignore"):
        rng = np.where(l[-20:] > 0, h[-20:] / l[-20:] - 1.0, np.nan)
    adr = float(np.nanmean(rng) * 100)

    tr = np.maximum.reduce([
        h[1:] - l[1:],
        np.abs(h[1:] - c[:-1]),
        np.abs(l[1:] - c[:-1]),
    ])
    atr_pct = float(np.mean(tr[-14:]) / px * 100)

    def ret(days):
        if n <= days or c[-days - 1] <= 0:
            return np.nan
        return float(c[-1] / c[-days - 1] - 1.0) * 100

    # biggest low-to-high run inside the last 90 sessions - the pole proxy
    w = min(90, n)
    lows = np.minimum.accumulate(l[-w:])
    run_90 = float(np.max((h[-w:] / np.maximum(lows, 1e-9) - 1.0)) * 100)

    hi52 = float(np.max(h[-min(252, n):]))
    return dict(
        price=round(px, 4),
        dollar_vol=round(dv, 0),
        adr=round(adr, 2),
        atr_pct=round(atr_pct, 2),
        ret_1m=round(ret(21), 2) if not np.isnan(ret(21)) else "",
        ret_3m=round(ret(63), 2) if not np.isnan(ret(63)) else "",
        ret_6m=round(ret(126), 2) if not np.isnan(ret(126)) else "",
        run_90d=round(run_90, 2),
        from_52w_high=round((px / hi52 - 1.0) * 100, 2) if hi52 > 0 else "",
        bars=n,
        last_date=str(df.index[-1].date()),
    )


def sector_map(tickers: list[str]) -> dict[str, tuple[str, str]]:
    """Sector/industry for the theme layer. Best-effort - never fatal."""
    out: dict[str, tuple[str, str]] = {}
    for t in tickers:
        try:
            info = yf.Ticker(t).get_info()
            out[t] = (info.get("sector") or "", info.get("industry") or "")
        except Exception:                              # noqa: BLE001
            out[t] = ("", "")
    return out


def main() -> int:
    os.makedirs(OUT, exist_ok=True)

    print("fetching symbol directory ...", flush=True)
    syms = nasdaq_symbol_directory()
    print("  %d symbols" % len(syms), flush=True)

    tickers = syms["ticker"].tolist()
    if os.environ.get("SMOKE_TEST"):
        tickers = tickers[:CHUNK]

    print("downloading bars ...", flush=True)
    bars = download(tickers)
    print("  %d with usable history" % len(bars), flush=True)

    rows = []
    for t, df in bars.items():
        m = metrics(df)
        if not m:
            continue
        if (m["price"] < MIN_PRICE or m["dollar_vol"] < MIN_DOLLAR_VOL
                or m["adr"] < MIN_ADR or m["run_90d"] < MIN_RUN_90D):
            continue
        m["ticker"] = t
        rows.append(m)

    uni = pd.DataFrame(rows)
    if uni.empty:
        print("no survivors - writing empty digest", file=sys.stderr)
        uni.to_csv(os.path.join(OUT, "universe.csv"), index=False)
        return 1

    # rank by a blunt momentum composite; the screen does the real ranking
    uni["rs"] = (uni["ret_1m"].replace("", np.nan).astype(float).rank(pct=True) * 0.5
                 + uni["ret_3m"].replace("", np.nan).astype(float).rank(pct=True) * 0.3
                 + uni["ret_6m"].replace("", np.nan).astype(float).rank(pct=True) * 0.2)
    uni = uni.sort_values("rs", ascending=False).head(MAX_ROWS)

    print("enriching %d survivors with sector/industry ..." % len(uni), flush=True)
    sm = sector_map(uni["ticker"].tolist())
    uni["sector"] = uni["ticker"].map(lambda t: sm.get(t, ("", ""))[0])
    uni["industry"] = uni["ticker"].map(lambda t: sm.get(t, ("", ""))[1])
    uni = uni.merge(syms, on="ticker", how="left")

    cols = ["ticker", "name", "sector", "industry", "price", "adr", "atr_pct",
            "dollar_vol", "ret_1m", "ret_3m", "ret_6m", "run_90d",
            "from_52w_high", "rs", "bars", "last_date"]
    uni[cols].to_csv(os.path.join(OUT, "universe.csv"), index=False)

    keep = set(uni["ticker"])
    long = []
    for t in keep:
        df = bars[t].tail(130).reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        dcol = "date" if "date" in df.columns else df.columns[0]
        long.append(pd.DataFrame({
            "ticker": t,
            "date": pd.to_datetime(df[dcol]).dt.strftime("%Y-%m-%d"),
            "open": df["open"].round(4),
            "high": df["high"].round(4),
            "low": df["low"].round(4),
            "close": df["close"].round(4),
            "volume": df["volume"].astype("int64"),
        }))
    pd.concat(long, ignore_index=True).to_csv(
        os.path.join(OUT, "bars.csv"), index=False)

    with open(os.path.join(OUT, "STATUS.txt"), "w") as fh:
        fh.write("generated_utc=%s\n" % time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        fh.write("symbols_scanned=%d\n" % len(tickers))
        fh.write("with_history=%d\n" % len(bars))
        fh.write("survivors=%d\n" % len(uni))
        fh.write("latest_bar=%s\n" % uni["last_date"].max())

    print("wrote universe.csv (%d rows) and bars.csv" % len(uni))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
