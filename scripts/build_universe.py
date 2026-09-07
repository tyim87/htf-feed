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

import concurrent.futures as cf
import datetime as _dt
import io
import os
import random
import sys
import time
import urllib.request

import numpy as np
import pandas as pd
import yfinance as yf


def _daily_seed() -> int:
    """Stable within a run, different each day - reproducible but not fixed."""
    return int(_dt.date.today().strftime("%Y%m%d"))


OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

# ---- pre-filters. Deliberately loose: this stage only decides what is worth
# ---- shipping to the screen, which does the real geometry work.
MIN_PRICE = 1.00
MIN_DOLLAR_VOL = 2_000_000      # 20-day average
MIN_ADR = 3.5                   # %, the screen's own floor is 4.0
MIN_RUN_90D = 25.0              # % - needs to have moved to have a pole
MAX_ROWS = 800                  # cap on what we ship

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
    """
    Batch-download 6 months of daily bars. Failures are skipped, not fatal.

    The list is SHUFFLED first. Yahoo throttles part-way through a long run, and
    with the symbol directory in alphabetical order that would silently cost the
    same names every day - everything from roughly S to Z. Shuffling makes any
    loss random instead of systematic, so no region of the alphabet is
    permanently invisible to the screen.
    """
    tickers = list(tickers)
    random.Random(_daily_seed()).shuffle(tickers)
    got: dict[str, pd.DataFrame] = {}
    for i in range(0, len(tickers), CHUNK):
        batch = tickers[i:i + CHUNK]
        try:
            raw = yf.download(batch, period="2y", interval="1d",
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


def sector_map(tickers: list[str]) -> dict[str, tuple[str, str, str]]:
    """
    Sector/industry for the theme layer. Best-effort - never fatal.

    Threaded, because this was a sequential loop of up to 800 HTTP calls: slow
    enough to risk the job timing out, and if Yahoo started throttling part-way
    the names later in the list lost their sector - which, with an alphabetical
    list, meant the same names every day landed in "Unclassified" and were
    invisible to the theme layer. Shuffled for the same reason as download().
    """
    order = list(tickers)
    random.Random(_daily_seed()).shuffle(order)
    out: dict[str, tuple[str, str, str]] = {t: ("", "", "") for t in tickers}

    def one(t):
        for attempt in range(2):
            try:
                info = yf.Ticker(t).get_info()
                # The business summary is the single best theme signal -
                # names and industry labels routinely hide what a company
                # actually sells (Bakkt is crypto, filed as "Software -
                # Infrastructure"). Truncated to keep the digest small.
                summary = (info.get("longBusinessSummary") or "")[:400]
                summary = " ".join(summary.split())
                return t, (info.get("sector") or "",
                           info.get("industry") or "", summary)
            except Exception:                          # noqa: BLE001
                time.sleep(0.5 * (attempt + 1))
        return t, ("", "", "")

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for t, val in ex.map(one, order):
            out[t] = val

    missing = sum(1 for v in out.values() if not v[0])
    print("  sector/industry: %d of %d resolved" % (len(out) - missing, len(out)),
          flush=True)
    return out


def main() -> int:
    os.makedirs(OUT, exist_ok=True)

    print("fetching symbol directory ...", flush=True)
    syms = nasdaq_symbol_directory()
    print("  %d symbols" % len(syms), flush=True)

    tickers = syms["ticker"].tolist()
    # Shuffle BEFORE the smoke-test slice. Slicing first made every smoke
    # run scan the same 150 alphabetically-first symbols - which is exactly
    # how a full-market screen ends up looking like it only knows tickers
    # beginning with A.
    random.Random(_daily_seed()).shuffle(tickers)
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

    # Rank for the shipping cut. run_90d carries a quarter of the weight because
    # this decides which names the screen is even allowed to look at, and a
    # stock deep in a tight flag has a POOR recent return by construction -
    # that is what a flag is. Ranking on trailing returns alone would drop the
    # best setups before the geometry engine ever saw them.
    def pr(col):
        return uni[col].replace("", np.nan).astype(float).rank(pct=True)

    uni["rs"] = (pr("ret_1m") * 0.30 + pr("ret_3m") * 0.25
                 + pr("ret_6m") * 0.20 + pr("run_90d") * 0.25)
    uni = uni.sort_values("rs", ascending=False).head(MAX_ROWS)

    print("enriching %d survivors with sector/industry ..." % len(uni), flush=True)
    sm = sector_map(uni["ticker"].tolist())
    uni["sector"] = uni["ticker"].map(lambda t: sm.get(t, ("", "", ""))[0])
    uni["industry"] = uni["ticker"].map(lambda t: sm.get(t, ("", "", ""))[1])
    uni["summary"] = uni["ticker"].map(lambda t: sm.get(t, ("", "", ""))[2])
    uni = uni.merge(syms, on="ticker", how="left")

    cols = ["ticker", "name", "sector", "industry", "price", "adr", "atr_pct",
            "dollar_vol", "ret_1m", "ret_3m", "ret_6m", "run_90d",
            "from_52w_high", "rs", "bars", "last_date", "summary"]
    uni[cols].to_csv(os.path.join(OUT, "universe.csv"), index=False)

    keep = set(uni["ticker"])

    def frame(t, df):
        df = df.reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        dcol = "date" if "date" in df.columns else df.columns[0]
        return pd.DataFrame({
            "ticker": t,
            "date": pd.to_datetime(df[dcol]).dt.strftime("%Y-%m-%d"),
            "open": df["open"].round(4),
            "high": df["high"].round(4),
            "low": df["low"].round(4),
            "close": df["close"].round(4),
            "volume": df["volume"].astype("int64"),
        })

    daily, weekly = [], []
    for t in keep:
        full = bars[t]
        daily.append(frame(t, full.tail(130)))
        # Stine's laws are measured on WEEKLY bars and need ~52 of them. A
        # 130-bar daily file resamples to only ~27 weeks, which made the
        # Super Laws component silently score noise instead of saying so.
        wk = full.resample("W-FRI").agg({"Open": "first", "High": "max",
                                         "Low": "min", "Close": "last",
                                         "Volume": "sum"}).dropna()
        weekly.append(frame(t, wk.tail(110)))

    # gzipped: uncompressed these commit ~10MB a day, which becomes
    # gigabytes of git history inside a year.
    pd.concat(daily, ignore_index=True).to_csv(
        os.path.join(OUT, "bars.csv.gz"), index=False, compression="gzip")
    pd.concat(weekly, ignore_index=True).to_csv(
        os.path.join(OUT, "weekly.csv.gz"), index=False, compression="gzip")

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
