#!/usr/bin/env python3
"""
Builds the daily HTF universe digest from Kiwoom's REST API - full US market,
no 800-name cap, standard library only (no pandas / numpy / yfinance needed).

Runs on the PC whose IP is registered with Kiwoom. Writes the same files the
GitHub Action version writes, so the screen reads them unchanged:

    data/universe.csv    survivors + the metrics the screen ranks on
    data/bars.csv.gz     130 daily bars per survivor (long form)
    data/weekly.csv.gz   up to 110 weekly bars per survivor (W-FRI resample)
    data/scan.csv        EVERY symbol touched, with the pre-filter it failed
    data/STATUS.txt      run metadata

Two-stage fetch to stay inside Kiwoom's rate limit (~3 req/s):
    stage 1  one page (100 bars) per symbol        -> cheap pre-filter
    stage 2  PAGES_FULL pages for survivors only   -> full metrics + weekly

Environment (see feed-config.example.txt):
    KIWOOM_APP_KEY / KIWOOM_APP_SECRET   required
    PAGES_FULL        pages (x100 bars) for survivors, default 3 (~14 months;
                      gives 60+ weekly bars for the Super Laws). 5 = 2 years.
    MAX_ROWS          cap on survivors shipped, default 0 = no cap
    SMOKE_TEST=1      only the first 150 symbols
    WORKERS           parallel fetchers, default 3 (rate limit is global)
    ENRICH            1 (default) = try yfinance for sector/industry/summary
                      if it is installed; 0 = skip
"""
from __future__ import annotations

import concurrent.futures as cf
import csv
import datetime as dt
import gzip
import json
import math
import os
import random
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kiwoom_source as kw                              # noqa: E402

ROOT = os.path.dirname(HERE)
OUT = os.path.join(ROOT, "data")
CACHE = os.path.join(ROOT, "cache")

MIN_PRICE = 1.00
MIN_DOLLAR_VOL = 2_000_000
MIN_ADR = 3.5
MIN_RUN_90D = 25.0
MAX_ROWS = int(os.environ.get("MAX_ROWS", "0") or 0)
PAGES_FULL = int(os.environ.get("PAGES_FULL", "3") or 3)
WORKERS = int(os.environ.get("WORKERS", "3") or 3)
SMOKE = bool(os.environ.get("SMOKE_TEST"))
ENRICH = os.environ.get("ENRICH", "1") != "0"


def log(msg):
    print(time.strftime("%H:%M:%S ") + msg, flush=True)


# ------------------------------------------------------------------ metrics
def metrics(bars):
    """Same numbers as build_universe.py, without numpy."""
    n = len(bars)
    if n < 60:
        return None
    h = [b["high"] for b in bars]; l = [b["low"] for b in bars]
    c = [b["close"] for b in bars]; v = [b["volume"] for b in bars]
    px = c[-1]
    if px <= 0:
        return None
    dv = sum(c[i] * v[i] for i in range(n - 20, n)) / 20.0
    rng = [h[i] / l[i] - 1.0 for i in range(n - 20, n) if l[i] > 0]
    adr = 100.0 * sum(rng) / len(rng) if rng else 0.0
    tr = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(n - 14, n)]
    atr_pct = 100.0 * (sum(tr) / 14.0) / px

    def ret(days):
        if n <= days or c[-days - 1] <= 0:
            return ""
        return round((c[-1] / c[-days - 1] - 1.0) * 100, 2)

    w = min(90, n)
    lo, run = float("inf"), 0.0
    for i in range(n - w, n):
        lo = min(lo, l[i])
        if lo > 0:
            run = max(run, h[i] / lo - 1.0)
    hi52 = max(h[-min(252, n):])
    return dict(price=round(px, 4), dollar_vol=round(dv, 0), adr=round(adr, 2), atr_pct=round(atr_pct, 2),
                ret_1m=ret(21), ret_3m=ret(63), ret_6m=ret(126), run_90d=round(run * 100, 2),
                from_52w_high=round((px / hi52 - 1.0) * 100, 2) if hi52 > 0 else "",
                bars=n, last_date=bars[-1]["date"])


def prefilter_reason(m):
    if m is None:
        return "fewer than 60 bars"
    if m["price"] < MIN_PRICE:
        return "price < $%.2f" % MIN_PRICE
    if m["dollar_vol"] < MIN_DOLLAR_VOL:
        return "20d dollar volume < $%dM" % (MIN_DOLLAR_VOL // 1_000_000)
    if m["adr"] < MIN_ADR:
        return "ADR < %.1f%%" % MIN_ADR
    if m["run_90d"] < MIN_RUN_90D:
        return "90d run < %.0f%%" % MIN_RUN_90D
    return ""


def weekly(bars):
    """W-FRI resample: each ISO week labelled by its Friday."""
    out, cur = [], None
    for b in bars:
        d = dt.date.fromisoformat(b["date"])
        fri = d + dt.timedelta(days=4 - d.weekday())
        key = fri.isoformat()
        if cur is None or cur["date"] != key:
            if cur:
                out.append(cur)
            cur = dict(date=key, open=b["open"], high=b["high"], low=b["low"], close=b["close"], volume=b["volume"])
        else:
            cur["high"] = max(cur["high"], b["high"]); cur["low"] = min(cur["low"], b["low"])
            cur["close"] = b["close"]; cur["volume"] += b["volume"]
    if cur:
        out.append(cur)
    return out


# ------------------------------------------------------------------ fetching
def _cache_path(tag):
    os.makedirs(CACHE, exist_ok=True)
    return os.path.join(CACHE, "%s_%s.jsonl" % (tag, dt.date.today().strftime("%Y%m%d")))


def fetch_all(symbols, pages, tag):
    """{ticker: bars} for every symbol, resumable through a per-day cache so a
    crash half-way through 7,000 symbols does not restart from zero."""
    path = _cache_path(tag)
    got = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                    got[rec["t"]] = rec["b"]
                except Exception:                       # noqa: BLE001
                    pass
        if got:
            log("  %s: %d symbols restored from cache" % (tag, len(got)))
    todo = [s for s in symbols if s["ticker"] not in got]
    lock = __import__("threading").Lock()
    done = [len(got)]
    t0 = time.time()

    def one(s):
        b = kw.daily_bars(s["ticker"], s["exchange"], pages=pages)
        with lock:
            got[s["ticker"]] = b
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"t": s["ticker"], "b": b}) + "\n")
            done[0] += 1
            if done[0] % 250 == 0:
                el = time.time() - t0
                log("  %s: %d/%d  (%.1f req/s, throttled %d, errors %d)" % (
                    tag, done[0], len(symbols), kw.STATS["calls"] / max(el, 1), kw.STATS["throttled"], kw.STATS["errors"]))
        return s["ticker"]

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(one, todo))
    return got


def enrich(tickers, fallback):
    """sector/industry/summary via yfinance if it is installed; else the
    Kiwoom industry label (Korean) so the theme layer still has something."""
    out = {t: ("", fallback.get(t, ""), "") for t in tickers}
    if not ENRICH:
        return out, 0
    try:
        import yfinance as yf                           # noqa: WPS433
    except Exception:                                   # noqa: BLE001
        log("  yfinance not installed - sector/summary skipped (SIC from EDGAR still covers themes)")
        return out, 0
    order = list(tickers)
    random.Random(int(dt.date.today().strftime("%Y%m%d"))).shuffle(order)

    def one(t):
        for attempt in range(3):
            try:
                info = yf.Ticker(t).get_info()
                summary = " ".join((info.get("longBusinessSummary") or "")[:400].split())
                return t, (info.get("sector") or "", info.get("industry") or fallback.get(t, ""), summary)
            except Exception:                           # noqa: BLE001
                time.sleep(1.0 * (attempt + 1))
        return t, ("", fallback.get(t, ""), "")

    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        for t, val in ex.map(one, order):
            out[t] = val
    resolved = sum(1 for v in out.values() if v[0] or v[2])
    log("  sector/industry via yfinance: %d of %d resolved" % (resolved, len(out)))
    return out, resolved


# ------------------------------------------------------------------ writing
def write_csv(path, rows, cols, gz=False):
    opener = (lambda p: gzip.open(p, "wt", encoding="utf-8", newline="")) if gz else (lambda p: open(p, "w", encoding="utf-8", newline=""))
    with opener(path) as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def bar_rows(t, bars):
    for b in bars:
        yield dict(ticker=t, date=b["date"], open=round(b["open"], 4), high=round(b["high"], 4),
                   low=round(b["low"], 4), close=round(b["close"], 4), volume=int(b["volume"]))


def main():
    os.makedirs(OUT, exist_ok=True)
    t_start = time.time()
    log("Kiwoom token ...")
    kw.token()
    log("fetching US stock list ...")
    syms = kw.stock_list()
    log("  %d non-ETF symbols" % len(syms))
    random.Random(int(dt.date.today().strftime("%Y%m%d"))).shuffle(syms)
    if SMOKE:
        syms = syms[:150]
    names = {s["ticker"]: s["name"] for s in syms}
    ind_kr = {s["ticker"]: s["industry_kr"] for s in syms}

    log("stage 1: one page of bars for %d symbols ..." % len(syms))
    stage1 = fetch_all(syms, 1, "stage1")
    scan, survivors = [], []
    for s in syms:
        b = stage1.get(s["ticker"]) or []
        if not b:
            scan.append(dict(ticker=s["ticker"], name=s["name"], exchange=s["exchange"], rejected="no bars from Kiwoom"))
            continue
        m = metrics(b)
        why = prefilter_reason(m)
        scan.append(dict(ticker=s["ticker"], name=s["name"], exchange=s["exchange"], rejected=why,
                         price=m["price"] if m else "", dollar_vol=m["dollar_vol"] if m else "",
                         adr=m["adr"] if m else "", run_90d=m["run_90d"] if m else ""))
        if not why:
            survivors.append(s)
    with_hist = sum(1 for b in stage1.values() if len(b) >= 60)
    log("  %d with >=60 bars, %d pass the pre-filter" % (with_hist, len(survivors)))

    log("stage 2: %d pages for %d survivors ..." % (PAGES_FULL, len(survivors)))
    full = fetch_all(survivors, PAGES_FULL, "stage2") if PAGES_FULL > 1 else {s["ticker"]: stage1[s["ticker"]] for s in survivors}

    rows = []
    for s in survivors:
        b = full.get(s["ticker"]) or stage1.get(s["ticker"]) or []
        m = metrics(b)
        if not m or prefilter_reason(m):
            continue                                    # re-check on the longer history
        m["ticker"] = s["ticker"]
        rows.append(m)

    # RS rank exactly as the Action computes it (percentile ranks, blended)
    def pct_rank(col):
        vals = [(float(r[col]), i) for i, r in enumerate(rows) if r[col] != ""]
        vals.sort()
        out = {}
        k = 0
        while k < len(vals):
            j = k
            while j + 1 < len(vals) and vals[j + 1][0] == vals[k][0]:
                j += 1
            avg = (k + j + 2) / 2.0 / len(vals)         # average rank, pct=True
            for q in range(k, j + 1):
                out[vals[q][1]] = avg
            k = j + 1
        return out
    pr = {col: pct_rank(col) for col in ("ret_1m", "ret_3m", "ret_6m", "run_90d")}
    for i, r in enumerate(rows):
        parts = [(pr["ret_1m"].get(i), 0.30), (pr["ret_3m"].get(i), 0.25), (pr["ret_6m"].get(i), 0.20), (pr["run_90d"].get(i), 0.25)]
        r["rs"] = round(sum(p * w for p, w in parts if p is not None), 4) if any(p is not None for p, _ in parts) else ""
    rows.sort(key=lambda r: -(r["rs"] if r["rs"] != "" else -1))
    if MAX_ROWS:
        rows = rows[:MAX_ROWS]

    log("enriching %d survivors ..." % len(rows))
    sm, resolved = enrich([r["ticker"] for r in rows], ind_kr)
    for r in rows:
        sec, ind, summ = sm.get(r["ticker"], ("", "", ""))
        r["sector"], r["industry"], r["summary"], r["name"] = sec, ind, summ, names.get(r["ticker"], "")

    cols = ["ticker", "name", "sector", "industry", "price", "adr", "atr_pct", "dollar_vol", "ret_1m", "ret_3m",
            "ret_6m", "run_90d", "from_52w_high", "rs", "bars", "last_date", "summary"]
    write_csv(os.path.join(OUT, "universe.csv"), rows, cols)
    write_csv(os.path.join(OUT, "scan.csv"), scan, ["ticker", "name", "exchange", "rejected", "price", "dollar_vol", "adr", "run_90d"])

    daily, wk = [], []
    for r in rows:
        b = full.get(r["ticker"]) or stage1.get(r["ticker"]) or []
        daily.extend(bar_rows(r["ticker"], b[-130:]))
        wk.extend(bar_rows(r["ticker"], weekly(b)[-110:]))
    bcols = ["ticker", "date", "open", "high", "low", "close", "volume"]
    write_csv(os.path.join(OUT, "bars.csv.gz"), daily, bcols, gz=True)
    write_csv(os.path.join(OUT, "weekly.csv.gz"), wk, bcols, gz=True)

    lasts = {}
    for r in rows:
        lasts[r["last_date"]] = lasts.get(r["last_date"], 0) + 1
    latest = max(lasts, key=lasts.get) if lasts else ""
    with open(os.path.join(OUT, "STATUS.txt"), "w") as fh:
        fh.write("generated_utc=%s\n" % time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        fh.write("source=kiwoom\n")
        fh.write("symbols_scanned=%d\n" % len(syms))
        fh.write("with_history=%d\n" % with_hist)
        fh.write("survivors=%d\n" % len(rows))
        fh.write("latest_bar=%s\n" % latest)
        fh.write("latest_bar_coverage=%d/%d\n" % (lasts.get(latest, 0), len(rows)))
        fh.write("sector_resolved=%d\n" % resolved)
        fh.write("kiwoom_calls=%d\nkiwoom_throttled=%d\nkiwoom_errors=%d\n" % (kw.STATS["calls"], kw.STATS["throttled"], kw.STATS["errors"]))
        fh.write("runtime_min=%.1f\n" % ((time.time() - t_start) / 60))
    log("wrote universe.csv (%d rows), scan.csv (%d rows), bars/weekly; latest bar %s (%d/%d)" % (
        len(rows), len(scan), latest, lasts.get(latest, 0), len(rows)))
    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
