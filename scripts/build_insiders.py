#!/usr/bin/env python3
"""
Collects open-market insider buying for the whole universe, from SEC EDGAR.

WHY THIS EXISTS
---------------
The screen weights insider buying at 22% - second only to chart geometry. But
the screen itself can only afford to look up Form 4s for a handful of names,
so it was fetching them for the ~10 names that ALREADY ranked highest on
geometry and theme. That means the #2 signal could never change the ranking: a
stock with a middling flag and three executives buying could not surface,
because nobody looked.

Running it here fixes that. The Action has unrestricted network and time, so
every name in the universe carries an insider score BEFORE ranking, and the
pairing the strategy actually wants - a good flag PLUS real buying - can win.

WHAT IT SHIPS
-------------
    data/insiders.csv   one row per open-market BUY or SELL in the window:
                        ticker,date,owner,role,title,code,shares,price,value,
                        shares_after

Only codes P (open-market purchase) and S (open-market sale) are kept. Grants,
option exercises, tax withholding and gifts are dropped at source - they are
compensation, not conviction, and carrying them would invite the exact mistake
the scorer exists to prevent.

SEC etiquette: a descriptive User-Agent is required and requests are limited to
under 10/sec. Both are enforced below. Nothing here needs an API key.
"""
from __future__ import annotations

import concurrent.futures as cf
import csv
import datetime as dt
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

# SEC requires automated traffic to identify itself with a contact email,
# and returns 403 to anything without one. The address is read from the
# SEC_CONTACT repository secret so it never appears in this public file.
SEC_CONTACT = os.environ.get("SEC_CONTACT", "").strip()
UA = "htf-feed research script %s" % (SEC_CONTACT or "(no contact configured)")
LOOKBACK_DAYS = 180
MAX_FORM4_PER_TICKER = 40
WORKERS = 8
MIN_INTERVAL = 0.11          # ~9 requests/sec across all threads

KEEP_CODES = {"P", "S"}

_lock = threading.Lock()
_last = [0.0]
_errors = []


def _throttled_get(url: str, timeout: int = 30):
    """Global rate limit shared by every worker thread."""
    with _lock:
        wait = MIN_INTERVAL - (time.time() - _last[0])
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.time()
    # No Accept-Encoding: urllib does NOT transparently decompress, so
    # asking for gzip/deflate and mishandling the reply is exactly how the
    # first version silently returned an empty CIK map and produced a run
    # with zero insider rows that still reported success.
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception as exc:                            # noqa: BLE001
        with _lock:
            if len(_errors) < 6:
                _errors.append("%s -> %r" % (url.split("?")[0][:72], exc))
        return None


def ticker_to_cik():
    """SEC's own ticker -> CIK map. One file, whole market, free."""
    raw = _throttled_get("https://www.sec.gov/files/company_tickers.json", timeout=60)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:                                   # noqa: BLE001
        return {}
    out = {}
    for row in data.values():
        t = str(row.get("ticker", "")).upper().strip()
        if t:
            out[t] = str(row.get("cik_str", "")).zfill(10)
    return out


def recent_form4s(cik: str, since: dt.date):
    """[(filingDate, accessionNumber, primaryDocument)] for Form 4s since since."""
    raw = _throttled_get("https://data.sec.gov/submissions/CIK%s.json" % cik, timeout=45)
    if not raw:
        return []
    try:
        rec = json.loads(raw).get("filings", {}).get("recent", {})
    except Exception:                                   # noqa: BLE001
        return []
    forms = rec.get("form") or []
    dates = rec.get("filingDate") or []
    accs = rec.get("accessionNumber") or []
    docs = rec.get("primaryDocument") or []
    out = []
    for i, f in enumerate(forms):
        if f != "4" or i >= len(dates) or i >= len(accs) or i >= len(docs):
            continue
        try:
            d = dt.date(*(int(x) for x in dates[i].split("-")))
        except Exception:                               # noqa: BLE001
            continue
        if d < since:
            continue
        out.append((dates[i], accs[i], docs[i]))
        if len(out) >= MAX_FORM4_PER_TICKER:
            break
    return out


def _txt(node, *names):
    """Form 4 wraps most values in <value>; some filers omit it."""
    for n in names:
        el = node.find(n)
        if el is None:
            continue
        v = el.find("value")
        s = (v.text if v is not None else el.text) or ""
        s = s.strip()
        if s:
            return s
    return ""


def parse_form4(xml_bytes):
    """Extract open-market buys/sells. Returns [] for grant-only filings."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    owner = root.find(".//reportingOwner")
    name = role = title = ""
    if owner is not None:
        oid = owner.find("reportingOwnerId")
        name = _txt(oid if oid is not None else owner, "rptOwnerName")
        rel = owner.find("reportingOwnerRelationship")
        if rel is not None:
            flags = []
            if _txt(rel, "isDirector") in ("1", "true"):
                flags.append("director")
            if _txt(rel, "isOfficer") in ("1", "true"):
                flags.append("officer")
            if _txt(rel, "isTenPercentOwner") in ("1", "true"):
                flags.append("10% owner")
            role = ", ".join(flags)
            title = _txt(rel, "officerTitle")

    rows = []
    for tx in root.findall(".//nonDerivativeTransaction"):
        coding = tx.find("transactionCoding")
        code = _txt(coding, "transactionCode") if coding is not None else ""
        if code not in KEEP_CODES:
            continue
        amounts = tx.find("transactionAmounts")
        if amounts is None:
            continue
        try:
            shares = float(_txt(amounts, "transactionShares") or 0)
            price = float(_txt(amounts, "transactionPricePerShare") or 0)
        except ValueError:
            continue
        if shares <= 0:
            continue
        post = tx.find("postTransactionAmounts")
        after = 0.0
        if post is not None:
            try:
                after = float(_txt(post, "sharesOwnedFollowingTransaction") or 0)
            except ValueError:
                after = 0.0
        rows.append(dict(
            date=_txt(tx, "transactionDate"),
            owner=name, role=role, title=title, code=code,
            shares=shares, price=price, value=round(shares * price, 2),
            shares_after=after,
        ))
    return rows


def form4_url(cik: str, accession: str, primary_doc: str) -> str:
    # primaryDocument arrives as "xslF345X06/form4-...xml"; the raw XML is the
    # part after the stylesheet folder.
    doc = primary_doc.split("/")[-1]
    return "https://www.sec.gov/Archives/edgar/data/%s/%s/%s" % (
        int(cik), accession.replace("-", ""), doc)


def collect(tickers):
    since = dt.date.today() - dt.timedelta(days=LOOKBACK_DAYS)
    cikmap = ticker_to_cik()
    print("  ticker->CIK map: %d symbols" % len(cikmap), flush=True)
    if not cikmap:
        print("  FAILED to load the SEC ticker->CIK map. Errors:", file=sys.stderr)
        for e in _errors:
            print("    " + e, file=sys.stderr)
        return None

    pairs = [(t, cikmap[t]) for t in tickers if t in cikmap]
    print("  %d of %d universe names matched a CIK" % (len(pairs), len(tickers)), flush=True)

    filings = []

    def list_one(pair):
        t, cik = pair
        return t, cik, recent_form4s(cik, since)

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for n, (t, cik, f4) in enumerate(ex.map(list_one, pairs), 1):
            for _date, acc, doc in f4:
                filings.append((t, cik, acc, doc))
            if n % 100 == 0:
                print("    listed %d/%d tickers, %d Form 4s queued"
                      % (n, len(pairs), len(filings)), flush=True)
    print("  %d Form 4 filings to parse" % len(filings), flush=True)

    rows = []

    def fetch_one(item):
        t, cik, acc, doc = item
        raw = _throttled_get(form4_url(cik, acc, doc), timeout=30)
        if not raw:
            return []
        out = []
        for r in parse_form4(raw):
            r["ticker"] = t
            out.append(r)
        return out

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for n, got in enumerate(ex.map(fetch_one, filings), 1):
            rows.extend(got)
            if n % 500 == 0:
                print("    parsed %d/%d filings, %d buy/sell rows"
                      % (n, len(filings), len(rows)), flush=True)

    return rows


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    uni_path = os.path.join(OUT, "universe.csv")
    if not os.path.exists(uni_path):
        print("universe.csv missing - run build_universe.py first", file=sys.stderr)
        return 1
    with open(uni_path) as fh:
        tickers = [r["ticker"] for r in csv.DictReader(fh) if r.get("ticker")]
    if os.environ.get("SMOKE_TEST"):
        tickers = tickers[:40]
    if not SEC_CONTACT:
        # Not an error - just unconfigured. Skip cleanly rather than firing
        # hundreds of requests SEC will reject with 403.
        print("SEC_CONTACT secret is not set, so SEC would reject every "
              "request (403). Skipping insider collection; the screen will "
              "score insiders as absent.", flush=True)
        cols = ["ticker", "date", "owner", "role", "title", "code",
                "shares", "price", "value", "shares_after"]
        with open(os.path.join(OUT, "insiders.csv"), "w", newline="") as fh:
            csv.DictWriter(fh, fieldnames=cols).writeheader()
        with open(os.path.join(OUT, "STATUS.txt"), "a") as fh:
            fh.write("insider_status=skipped (SEC_CONTACT secret not set)\n")
        return 0

    print("collecting Form 4s for %d tickers ..." % len(tickers), flush=True)

    rows = collect(tickers)
    if rows is None:
        # Fail loudly. A silent empty insiders.csv would quietly drop the
        # screen's second-heaviest signal while everything looked green.
        return 1
    rows.sort(key=lambda r: (r["ticker"], r.get("date") or ""))

    cols = ["ticker", "date", "owner", "role", "title", "code",
            "shares", "price", "value", "shares_after"]
    with open(os.path.join(OUT, "insiders.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})

    buys = sum(1 for r in rows if r["code"] == "P")
    names_with_buys = len({r["ticker"] for r in rows if r["code"] == "P"})
    print("wrote insiders.csv: %d rows (%d open-market buys across %d names)"
          % (len(rows), buys, names_with_buys))

    with open(os.path.join(OUT, "STATUS.txt"), "a") as fh:
        fh.write("insider_status=ok\n")
        fh.write("insider_rows=%d\n" % len(rows))
        fh.write("insider_buy_rows=%d\n" % buys)
        fh.write("names_with_open_market_buys=%d\n" % names_with_buys)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
