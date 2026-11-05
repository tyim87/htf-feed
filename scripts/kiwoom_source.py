#!/usr/bin/env python3
"""
Kiwoom Securities REST API as a bar source for the HTF feed.

Standard library only - nothing to pip install.

Runs on the PC whose public IP is registered on openapi.kiwoom.com (the API
rejects every other IP with error 8010, which is why this cannot run inside
a GitHub Action). Credentials come from the environment:

    KIWOOM_APP_KEY, KIWOOM_APP_SECRET      required
    KIWOOM_BASE                            default https://api.kiwoom.com
                                           (mock keys: https://mockapi.kiwoom.com)
    KIWOOM_RPS                             requests per second, default 3.0
                                           (smoke test: ~3.8 achieved, 2/30 throttled)

Endpoints used (all POST, JSON):
    /oauth2/token                      grant_type=client_credentials -> token
    /api/us/stkinfo  api-id usa10099   US stock list, paginated via cont-yn/next-key
    /api/us/chart    api-id usa06012   daily OHLCV, 100 bars per page, newest first;
                                       strt_dt is the date to walk BACK from, and
                                       cont-yn/next-key walks further back

Bars are returned oldest-first as dicts {date, open, high, low, close, volume}
with date as 'YYYY-MM-DD'. upd_stkpc_tp=1 asks for adjusted prices,
exrt_appl_tp=0 keeps them in USD.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = os.environ.get("KIWOOM_BASE", "https://api.kiwoom.com").rstrip("/")
RPS = float(os.environ.get("KIWOOM_RPS", "3.0"))
MIN_INTERVAL = 1.0 / max(RPS, 0.2)
TIMEOUT = 30
RETRIES = 4

_lock = threading.Lock()
_last = [0.0]
_token = {"value": None, "issued": 0.0}
STATS = {"calls": 0, "throttled": 0, "errors": 0, "retries": 0}


class KiwoomError(RuntimeError):
    pass


def _creds():
    key = os.environ.get("KIWOOM_APP_KEY", "").strip()
    sec = os.environ.get("KIWOOM_APP_SECRET", "").strip()
    if not key or not sec:
        raise KiwoomError("KIWOOM_APP_KEY / KIWOOM_APP_SECRET not set")
    return key, sec


def _pace():
    with _lock:
        wait = MIN_INTERVAL - (time.time() - _last[0])
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.time()


def _post(path, api_id, body, token=None, cont=None):
    hdr = {"Content-Type": "application/json;charset=UTF-8"}
    if api_id:
        hdr["api-id"] = api_id
    if token:
        hdr["authorization"] = "Bearer " + token
    if cont:
        hdr["cont-yn"], hdr["next-key"] = cont
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(), headers=hdr, method="POST")
    _pace()
    STATS["calls"] += 1
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        payload = json.loads(r.read().decode("utf-8", "replace") or "{}")
        return payload, {k.lower(): v for k, v in r.headers.items()}


def token(force=False):
    """Access token, cached for ~20h (Kiwoom tokens last a day)."""
    if _token["value"] and not force and time.time() - _token["issued"] < 20 * 3600:
        return _token["value"]
    key, sec = _creds()
    p, _ = _post("/oauth2/token", None, {"grant_type": "client_credentials", "appkey": key, "secretkey": sec})
    tok = p.get("token")
    if not tok:
        raise KiwoomError("token refused: %s" % {k: v for k, v in p.items() if k != "token"})
    _token["value"], _token["issued"] = tok, time.time()
    return tok


def _is_throttle(payload):
    msg = str(payload.get("return_msg", ""))
    rc = str(payload.get("return_code", "0"))
    return rc not in ("0", "") and any(w in msg for w in ("초과", "제한", "limit", "너무", "빈번"))


def call(path, api_id, body, cont=None):
    """One API call with pacing, throttle back-off, token refresh and retries.
    Returns (payload, headers). Raises KiwoomError after RETRIES failures."""
    last_exc = None
    for attempt in range(RETRIES):
        try:
            p, h = _post(path, api_id, body, token(), cont)
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code == 429:
                STATS["throttled"] += 1
                time.sleep(1.5 * (attempt + 1))
                continue
            if exc.code in (401, 403):
                token(force=True)
                STATS["retries"] += 1
                continue
            STATS["errors"] += 1
            time.sleep(1.0 * (attempt + 1))
            continue
        except Exception as exc:                        # noqa: BLE001  network blips
            last_exc = exc
            STATS["errors"] += 1
            time.sleep(1.0 * (attempt + 1))
            continue
        rc = str(p.get("return_code", "0"))
        if rc in ("0", ""):
            return p, h
        msg = str(p.get("return_msg", ""))
        last_exc = KiwoomError("%s %s: return_code=%s %s" % (path, api_id, rc, msg))
        if "token" in msg.lower() or "인증" in msg or "8005" in msg:
            token(force=True)
            STATS["retries"] += 1
            continue
        # Any other non-zero code is treated as a throttle: back off and retry.
        # (The smoke test saw 2/30 back-to-back calls refused this way.)
        STATS["throttled"] += 1 if _is_throttle(p) or attempt == 0 else 0
        time.sleep(1.5 * (attempt + 1))
    if isinstance(last_exc, KiwoomError):
        raise last_exc
    raise KiwoomError("%s %s failed after %d attempts: %r" % (path, api_id, RETRIES, last_exc))


# ---------------------------------------------------------------- stock list
_TICKER_OK = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")


def stock_list(include_etf=False):
    """[{ticker, name, exchange, industry_kr, is_etf}] for the US market."""
    rows, cont, pages = [], None, 0
    while True:
        p, h = call("/api/us/stkinfo", "usa10099", {"stex_tp": "%"}, cont)
        rows.extend(p.get("list") or [])
        pages += 1
        if h.get("cont-yn") == "Y" and h.get("next-key") and pages < 400:
            cont = ("Y", h["next-key"])
        else:
            break
    out, seen = [], set()
    for r in rows:
        t = str(r.get("stk_cd", "")).strip().upper()
        etf = str(r.get("isEtf", "")).upper() in ("Y", "1", "TRUE")
        if not t or t in seen or (etf and not include_etf) or not _TICKER_OK.match(t):
            continue
        seen.add(t)
        out.append(dict(ticker=t, name=str(r.get("stk_enm") or r.get("stk_nm") or "").strip(),
                        exchange=str(r.get("stex_tp", "")).strip(), industry_kr=str(r.get("upgb", "")).strip(),
                        is_etf=etf))
    return out


# ---------------------------------------------------------------- daily bars
def _row(r):
    try:
        d = str(r.get("dt", ""))
        if len(d) != 8:
            return None
        o, h, l, c = (abs(float(r.get(k) or 0)) for k in ("open_pric", "high_pric", "low_pric", "cur_prc"))
        v = int(float(r.get("acc_trde_qty") or 0))
        if c <= 0 or h <= 0 or l <= 0:
            return None
        if o <= 0:
            o = c
        return dict(date="%s-%s-%s" % (d[:4], d[4:6], d[6:]), open=o, high=max(h, o, c), low=min(l, o, c), close=c, volume=v)
    except (TypeError, ValueError):
        return None


def daily_bars(ticker, exchange, pages=1, end_date=None):
    """Up to pages*100 daily bars, oldest first. Returns [] on any API error
    (the caller records the symbol as unfetched rather than aborting the run)."""
    end = (end_date or dt.date.today()).strftime("%Y%m%d")
    body = {"stex_tp": exchange, "stk_cd": ticker, "strt_dt": end, "upd_stkpc_tp": "1", "exrt_appl_tp": "0"}
    out, cont = [], None
    for _ in range(max(1, pages)):
        try:
            p, h = call("/api/us/chart", "usa06012", body, cont)
        except KiwoomError as exc:
            if not out:
                sys.stderr.write("  %s: %s\n" % (ticker, exc))
            break
        rl = p.get("result_list") or []
        got = [x for x in (_row(r) for r in rl) if x]
        out.extend(got)
        if not rl or h.get("cont-yn") != "Y" or not h.get("next-key"):
            break
        cont = ("Y", h["next-key"])
    # newest-first pages, possibly overlapping at the seam -> dedupe on date
    seen, bars = set(), []
    for b in out:
        if b["date"] not in seen:
            seen.add(b["date"])
            bars.append(b)
    bars.sort(key=lambda b: b["date"])
    return bars


if __name__ == "__main__":        # quick manual check: python kiwoom_source.py NVDA ND 2
    t = sys.argv[1] if len(sys.argv) > 1 else "NVDA"
    x = sys.argv[2] if len(sys.argv) > 2 else "ND"
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    b = daily_bars(t, x, pages=n)
    print(t, len(b), "bars", b[0]["date"] if b else "", "..", b[-1]["date"] if b else "")
    print(STATS)
