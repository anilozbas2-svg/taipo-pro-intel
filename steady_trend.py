import os
import time
import math
import logging
from typing import Dict, Any, List, Tuple, Optional

import requests

logger = logging.getLogger("STEADY_TREND")

# -------------------------
# ENV helpers
# -------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "")
    if val == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None

# -------------------------
# ENV
# -------------------------
STEADY_TREND_ENABLED = _env_bool("STEADY_TREND_ENABLED", False)
STEADY_TREND_CHAT_ID = os.getenv("STEADY_TREND_CHAT_ID", "").strip()

STEADY_TREND_INTERVAL_MIN = _env_int("STEADY_TREND_INTERVAL_MIN", 2)

# Ağır tren filtreleri (TV tabanlı)
STEADY_TREND_MIN_PCT = _env_float("STEADY_TREND_MIN_PCT", 0.60)          # intraday %
STEADY_TREND_MAX_PCT = _env_float("STEADY_TREND_MAX_PCT", 2.20)          # çok kaçtıysa “tren” değil
STEADY_TREND_MIN_VOL_SPIKE = _env_float("STEADY_TREND_MIN_VOL_SPIKE", 1.05)  # vol/avg10

# “düzenlilik” proxy (şimdilik hafif)
STEADY_TREND_PROXY_MIN_STEADY = _env_float("STEADY_TREND_PROXY_MIN_STEADY", 0.62)

# Spam önleme
STEADY_TREND_COOLDOWN_MIN = _env_int("STEADY_TREND_COOLDOWN_MIN", 45)

# TradingView scanner
TV_SCAN_URL = os.getenv("STEADY_TREND_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = _env_int("STEADY_TREND_TV_TIMEOUT", 12)

# -------------------------
# In-memory cooldown state
# -------------------------
_LAST_SENT_TS: Dict[str, float] = {}

def _cooldown_ok(symbol: str) -> bool:
    now = time.time()
    last = _LAST_SENT_TS.get(symbol, 0.0)
    if now - last < STEADY_TREND_COOLDOWN_MIN * 60:
        return False
    _LAST_SENT_TS[symbol] = now
    return True

# -------------------------
# TradingView scan for specific tickers
# -------------------------
def _tv_scan_for_tickers(tickers: List[str]) -> List[Dict[str, Any]]:
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    if not tickers:
        return []

    payload = {
        "filter": [
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"},
            {"left": "close", "operation": "nempty"},
        ],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": tickers},
        "columns": ["name", "change", "volume", "close", "average_volume_10d_calc"],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, min(200, len(tickers))]
    }

    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        js = r.json() or {}
        out: List[Dict[str, Any]] = []

        for row in (js.get("data") or []):
            d = row.get("d") or []
            if len(d) < 4:
                continue

            sym = str(d[0]).strip().upper()
            pct = _safe_float(d[1])      # günlük change (%)
            vol = _safe_float(d[2])      # volume
            last = _safe_float(d[3])     # close
            av10 = _safe_float(d[4]) if len(d) >= 5 else None

            if not sym or pct is None or vol is None or last is None:
                continue

            vol_spike_10g = None
            if av10 is not None and av10 > 0:
                vol_spike_10g = vol / av10

            # “steadiness” proxy: aşırı spike değil, kontrollü aralık + hacim var ise yükselt
            # (gerçek bar-up oranı yok; TV ile proxy)
            steady_proxy = 0.50
            if STEADY_TREND_MIN_PCT <= pct <= STEADY_TREND_MAX_PCT:
                steady_proxy += 0.15
            if vol_spike_10g is not None and vol_spike_10g >= STEADY_TREND_MIN_VOL_SPIKE:
                steady_proxy += 0.15
            if pct <= (STEADY_TREND_MIN_PCT + (STEADY_TREND_MAX_PCT - STEADY_TREND_MIN_PCT) * 0.65):
                steady_proxy += 0.10

            steady_proxy = max(0.0, min(1.0, steady_proxy))

            out.append({
                "symbol": sym,
                "last": last,
                "pct_day": pct,
                "vol_spike_10g": vol_spike_10g,
                "bars_up_ratio": steady_proxy,   # proxy
                "delta_15m": None,               # yok (TV ile)
            })

        return out
    except Exception as e:
        logger.warning("STEADY_TREND: TV scan error: %s", e)
        return []

# -------------------------
# Filters / scoring
# -------------------------
def _passes_filters(row: Dict[str, Any]) -> Tuple[bool, str]:
    pct_day = _safe_float(row.get("pct_day"))
    vol_spike = _safe_float(row.get("vol_spike_10g"))
    cons = _safe_float(row.get("bars_up_ratio"))

    if pct_day is None or vol_spike is None or cons is None:
        return (False, "bad_data")

    if pct_day < STEADY_TREND_MIN_PCT:
        return (False, "pct_low")
    if pct_day > STEADY_TREND_MAX_PCT:
        return (False, "pct_too_high")
    if vol_spike < STEADY_TREND_MIN_VOL_SPIKE:
        return (False, "vol_low")
    if cons < STEADY_TREND_PROXY_MIN_STEADY:
        return (False, "steady_proxy_low")

    return (True, "ok")

def _steady_score(row: Dict[str, Any]) -> float:
    pct_day = float(row.get("pct_day", 0.0) or 0.0)
    vol_spike = float(row.get("vol_spike_10g", 0.0) or 0.0)
    cons = float(row.get("bars_up_ratio", 0.0) or 0.0)

    s = 0.0
    s += cons * 3.0
    s += min(vol_spike, 2.5) * 1.8
    # pct çok kaçarsa puanı kır (tren değil)
    pct_norm = max(0.0, min(1.0, (pct_day - STEADY_TREND_MIN_PCT) / max(0.01, (STEADY_TREND_MAX_PCT - STEADY_TREND_MIN_PCT))))
    s += pct_norm * 1.2
    return s

def _format_msg(row: Dict[str, Any]) -> str:
    sym = row.get("symbol", "?")
    last = row.get("last", None)
    pct_day = row.get("pct_day", None)
    vol_spike = row.get("vol_spike_10g", None)
    cons = row.get("bars_up_ratio", None)
    score = row.get("steady_score", None)

    def fnum(x, nd=2):
        try:
            return f"{float(x):.{nd}f}"
        except Exception:
            return "n/a"

    lines = []
    lines.append("🚄 STEADY TREND – AĞIR TREN (TV)")
    lines.append("")
    lines.append(f"HİSSE: {sym}")
    lines.append(f"FİYAT: {fnum(last, 2)}")
    lines.append(f"GÜNLÜK: +{fnum(pct_day, 2)}%")
    lines.append(f"HACİM: {fnum(vol_spike, 2)}x (10g-TV)")
    lines.append(f"İSTİKRAR(PROXY): {fnum(cons, 2)}")
    lines.append(f"SKOR: {fnum(score, 2)}")
    lines.append("")
    lines.append("🧠 Mentor notu: Spike değil; kontrollü tırmanış (TV tabanlı). Takipte kal.")
    return "\n".join(lines)

# -------------------------
# PUBLIC ENTRY (main.py ile uyumlu)
# steady_trend_job(ctx, bist_open_fn, fetch_rows_fn, telegram_send_fn)
# -------------------------
async def steady_trend_job(ctx, bist_open_fn, fetch_rows_fn, telegram_send_fn) -> None:
    if not STEADY_TREND_ENABLED:
        logger.info("STEADY_TREND: disabled -> return")
        return

    if not STEADY_TREND_CHAT_ID:
        logger.warning("STEADY_TREND: missing STEADY_TREND_CHAT_ID -> return")
        return

    try:
        if bist_open_fn and (not bist_open_fn()):
            logger.info("STEADY_TREND: session_closed -> return")
            return
    except Exception as e:
        logger.warning("STEADY_TREND: bist_open_fn error: %s", e)
        return

    # fetch rows from main.py (expected: [{"ticker": "THYAO"}, ...])
    try:
        rows = await fetch_rows_fn(ctx) if fetch_rows_fn else []
    except Exception as e:
        logger.warning("STEADY_TREND: fetch_rows_fn error: %s", e)
        return

    tickers: List[str] = []
    for r in (rows or []):
        t = (r.get("ticker") or r.get("symbol") or "").strip().upper()
        if t:
            tickers.append(t)

    if not tickers:
        logger.info("STEADY_TREND: no tickers")
        return

    tv_rows = _tv_scan_for_tickers(tickers)
    if not tv_rows:
        logger.info("STEADY_TREND: TV returned no rows")
        return

    picks: List[Dict[str, Any]] = []
    for r in tv_rows:
        ok, _reason = _passes_filters(r)
        if not ok:
            continue
        r["steady_score"] = _steady_score(r)
        picks.append(r)

    if not picks:
        logger.info("STEADY_TREND: no alerts")
        return

    picks.sort(key=lambda x: float(x.get("steady_score", 0.0) or 0.0), reverse=True)
    top = picks[:3]

    sent_any = 0
    for r in top:
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        if not _cooldown_ok(sym):
            continue

        msg = _format_msg(r)
        try:
            await telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)
            sent_any += 1
        except Exception as e:
            logger.warning("STEADY_TREND: telegram_send_fn error for %s: %s", sym, e)

    logger.info("STEADY_TREND: sent=%d top=%d", sent_any, len(top))
