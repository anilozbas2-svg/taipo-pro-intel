import os
import time
import math
import logging
import inspect
from typing import Dict, Any, List, Optional

import requests

logger = logging.getLogger("STEADY_TREND")


# =========================================================
# ENV HELPERS
# =========================================================
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


def _safe_chat_id(raw: str) -> Optional[int]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


# =========================================================
# TICKER NORMALIZATION
# =========================================================
def _tv_ticker(sym: str) -> str:
    """
    Converts:
      THYAO.IS -> BIST:THYAO
      THYAO    -> BIST:THYAO
      BIST:THYAO -> BIST:THYAO
    """
    s = (sym or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".IS"):
        s = s[:-3]
    if ":" in s:
        return s
    return f"BIST:{s}"


def _norm_symbol(sym: str) -> str:
    """
    Converts:
      BIST:THYAO -> THYAO
      THYAO.IS   -> THYAO
      THYAO      -> THYAO
    """
    s = (sym or "").strip().upper()
    if not s:
        return ""
    if ":" in s:
        s = s.split(":")[-1].strip()
    if s.endswith(".IS"):
        s = s[:-3]
    return s


# =========================================================
# ENV
# =========================================================
STEADY_TREND_ENABLED = _env_bool("STEADY_TREND_ENABLED", False)
STEADY_TREND_CHAT_ID_RAW = os.getenv("STEADY_TREND_CHAT_ID", "").strip()
STEADY_TREND_CHAT_ID = _safe_chat_id(STEADY_TREND_CHAT_ID_RAW)

STEADY_TREND_INTERVAL_MIN = _env_int("STEADY_TREND_INTERVAL_MIN", 2)
STEADY_TREND_COOLDOWN_MIN = _env_int("STEADY_TREND_COOLDOWN_MIN", 45)

STEADY_TREND_MIN_PCT = _env_float("STEADY_TREND_MIN_PCT", 0.60)
STEADY_TREND_MAX_PCT = _env_float("STEADY_TREND_MAX_PCT", 2.20)
STEADY_TREND_MIN_VOL_SPIKE = _env_float("STEADY_TREND_MIN_VOL_SPIKE", 1.05)

# Senin ENV’de 0.30, default da 0.30 olmalı
STEADY_TREND_PROXY_MIN_STEADY = _env_float("STEADY_TREND_PROXY_MIN_STEADY", 0.30)

TV_SCAN_URL = os.getenv("STEADY_TREND_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = _env_int("STEADY_TREND_TV_TIMEOUT", 12)

STEADY_TREND_DRY_RUN = _env_bool("STEADY_TREND_DRY_RUN", False)
STEADY_TREND_DRY_RUN_TAG = _env_bool("STEADY_TREND_DRY_RUN_TAG", False)

# =========================================================
# COOLDOWN MEMORY (in-memory)
# =========================================================
_LAST_SENT_TS: Dict[str, float] = {}


def _cooldown_ok(symbol: str) -> bool:
    now = time.time()
    last = _LAST_SENT_TS.get(symbol, 0.0)
    if now - last < STEADY_TREND_COOLDOWN_MIN * 60:
        return False
    _LAST_SENT_TS[symbol] = now
    return True


# =========================================================
# TRADINGVIEW SCAN
# =========================================================
def _tv_scan_for_tickers(tickers: List[str]) -> List[Dict[str, Any]]:
    tv_tickers = [_tv_ticker(t) for t in (tickers or []) if t and str(t).strip()]
    tv_tickers = [t for t in tv_tickers if t]
    if not tv_tickers:
        return []

    payload = {
        "filter": [
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"},
            {"left": "close", "operation": "nempty"},
        ],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": tv_tickers},
        "columns": [
            "name",
            "change",  # percent change
            "volume",
            "close",
            "average_volume_10d_calc",
        ],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, min(200, len(tv_tickers))],
    }

    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        js = r.json() or {}

        out: List[Dict[str, Any]] = []
        for row in (js.get("data") or []):
            d = row.get("d") or []
            # expected: [name, change, volume, close, avg_vol_10d]
            if len(d) < 4:
                continue

            sym = _norm_symbol(str(d[0]))
            pct = _safe_float(d[1])
            vol = _safe_float(d[2])
            last = _safe_float(d[3])
            av10 = _safe_float(d[4]) if len(d) >= 5 else None

            if not sym or pct is None or vol is None or last is None:
                continue

            vol_spike_10g = None
            if av10 and av10 > 0:
                vol_spike_10g = vol / av10

            # "Steady proxy": candle datası yok, o yüzden kontrollü proxy skor
            # 0.0 - 1.0 arası normalize ediyoruz
            steady_proxy = 0.20
            if STEADY_TREND_MIN_PCT <= pct <= STEADY_TREND_MAX_PCT:
                steady_proxy += 0.45
            if vol_spike_10g is not None and vol_spike_10g >= STEADY_TREND_MIN_VOL_SPIKE:
                steady_proxy += 0.35
            steady_proxy = max(0.0, min(1.0, steady_proxy))

            out.append(
                {
                    "symbol": sym,
                    "last": last,
                    "pct_day": pct,
                    "vol_spike_10g": vol_spike_10g,
                    "steady_proxy": steady_proxy,
                }
            )

        return out

    except Exception as e:
        logger.warning("STEADY_TREND: TV scan error: %s", e)
        return []


# =========================================================
# FILTER & SCORE
# =========================================================
def _passes_filters(row: Dict[str, Any]) -> bool:
    pct = _safe_float(row.get("pct_day"))
    vol = _safe_float(row.get("vol_spike_10g"))
    proxy = _safe_float(row.get("steady_proxy"))

    # vol_spike_10g yoksa direkt eliyoruz (AVG volume dönmediyse)
    if pct is None or vol is None or proxy is None:
        return False

    if pct < STEADY_TREND_MIN_PCT:
        return False
    if pct > STEADY_TREND_MAX_PCT:
        return False
    if vol < STEADY_TREND_MIN_VOL_SPIKE:
        return False
    if proxy < STEADY_TREND_PROXY_MIN_STEADY:
        return False

    return True


def _steady_score(row: Dict[str, Any]) -> float:
    pct = float(row.get("pct_day") or 0.0)
    vol = float(row.get("vol_spike_10g") or 0.0)
    proxy = float(row.get("steady_proxy") or 0.0)

    denom = max(0.01, (STEADY_TREND_MAX_PCT - STEADY_TREND_MIN_PCT))
    pct_norm = max(0.0, min(1.0, (pct - STEADY_TREND_MIN_PCT) / denom))

    s = 0.0
    s += proxy * 4.0
    s += min(vol, 3.0) * 2.0
    s += pct_norm * 1.5
    return s


# =========================================================
# MESSAGE FORMAT
# =========================================================
def _format_msg(row: Dict[str, Any]) -> str:
    def fnum(x: Any, nd: int = 2) -> str:
        try:
            return f"{float(x):.{nd}f}"
        except Exception:
            return "n/a"

    sym = row.get("symbol", "n/a")

    # DRY-RUN etiketi (isteğe bağlı)
    prefix = "🧪 <b>DRY-RUN</b>\n" if STEADY_TREND_DRY_RUN_TAG else ""

    return (
        prefix
        + "🚄 <b>STEADY TREND – AĞIR TREN</b>\n"
        + "______________________________\n"
        + f"📌 <b>Hisse</b>: <code>{sym}</code>\n"
        + f"💰 <b>Fiyat</b>: {fnum(row.get('last'), 2)}\n"
        + f"📈 <b>Günlük</b>: +{fnum(row.get('pct_day'), 2)}%\n"
        + f"📊 <b>Hacim (10g)</b>: {fnum(row.get('vol_spike_10g'), 2)}x\n"
        + f"🧠 <b>Steady Proxy</b>: {fnum(row.get('steady_proxy'), 2)}\n"
        + f"🏁 <b>Skor</b>: {fnum(row.get('steady_score'), 2)}\n\n"
        + "📝 <i>Mentor notu: Spike kovalamıyoruz; kontrollü tırmanış.</i>"
    )


# =========================================================
# MAIN ENTRY (called from main.py via app.bot_data adapters)
# =========================================================
async def steady_trend_job(ctx, bist_open_fn, fetch_rows_fn, telegram_send_fn) -> None:
    if not STEADY_TREND_ENABLED:
        return
    if STEADY_TREND_CHAT_ID is None:
        return
    if not telegram_send_fn or not fetch_rows_fn:
        return

    # BIST açık değilse normalde durur; DRY_RUN ile bypass ederiz
        try:
            if (not STEADY_TREND_DRY_RUN) and bist_open_fn and (not bist_open_fn()):
                return
        except Exception:
            if not STEADY_TREND_DRY_RUN:
                return

    # fetch_rows_fn async/sync safe
    try:
        if inspect.iscoroutinefunction(fetch_rows_fn):
            rows = await fetch_rows_fn(ctx)
        else:
            rows = fetch_rows_fn(ctx)
    except Exception:
        return

    tickers: List[str] = []
    for r in (rows or []):
        t = (r.get("ticker") or r.get("symbol") or "").strip().upper()
        if t:
            tickers.append(t)

    if not tickers:
        return

    tv_rows = _tv_scan_for_tickers(tickers)
    picks: List[Dict[str, Any]] = []
    for r in tv_rows:
        if not _passes_filters(r):
            continue
        r["steady_score"] = _steady_score(r)
        picks.append(r)

    if not picks:
        return

    picks.sort(key=lambda x: x.get("steady_score", 0.0), reverse=True)
    top = picks[:3]

    for r in top:
        sym = str(r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        if not _cooldown_ok(sym):
            continue

        msg = _format_msg(r)
        try:
            if inspect.iscoroutinefunction(telegram_send_fn):
                await telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)
            else:
                telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)
        except Exception:
            continue


# =========================================================
# Backward compatibility entrypoint (main.py schedule_jobs can call this)
# =========================================================
async def job_steady_trend_scan(context, *args, **kwargs) -> None:
    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", {}) if app else {}

    bist_open_fn = bot_data.get("bist_session_open")
    fetch_rows_fn = bot_data.get("fetch_universe_rows")
    telegram_send_fn = bot_data.get("telegram_send")

    if not fetch_rows_fn or not telegram_send_fn:
        return

    await steady_trend_job(context, bist_open_fn, fetch_rows_fn, telegram_send_fn)
