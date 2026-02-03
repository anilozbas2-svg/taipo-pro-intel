import os
import json
import time
import math
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import CommandHandler, ContextTypes, Application

logger = logging.getLogger("MOMO_PRIME")

# ==========================
# PRIME CONFIG (env)
# ==========================
MOMO_PRIME_ENABLED = os.getenv("MOMO_PRIME_ENABLED", "1").strip() == "1"
MOMO_PRIME_CHAT_ID = os.getenv("MOMO_PRIME_CHAT_ID", "").strip()

MOMO_PRIME_INTERVAL_MIN = int(os.getenv("MOMO_PRIME_INTERVAL_MIN", "3"))

PRIME_PCT_MIN = float(os.getenv("MOMO_PRIME_PCT_MIN", "0.30"))
PRIME_PCT_CORE_MAX = float(os.getenv("MOMO_PRIME_PCT_CORE_MAX", "0.60"))
PRIME_PCT_MAX = float(os.getenv("MOMO_PRIME_PCT_MAX", "0.80"))

PRIME_VOL_RATIO_MIN = float(os.getenv("MOMO_PRIME_VOL_RATIO_MIN", "1.80"))
PRIME_COOLDOWN_SEC = int(os.getenv("MOMO_PRIME_COOLDOWN_SEC", "14400"))  # 4h

# TradingView scanner
TV_SCAN_URL = os.getenv("MOMO_PRIME_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = int(os.getenv("MOMO_PRIME_TV_TIMEOUT", "12"))

# Yahoo (only for averages/position windows)
YAHOO_TIMEOUT = int(os.getenv("MOMO_PRIME_YAHOO_TIMEOUT", "12"))
YAHOO_SUFFIX = os.getenv("MOMO_PRIME_YAHOO_SUFFIX", ".IS").strip()  # BIST

# State files (isolated)
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
PRIME_STATE_FILE = os.path.join(DATA_DIR, "momo_prime_state.json")
PRIME_LAST_ALERT_FILE = os.path.join(DATA_DIR, "momo_prime_last_alert.json")

# Small caches (RAM)
_YAHOO_CACHE: Dict[str, Dict[str, Any]] = {}  # {sym: {ts, data}}
_YAHOO_CACHE_TTL = int(os.getenv("MOMO_PRIME_YAHOO_CACHE_TTL", "1800"))  # 30 min


# ==========================
# JSON helpers
# ==========================
def _load_json(path: str, default: dict) -> dict:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.exception("PRIME load_json error: %s", e)
        return default


def _save_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.exception("PRIME save_json error: %s", e)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_iso(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return None


# ==========================
# PRIME state defaults
# ==========================
def _default_prime_state() -> dict:
    return {
        "schema_version": "1.0",
        "system": "momo_prime_balina",
        "telegram": {
            "momo_prime_chat_id": int(MOMO_PRIME_CHAT_ID) if MOMO_PRIME_CHAT_ID else None
        },
        "scan": {
            "interval_seconds": MOMO_PRIME_INTERVAL_MIN * 60,
            "last_scan_utc": None
        },
        "rules": {
            "pct_min": PRIME_PCT_MIN,
            "pct_core_max": PRIME_PCT_CORE_MAX,
            "pct_max": PRIME_PCT_MAX,
            "vol_ratio_min": PRIME_VOL_RATIO_MIN,
            "cooldown_seconds": PRIME_COOLDOWN_SEC,
            "reference_windows_days": [20, 400],
            "position_windows_days": [30, 90, 180]
        }
    }


def _default_last_alert() -> dict:
    return {
        "schema_version": "1.0",
        "system": "momo_prime_balina",
        "cooldown_seconds": PRIME_COOLDOWN_SEC,
        "last_alert_by_symbol": {}
    }


# ==========================
# PRIME calculations
# ==========================
def _phase_from_pct(pct: float) -> Optional[str]:
    if pct < PRIME_PCT_MIN or pct > PRIME_PCT_MAX:
        return None
    if pct <= PRIME_PCT_CORE_MAX:
        return "CORE"
    return "LATE"


def _hash_message(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


def _cooldown_ok(last_alert_ts: Optional[float], now_ts: float) -> bool:
    if last_alert_ts is None:
        return True
    return (now_ts - last_alert_ts) >= PRIME_COOLDOWN_SEC


def _pct_position(close: float, low: float, high: float) -> Optional[float]:
    try:
        if any(map(lambda v: v is None or isinstance(v, float) and math.isnan(v), [close, low, high])):
            return None
        if high <= low:
            return None
        return max(0.0, min(1.0, (close - low) / (high - low)))
    except Exception:
        return None


# ==========================
# TradingView scan (fast filter)
# ==========================
def _tv_scan_rows() -> List[dict]:
    # We keep it lightweight: ask for symbol, change, volume
    payload = {
        "filter": [
            {"left": "market_cap_basic", "operation": "nempty"},
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"}
        ],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "change", "volume", "close"],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, 200]
    }
    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        out = []
        for row in data.get("data", []) or []:
            d = row.get("d") or []
            if len(d) < 4:
                continue
            out.append({
                "symbol": str(d[0]).strip().upper(),
                "change_pct": float(d[1]),
                "volume": float(d[2]),
                "close": float(d[3])
            })
        return out
    except Exception as e:
        logger.error("PRIME TV scan error: %s", e)
        return []


# ==========================
# Yahoo chart (averages & position)
# ==========================
def _yahoo_chart(symbol: str) -> Optional[dict]:
    now = time.time()
    cached = _YAHOO_CACHE.get(symbol)
    if cached and (now - cached.get("ts", 0)) < _YAHOO_CACHE_TTL:
        return cached.get("data")

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "range": "400d",
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits"
    }
    try:
        r = requests.get(url, params=params, timeout=YAHOO_TIMEOUT)
        r.raise_for_status()
        js = r.json() or {}
        _YAHOO_CACHE[symbol] = {"ts": now, "data": js}
        return js
    except Exception as e:
        logger.error("PRIME Yahoo error %s: %s", symbol, e)
        return None


def _extract_yahoo_series(js: dict) -> Optional[Tuple[List[int], List[float], List[float], List[float], List[float]]]:
    try:
        chart = (((js or {}).get("chart") or {}).get("result") or [None])[0]
        if not chart:
            return None
        ts = chart.get("timestamp") or []
        ind = (chart.get("indicators") or {}).get("quote") or []
        q = ind[0] if ind else {}
        closes = q.get("close") or []
        highs = q.get("high") or []
        lows = q.get("low") or []
        vols = q.get("volume") or []
        if not ts or not closes or not highs or not lows or not vols:
            return None
        return ts, closes, highs, lows, vols
    except Exception:
        return None


def _avg_volume(vols: List[float], n: int) -> Optional[float]:
    vals = [v for v in vols[-n:] if v is not None]
    if len(vals) < max(5, int(n * 0.5)):
        return None
    return sum(vals) / float(len(vals))


def _window_low_high(closes: List[float], highs: List[float], lows: List[float], n: int) -> Optional[Tuple[float, float, float]]:
    c = [v for v in closes[-n:] if v is not None]
    h = [v for v in highs[-n:] if v is not None]
    l = [v for v in lows[-n:] if v is not None]
    if len(c) < max(5, int(n * 0.5)) or not h or not l:
        return None
    close_last = c[-1]
    return close_last, min(l), max(h)


def _compute_prime_metrics(ticker: str, today_volume: float) -> Optional[dict]:
    yahoo_symbol = f"{ticker}{YAHOO_SUFFIX}"
    js = _yahoo_chart(yahoo_symbol)
    ser = _extract_yahoo_series(js or {})
    if not ser:
        return None

    _ts, closes, highs, lows, vols = ser

    av20 = _avg_volume(vols, 20)
    av400 = _avg_volume(vols, 400)

    if not av20 or not av400:
        return None

    r20 = today_volume / av20 if av20 > 0 else None
    r400 = today_volume / av400 if av400 > 0 else None
    if r20 is None or r400 is None:
        return None

    w1 = _window_low_high(closes, highs, lows, 30)
    w3 = _window_low_high(closes, highs, lows, 90)
    w6 = _window_low_high(closes, highs, lows, 180)
    if not w1 or not w3 or not w6:
        return None

    c1, lo1, hi1 = w1
    c3, lo3, hi3 = w3
    c6, lo6, hi6 = w6

    p1 = _pct_position(c1, lo1, hi1)
    p3 = _pct_position(c3, lo3, hi3)
    p6 = _pct_position(c6, lo6, hi6)

    return {
        "vol_ratio_20d": r20,
        "vol_ratio_400d": r400,
        "pos_1m": p1,
        "pos_3m": p3,
        "pos_6m": p6
    }


# ==========================
# Message formatting
# ==========================
def _fmt_pos(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"%{int(round(x * 100))}"


def _format_prime_message(
    ticker: str,
    pct: float,
    phase: str,
    today_volume: float,
    r20: float,
    r400: float,
    p1: Optional[float],
    p3: Optional[float],
    p6: Optional[float]
) -> str:
    # No heavy indicators; keep it clean.
    msg = (
        "🐳🔥 <b>MOMO PRIME BALİNA</b>\n\n"
        f"<b>HİSSE:</b> {ticker}\n"
        f"<b>İLK SİNYAL:</b> {pct:+.2f}%  <b>({phase})</b>\n"
        f"<b>HACİM:</b> {r20:.2f}x (20g) | {r400:.2f}x (400g)\n"
        f"<b>DİP-TEPE KONUM:</b> 1A { _fmt_pos(p1) } | 3A { _fmt_pos(p3) } | 6A { _fmt_pos(p6) }\n\n"
        "🧠 <i>Mentor notu:</i> Erken faz PRIME alarm. Takip modunda.\n"
        f"⏱ {datetime.now().strftime('%H:%M')}"
    )
    return msg


# ==========================
# PRIME decision
# ==========================
def _should_alert(
    last_alert_map: dict,
    ticker: str,
    pct: float,
    phase: str,
    r20: float,
    r400: float,
    now_ts: float,
    message_hash: str
) -> bool:
    entry = (last_alert_map.get(ticker) or {})
    last_ts = _parse_utc_iso(entry.get("last_alert_utc"))
    if not _cooldown_ok(last_ts, now_ts):
        return False

    # Volume condition (>= min on max(20,400))
    if max(r20, r400) < PRIME_VOL_RATIO_MIN:
        return False

    # Same-message guard (same scan duplication)
    if entry.get("last_message_hash") == message_hash:
        return False

    # Pct already checked before, but keep safe:
    if pct < PRIME_PCT_MIN or pct > PRIME_PCT_MAX:
        return False
    if phase not in ("CORE", "LATE"):
        return False

    return True


# ==========================
# /prime command
# ==========================
async def cmd_prime(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sub = ""
    if context.args:
        sub = (context.args[0] or "").strip().lower()

    if sub in ("help", ""):
        txt = (
            "🐳🔥 MOMO PRIME BALİNA\n\n"
            "Komutlar:\n"
            "• /prime status  → PRIME durum\n"
            "• /prime test    → test mesajı\n\n"
            "Not: PRIME tarama 3 dk; koşullar: %0.30–%0.80 + hacim ≥1.8x (20g/400g) + 4 saat cooldown."
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "status":
        st = _load_json(PRIME_STATE_FILE, _default_prime_state())
        la = _load_json(PRIME_LAST_ALERT_FILE, _default_last_alert())
        last_scan = ((st.get("scan") or {}).get("last_scan_utc")) or "n/a"
        n_alerts = len((la.get("last_alert_by_symbol") or {}))
        txt = (
            "🐳🔥 PRIME STATUS\n\n"
            f"enabled: {int(MOMO_PRIME_ENABLED)}\n"
            f"chat_id: {MOMO_PRIME_CHAT_ID or 'n/a'}\n"
            f"last_scan_utc: {last_scan}\n"
            f"tracked_alerts: {n_alerts}\n"
            f"cooldown(h): {PRIME_COOLDOWN_SEC / 3600:.0f}\n"
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "test":
        if not MOMO_PRIME_CHAT_ID:
            await update.effective_message.reply_text("MOMO_PRIME_CHAT_ID yok. Env ayarla.")
            return
        try:
            await context.bot.send_message(
                chat_id=MOMO_PRIME_CHAT_ID,
                text="🐳🔥 <b>MOMO PRIME BALİNA</b>\n\nTest mesajı ✅",
                parse_mode=ParseMode.HTML
            )
            await update.effective_message.reply_text("Test gönderildi ✅")
        except Exception as e:
            await update.effective_message.reply_text(f"Test hata: {e}")
        return

    await update.effective_message.reply_text("Bilinmeyen alt komut. /prime help")


def register_momo_prime(app: Application) -> None:
    app.add_handler(CommandHandler("prime", cmd_prime))


# ==========================
# Scheduled job
# ==========================
async def job_momo_prime_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not MOMO_PRIME_ENABLED:
        return
    if not MOMO_PRIME_CHAT_ID:
        return

    now_ts = time.time()

    # Ensure state files exist
    st = _load_json(PRIME_STATE_FILE, _default_prime_state())
    la = _load_json(PRIME_LAST_ALERT_FILE, _default_last_alert())

    last_alert_by_symbol = la.get("last_alert_by_symbol") or {}

    # Fast candidates from TradingView
    rows = _tv_scan_rows()
    if not rows:
        st["scan"]["last_scan_utc"] = _utc_now_iso()
        _save_json(PRIME_STATE_FILE, st)
        return

    # Filter by pct band first
    candidates = []
    for r in rows:
        ticker = (r.get("symbol") or "").strip().upper()
        pct = float(r.get("change_pct") or 0.0)
        vol = float(r.get("volume") or 0.0)

        phase = _phase_from_pct(pct)
        if not phase:
            continue
        if vol <= 0:
            continue

        candidates.append((ticker, pct, phase, vol))

    # Hard cap to avoid burst
    candidates = candidates[:25]

    sent_any = False

    for (ticker, pct, phase, today_vol) in candidates:
        metrics = _compute_prime_metrics(ticker, today_vol)
        if not metrics:
            continue

        r20 = float(metrics["vol_ratio_20d"])
        r400 = float(metrics["vol_ratio_400d"])
        p1 = metrics.get("pos_1m")
        p3 = metrics.get("pos_3m")
        p6 = metrics.get("pos_6m")

        msg = _format_prime_message(ticker, pct, phase, today_vol, r20, r400, p1, p3, p6)
        mh = _hash_message(msg)

        if not _should_alert(last_alert_by_symbol, ticker, pct, phase, r20, r400, now_ts, mh):
            continue

        try:
            await context.bot.send_message(
                chat_id=MOMO_PRIME_CHAT_ID,
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            sent_any = True
        except Exception as e:
            logger.error("PRIME send error: %s", e)
            continue

        # Update last-alert authority
        last_alert_by_symbol[ticker] = {
            "last_alert_utc": _utc_now_iso(),
            "last_alert_pct": pct,
            "last_phase": phase,
            "last_vol_ratio_20d": r20,
            "last_vol_ratio_400d": r400,
            "last_position_1m": p1,
            "last_position_3m": p3,
            "last_position_6m": p6,
            "last_message_hash": mh
        }

    la["last_alert_by_symbol"] = last_alert_by_symbol
    _save_json(PRIME_LAST_ALERT_FILE, la)

    st["scan"]["last_scan_utc"] = _utc_now_iso()
    _save_json(PRIME_STATE_FILE, st)

    if sent_any:
        logger.info("PRIME: sent alerts")
