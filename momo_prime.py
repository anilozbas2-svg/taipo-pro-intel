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

# Watchlist advantage (cooldown reduction factor when ticker is already in watchlist)
PRIME_WATCHLIST_COOLDOWN_FACTOR = float(os.getenv("PRIME_WATCHLIST_COOLDOWN_FACTOR", "0.5"))  # 0.5 => half cooldown

# TradingView scanner
TV_SCAN_URL = os.getenv("MOMO_PRIME_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = int(os.getenv("MOMO_PRIME_TV_TIMEOUT", "12"))

# Yahoo (only for averages/position windows)
YAHOO_TIMEOUT = int(os.getenv("MOMO_PRIME_YAHOO_TIMEOUT", "12"))
YAHOO_SUFFIX = os.getenv("MOMO_PRIME_YAHOO_SUFFIX", ".IS").strip()  # BIST

# Rate-limit protections
MOMO_PRIME_YAHOO_MAX_PER_SCAN = int(os.getenv("MOMO_PRIME_YAHOO_MAX_PER_SCAN", "3"))
MOMO_PRIME_YAHOO_BLOCK_SEC = int(os.getenv("MOMO_PRIME_YAHOO_BLOCK_SEC", "900"))  # 15 min

# State files (isolated)
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
PRIME_STATE_FILE = os.path.join(DATA_DIR, "momo_prime_state.json")
PRIME_LAST_ALERT_FILE = os.path.join(DATA_DIR, "momo_prime_last_alert.json")

# ==========================
# PRIME WATCHLIST (for KILIT)
# ==========================
PRIME_WATCHLIST_FILE = os.path.join(DATA_DIR, "momo_prime_watchlist.json")
PRIME_WATCHLIST_MAX = int(os.getenv("PRIME_WATCHLIST_MAX", "180"))  # liste şişmesin


def _prime_watchlist_default() -> dict:
    return {
        "schema_version": "1.0",
        "system": "momo_prime_watchlist",
        "updated_utc": None,
        "symbols": []
    }


def _prime_watchlist_load() -> dict:
    try:
        if not os.path.exists(PRIME_WATCHLIST_FILE):
            return _prime_watchlist_default()
        with open(PRIME_WATCHLIST_FILE, "r", encoding="utf-8") as f:
            d = json.load(f) or {}
        if "symbols" not in d:
            d["symbols"] = []
        return d
    except Exception as e:
        logger.exception("PRIME watchlist load error: %s", e)
        return _prime_watchlist_default()


def _prime_watchlist_save(d: dict) -> None:
    try:
        os.makedirs(os.path.dirname(PRIME_WATCHLIST_FILE), exist_ok=True)
        tmp = PRIME_WATCHLIST_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PRIME_WATCHLIST_FILE)
    except Exception as e:
        logger.exception("PRIME watchlist save error: %s", e)


def _prime_watchlist_normalize(sym: str) -> str:
    s = (sym or "").strip().upper()
    if ":" in s:
        s = s.split(":")[-1].strip()
    # only keep alnum
    s = "".join([c for c in s if c.isalnum()]).upper()
    return s


def prime_watchlist_add(symbol: str) -> None:
    s = _prime_watchlist_normalize(symbol)
    if not s:
        return

    d = _prime_watchlist_load()
    syms = [_prime_watchlist_normalize(x) for x in (d.get("symbols") or [])]
    syms = [x for x in syms if x]

    if s in syms:
        syms = [x for x in syms if x != s]
        syms.insert(0, s)
    else:
        syms.insert(0, s)

    if len(syms) > PRIME_WATCHLIST_MAX:
        syms = syms[:PRIME_WATCHLIST_MAX]

    d["symbols"] = syms
    d["updated_utc"] = _utc_now_iso()
    _prime_watchlist_save(d)


def prime_watchlist_remove(symbol: str) -> bool:
    s = _prime_watchlist_normalize(symbol)
    if not s:
        return False

    d = _prime_watchlist_load()
    syms = [_prime_watchlist_normalize(x) for x in (d.get("symbols") or [])]
    syms = [x for x in syms if x]

    if s not in syms:
        return False

    syms = [x for x in syms if x != s]
    d["symbols"] = syms
    d["updated_utc"] = _utc_now_iso()
    _prime_watchlist_save(d)
    return True


def prime_watchlist_list() -> List[str]:
    d = _prime_watchlist_load()
    syms = d.get("symbols") or []

    out: List[str] = []
    seen: set = set()

    for x in syms:
        s = _prime_watchlist_normalize(str(x))
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)

    return out


def prime_watchlist_clear() -> None:
    wl = _load_json(PRIME_WATCHLIST_FILE, {"symbols": []})
    wl["symbols"] = []
    wl["updated_utc"] = _utc_now_iso()
    _save_json(PRIME_WATCHLIST_FILE, wl)


def prime_watchlist_peek(limit: int = 25) -> List[str]:
    d = _prime_watchlist_load()
    syms = [_prime_watchlist_normalize(x) for x in (d.get("symbols") or [])]
    syms = [x for x in syms if x]
    lim = max(1, int(limit))
    return syms[:lim]


# Small caches (RAM)
_YAHOO_CACHE: Dict[str, Dict[str, Any]] = {}
_YAHOO_CACHE_TTL = int(os.getenv("MOMO_PRIME_YAHOO_CACHE_TTL", "1800"))  # 30 min

# Global Yahoo backoff (in-memory)
YAHOO_BLOCKED_UNTIL_TS = 0.0


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


# ==========================
# PRIME state defaults
# ==========================
def _default_prime_state() -> dict:
    return {
        "schema_version": "1.1",
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
            "watchlist_cooldown_factor": PRIME_WATCHLIST_COOLDOWN_FACTOR,
            "reference_windows_days": [10, 20, 400],
            "position_windows_days": [30, 90, 180],
            "yahoo_max_per_scan": MOMO_PRIME_YAHOO_MAX_PER_SCAN,
            "yahoo_block_sec": MOMO_PRIME_YAHOO_BLOCK_SEC
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


def _cooldown_ok(last_alert_ts: Optional[float], now_ts: float, cooldown_sec: int = PRIME_COOLDOWN_SEC) -> bool:
    if last_alert_ts is None:
        return True
    return (now_ts - last_alert_ts) >= int(cooldown_sec)


def _pct_position(close: float, low: float, high: float) -> Optional[float]:
    try:
        if any(map(lambda v: v is None or (isinstance(v, float) and math.isnan(v)), [close, low, high])):
            return None
        if high <= low:
            return None
        return max(0.0, min(1.0, (close - low) / (high - low)))
    except Exception:
        return None


def _watchlist_cooldown_seconds(is_in_watchlist: bool) -> int:
    if not is_in_watchlist:
        return int(PRIME_COOLDOWN_SEC)
    factor = PRIME_WATCHLIST_COOLDOWN_FACTOR
    try:
        f = float(factor)
    except Exception:
        f = 0.5
    f = max(0.1, min(1.0, f))
    return max(60, int(PRIME_COOLDOWN_SEC * f))


# ==========================
# TradingView scan (fast filter)
# Adds avg_volume_10d_calc => r10 fallback
# ==========================
def _tv_scan_rows() -> List[dict]:
    payload = {
        "filter": [
            {"left": "market_cap_basic", "operation": "nempty"},
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"}
        ],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "change", "volume", "close", "average_volume_10d_calc"],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, 200]
    }
    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}

        out: List[dict] = []
        for row in data.get("data", []) or []:
            d = row.get("d") or []
            if len(d) < 4:
                continue

            sym = str(d[0]).strip().upper()
            chg = _safe_float(d[1])
            vol = _safe_float(d[2])
            cls = _safe_float(d[3])
            av10 = _safe_float(d[4]) if len(d) >= 5 else None

            if not sym or chg is None or vol is None or cls is None:
                continue

            out.append({
                "symbol": sym,
                "change_pct": chg,
                "volume": vol,
                "close": cls,
                "avg10": av10
            })
        return out
    except Exception as e:
        logger.error("PRIME TV scan error: %s", e)
        return []


# ==========================
# Yahoo chart (averages & position)
# ==========================
def _yahoo_allowed_now() -> bool:
    global YAHOO_BLOCKED_UNTIL_TS
    return time.time() >= YAHOO_BLOCKED_UNTIL_TS


def _yahoo_block_now() -> None:
    global YAHOO_BLOCKED_UNTIL_TS
    YAHOO_BLOCKED_UNTIL_TS = time.time() + float(MOMO_PRIME_YAHOO_BLOCK_SEC)


def _yahoo_chart(symbol: str) -> Optional[dict]:
    if not _yahoo_allowed_now():
        return None

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

        if r.status_code == 429:
            _yahoo_block_now()
            logger.error("PRIME Yahoo 429 -> blocked for %ss", MOMO_PRIME_YAHOO_BLOCK_SEC)
            return None

        r.raise_for_status()
        js = r.json() or {}
        _YAHOO_CACHE[symbol] = {"ts": now, "data": js}
        return js
    except Exception as e:
        try:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 429:
                _yahoo_block_now()
                logger.error("PRIME Yahoo 429 (exception) -> blocked for %ss", MOMO_PRIME_YAHOO_BLOCK_SEC)
                return None
        except Exception:
            pass

        logger.warning("PRIME Yahoo error %s: %s", symbol, e)
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
        "vol_ratio_20d": float(r20),
        "vol_ratio_400d": float(r400),
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
    r10: Optional[float],
    r20: Optional[float],
    r400: Optional[float],
    p1: Optional[float],
    p3: Optional[float],
    p6: Optional[float],
    yahoo_ok: bool
) -> str:
    if phase == "CORE":
        header = "🔵🐳 <b>CORE PRIME – MOMO BALİNA</b>"
        mentor = "🧠 <i>Mentor notu:</i> Erken faz. Balina yeni girdi → radar aç."
    else:
        header = "🟠⚠️ <b>LATE PRIME – MOMO BALİNA</b>"
        mentor = "🧠 <i>Mentor notu:</i> Geç faz. Teyit ve takip önemli → dikkatli."

    if yahoo_ok:
        yline = ""
    else:
        yline = "⚠️ <b>PRIME-LITE:</b> Yahoo teyidi yok (429/erişim/limit). TV avg10 ile gönderildi.\n"

    parts = []
    parts.append(f"{header}\n\n")
    parts.append(f"<b>HİSSE:</b> {ticker}\n")
    parts.append(f"<b>İLK SİNYAL:</b> {pct:+.2f}%  <b>({phase})</b>\n")

    vol_line = "<b>HACİM:</b> "
    vol_bits = []
    if r10 is not None:
        vol_bits.append(f"{r10:.2f}x (10g-TV)")
    if r20 is not None:
        vol_bits.append(f"{r20:.2f}x (20g)")
    if r400 is not None:
        vol_bits.append(f"{r400:.2f}x (400g)")
    if not vol_bits:
        vol_bits.append("n/a")
    vol_line += " | ".join(vol_bits) + "\n"
    parts.append(vol_line)

    parts.append(f"{yline}")

    if yahoo_ok:
        parts.append(f"<b>DİP-TEPE KONUM:</b> 1A {_fmt_pos(p1)} | 3A {_fmt_pos(p3)} | 6A {_fmt_pos(p6)}\n\n")
    else:
        parts.append("<b>DİP-TEPE KONUM:</b> n/a (Yahoo yok)\n\n")

    parts.append(f"{mentor}\n")
    parts.append(f"⏱ {datetime.now().strftime('%H:%M')}")

    return "".join(parts)


# ==========================
# PRIME decision
# Uses max(r10, r20, r400) for volume rule
# ==========================
def _should_alert(
    last_alert_map: dict,
    ticker: str,
    pct: float,
    phase: str,
    r10: Optional[float],
    r20: Optional[float],
    r400: Optional[float],
    now_ts: float,
    message_hash: str,
    cooldown_sec: int
) -> bool:
    entry = (last_alert_map.get(ticker) or {})
    last_ts = _parse_utc_iso(entry.get("last_alert_utc"))
    if not _cooldown_ok(last_ts, now_ts, cooldown_sec):
        return False

    ratios = [v for v in [r10, r20, r400] if isinstance(v, (int, float)) and v is not None]
    if not ratios:
        return False
    if max(ratios) < PRIME_VOL_RATIO_MIN:
        return False

    if entry.get("last_message_hash") == message_hash:
        return False

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
            "• /prime status        → PRIME durum\n"
            "• /prime test          → test mesajı\n\n"
            "• /prime force THYAO   → watchlist’e elle ekle\n"
            "• /prime remove THYAO  → watchlist’ten sil\n"
            "• /prime watchlist     → watchlist’i göster\n"
            "• /prime top           → watchlist top (ilk 15)\n"
            "• /prime clear         → watchlist’i temizle\n\n"
            "Not: PRIME tarama 3 dk; koşullar: %0.30–%0.80 + hacim ≥1.8x (10g-TV / 20g / 400g).\n"
            f"Cooldown: {int(PRIME_COOLDOWN_SEC / 3600)} saat | Watchlist avantaj: x{PRIME_WATCHLIST_COOLDOWN_FACTOR} cooldown\n"
            f"Yahoo max/scan: {MOMO_PRIME_YAHOO_MAX_PER_SCAN} | Yahoo block: {int(MOMO_PRIME_YAHOO_BLOCK_SEC / 60)} dk"
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "status":
        st = _load_json(PRIME_STATE_FILE, _default_prime_state())
        la = _load_json(PRIME_LAST_ALERT_FILE, _default_last_alert())
        last_scan = ((st.get("scan") or {}).get("last_scan_utc")) or "n/a"
        n_alerts = len((la.get("last_alert_by_symbol") or {}))
        blocked_left = max(0, int(YAHOO_BLOCKED_UNTIL_TS - time.time()))
        txt = (
            "🐳🔥 PRIME STATUS\n\n"
            f"enabled: {int(MOMO_PRIME_ENABLED)}\n"
            f"chat_id: {MOMO_PRIME_CHAT_ID or 'n/a'}\n"
            f"last_scan_utc: {last_scan}\n"
            f"tracked_alerts: {n_alerts}\n"
            f"cooldown(h): {PRIME_COOLDOWN_SEC / 3600:.0f}\n"
            f"watchlist_cd_factor: {PRIME_WATCHLIST_COOLDOWN_FACTOR}\n"
            f"yahoo_allowed: {int(_yahoo_allowed_now())}\n"
            f"yahoo_block_left_sec: {blocked_left}\n"
            f"yahoo_max_per_scan: {MOMO_PRIME_YAHOO_MAX_PER_SCAN}\n"
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

    if sub == "force":
        if len(context.args) < 2:
            await update.effective_message.reply_text("Kullanım: /prime force THYAO")
            return

        ticker = _prime_watchlist_normalize(context.args[1] or "")
        if not ticker:
            await update.effective_message.reply_text("Ticker boş olamaz.")
            return

        prime_watchlist_add(ticker)
        wl = prime_watchlist_list()
        await update.effective_message.reply_text(
            f"✅ PRIME force eklendi: {ticker}\nwatchlist_count: {len(wl)}"
        )
        return

    if sub == "remove":
        if len(context.args) < 2:
            await update.effective_message.reply_text("Kullanım: /prime remove THYAO")
            return

        ticker = _prime_watchlist_normalize(context.args[1] or "")
        if not ticker:
            await update.effective_message.reply_text("Ticker boş olamaz.")
            return

        ok = prime_watchlist_remove(ticker)
        wl = prime_watchlist_list()
        if ok:
            await update.effective_message.reply_text(f"🗑️ PRIME kaldırıldı: {ticker}\nwatchlist_count: {len(wl)}")
        else:
            await update.effective_message.reply_text(f"⚠️ Listede yok: {ticker}\nwatchlist_count: {len(wl)}")
        return

    if sub == "top":
        wl = prime_watchlist_list()
        if not wl:
            await update.effective_message.reply_text("📌 PRIME watchlist boş.")
            return

        top = wl[:15]
        txt = "🔥 PRIME TOP\n\n" + "\n".join([f"• {x}" for x in top])
        if len(wl) > len(top):
            txt += f"\n\n(+{len(wl) - len(top)} daha)"
        await update.effective_message.reply_text(txt)
        return

    if sub in ("watchlist", "list"):
        wl = prime_watchlist_list()
        if not wl:
            await update.effective_message.reply_text("📌 PRIME watchlist boş.")
            return

        top = wl[:40]
        txt = "📌 PRIME WATCHLIST\n\n" + "\n".join([f"• {x}" for x in top])
        if len(wl) > len(top):
            txt += f"\n\n(+{len(wl) - len(top)} daha)"

        await update.effective_message.reply_text(txt)
        return

    if sub == "clear":
        prime_watchlist_clear()
        await update.effective_message.reply_text("🧹 PRIME watchlist temizlendi.")
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

    st = _load_json(PRIME_STATE_FILE, _default_prime_state())
    la = _load_json(PRIME_LAST_ALERT_FILE, _default_last_alert())
    last_alert_by_symbol = la.get("last_alert_by_symbol") or {}

    # snapshot watchlist once per scan (fast + consistent)
    wl_now = set(prime_watchlist_peek(200))

    rows = _tv_scan_rows()
    st["scan"]["last_scan_utc"] = _utc_now_iso()
    _save_json(PRIME_STATE_FILE, st)

    if not rows:
        return

    pre_candidates: List[Tuple[str, float, str, float, Optional[float]]] = []
    for r in rows:
        ticker = (r.get("symbol") or "").strip().upper()
        pct = float(r.get("change_pct") or 0.0)
        vol = float(r.get("volume") or 0.0)
        av10 = _safe_float(r.get("avg10"))

        phase = _phase_from_pct(pct)
        if not phase:
            continue
        if vol <= 0:
            continue

        is_in_watch = _prime_watchlist_normalize(ticker) in wl_now
        cd = _watchlist_cooldown_seconds(is_in_watch)

        entry = last_alert_by_symbol.get(ticker) or {}
        last_ts = _parse_utc_iso(entry.get("last_alert_utc"))
        if not _cooldown_ok(last_ts, now_ts, cd):
            continue

        pre_candidates.append((ticker, pct, phase, vol, av10))

    pre_candidates = pre_candidates[:25]

    sent_any = False
    yahoo_used = 0

    for (ticker, pct, phase, today_vol, av10) in pre_candidates:
        is_in_watch = _prime_watchlist_normalize(ticker) in wl_now
        cd = _watchlist_cooldown_seconds(is_in_watch)

        r10 = None
        if av10 is not None and av10 > 0:
            r10 = today_vol / av10

        yahoo_ok = False
        r20 = None
        r400 = None
        p1 = None
        p3 = None
        p6 = None

        if _yahoo_allowed_now() and yahoo_used < MOMO_PRIME_YAHOO_MAX_PER_SCAN:
            metrics = _compute_prime_metrics(ticker, today_vol)
            yahoo_used += 1
            if metrics:
                yahoo_ok = True
                r20 = float(metrics.get("vol_ratio_20d")) if metrics.get("vol_ratio_20d") is not None else None
                r400 = float(metrics.get("vol_ratio_400d")) if metrics.get("vol_ratio_400d") is not None else None
                p1 = metrics.get("pos_1m")
                p3 = metrics.get("pos_3m")
                p6 = metrics.get("pos_6m")

        msg = _format_prime_message(
            ticker=ticker,
            pct=pct,
            phase=phase,
            r10=r10,
            r20=r20,
            r400=r400,
            p1=p1,
            p3=p3,
            p6=p6,
            yahoo_ok=yahoo_ok
        )
        mh = _hash_message(msg)

        if not _should_alert(last_alert_by_symbol, ticker, pct, phase, r10, r20, r400, now_ts, mh, cd):
            continue

        try:
            await context.bot.send_message(
                chat_id=MOMO_PRIME_CHAT_ID,
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            prime_watchlist_add(ticker)
            # keep local snapshot fresh for the remainder of scan
            wl_now.add(_prime_watchlist_normalize(ticker))
            sent_any = True
        except Exception as e:
            logger.error("PRIME send error: %s", e)
            continue

        last_alert_by_symbol[ticker] = {
            "last_alert_utc": _utc_now_iso(),
            "last_alert_pct": pct,
            "last_phase": phase,
            "last_vol_ratio_10d_tv": r10,
            "last_vol_ratio_20d": r20,
            "last_vol_ratio_400d": r400,
            "last_position_1m": p1,
            "last_position_3m": p3,
            "last_position_6m": p6,
            "yahoo_ok": int(yahoo_ok),
            "last_message_hash": mh,
            "cooldown_used_sec": int(cd),
            "in_watchlist": int(is_in_watch)
        }

    la["last_alert_by_symbol"] = last_alert_by_symbol
    _save_json(PRIME_LAST_ALERT_FILE, la)

    if sent_any:
        logger.info("PRIME: sent alerts (yahoo_used=%d, yahoo_allowed=%d)", yahoo_used, int(_yahoo_allowed_now()))
    else:
        logger.info("PRIME: no alerts (yahoo_used=%d, yahoo_allowed=%d)", yahoo_used, int(_yahoo_allowed_now()))
