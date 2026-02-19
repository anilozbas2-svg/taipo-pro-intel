import os
import json
import time
import logging
from typing import Dict, Any, List, Tuple, Optional

logger = logging.getLogger(__name__)

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

# -------------------------
# ENV
# -------------------------
STEADY_TREND_ENABLED = _env_bool("STEADY_TREND_ENABLED", False)
STEADY_TREND_CHAT_ID = os.getenv("STEADY_TREND_CHAT_ID", "").strip()
STEADY_TREND_INTERVAL_MIN = _env_int("STEADY_TREND_INTERVAL_MIN", 2)

# Ağır tren filtreleri
STEADY_TREND_MIN_PCT = _env_float("STEADY_TREND_MIN_PCT", 0.60)              # intraday toplam %
STEADY_TREND_MAX_PCT = _env_float("STEADY_TREND_MAX_PCT", 2.20)              # çok kaçtıysa artık “tren” değil
STEADY_TREND_MIN_DELTA = _env_float("STEADY_TREND_MIN_DELTA", 0.10)          # son 15dk artış %
STEADY_TREND_MIN_VOL_SPIKE = _env_float("STEADY_TREND_MIN_VOL_SPIKE", 1.05)  # vol/avg10 (TV)
STEADY_TREND_MIN_CONSISTENCY = _env_float("STEADY_TREND_MIN_CONSISTENCY", 0.62)  # +bar oranı

# Spam önleme
STEADY_TREND_COOLDOWN_MIN = _env_int("STEADY_TREND_COOLDOWN_MIN", 45)

# Watchlist avantajı (PRIME ile birleşince burası altın)
STEADY_TREND_WATCHLIST_SCORE_BOOST = _env_float("STEADY_TREND_WL_SCORE_BOOST", 1.25)  # score'a ek boost
STEADY_TREND_WATCHLIST_COOLDOWN_MULT = _env_float("STEADY_TREND_WL_COOLDOWN_MULT", 0.50)  # cooldown x0.5
STEADY_TREND_AUTO_ADD_TO_PRIME_WATCHLIST = _env_bool("STEADY_TREND_AUTO_ADD_TO_PRIME_WL", True)

# Persist state
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
STEADY_STATE_FILE = os.path.join(DATA_DIR, "steady_trend_state.json")
STEADY_STATE_MAX = _env_int("STEADY_TREND_STATE_MAX", 2000)

# -------------------------
# State helpers (persist cooldown)
# -------------------------
def _now_ts() -> float:
    return time.time()

def _load_state() -> dict:
    try:
        if not os.path.exists(STEADY_STATE_FILE):
            return {"last_sent_ts": {}}
        with open(STEADY_STATE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f) or {}
        if "last_sent_ts" not in d or not isinstance(d["last_sent_ts"], dict):
            d["last_sent_ts"] = {}
        return d
    except Exception as e:
        logger.warning("STEADY_TREND: state load error: %s", e)
        return {"last_sent_ts": {}}

def _save_state(d: dict) -> None:
    try:
        os.makedirs(os.path.dirname(STEADY_STATE_FILE), exist_ok=True)
        tmp = STEADY_STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STEADY_STATE_FILE)
    except Exception as e:
        logger.warning("STEADY_TREND: state save error: %s", e)

def _prune_state(last_sent_ts: Dict[str, float]) -> Dict[str, float]:
    if len(last_sent_ts) <= STEADY_STATE_MAX:
        return last_sent_ts
    items = sorted(last_sent_ts.items(), key=lambda kv: float(kv[1]), reverse=True)
    items = items[:STEADY_STATE_MAX]
    return {k: float(v) for k, v in items}

def _cooldown_ok(symbol: str, is_watchlist: bool, state: dict) -> bool:
    now = _now_ts()
    last_map = state.get("last_sent_ts") or {}
    last = float(last_map.get(symbol, 0.0) or 0.0)

    cooldown_sec = float(STEADY_TREND_COOLDOWN_MIN) * 60.0
    if is_watchlist:
        cooldown_sec *= max(0.1, float(STEADY_TREND_WATCHLIST_COOLDOWN_MULT))

    if now - last < cooldown_sec:
        return False

    last_map[symbol] = now
    state["last_sent_ts"] = _prune_state(last_map)
    return True

# -------------------------
# Formatting / filters
# -------------------------
def _format_msg(row: Dict[str, Any], is_watchlist: bool) -> str:
    sym = row.get("symbol", "?")
    last = row.get("last", None)
    pct_day = row.get("pct_day", None)
    delta_15m = row.get("delta_15m", None)
    vol_spike = row.get("vol_spike_10g", None)
    cons = row.get("bars_up_ratio", None)
    score = row.get("steady_score", None)

    def fnum(x, nd=2):
        try:
            return f"{float(x):.{nd}f}"
        except Exception:
            return "n/a"

    tag = "✅ WATCHLIST" if is_watchlist else "🟦 SERBEST"

    lines: List[str] = []
    lines.append("🚄 STEADY TREND – AĞIR TREN")
    lines.append(f"{tag}")
    lines.append("")
    lines.append(f"HİSSE: {sym}")
    lines.append(f"FİYAT: {fnum(last, 2)}")
    lines.append(f"GÜNLÜK: +{fnum(pct_day, 2)}%")
    lines.append(f"DELTA(15d): +{fnum(delta_15m, 2)}%")
    lines.append(f"HACİM: {fnum(vol_spike, 2)}x (10g-TV)")
    lines.append(f"İSTİKRAR: {fnum(cons, 2)} (up-bar oranı)")
    lines.append(f"SKOR: {fnum(score, 2)}")
    lines.append("")
    lines.append("🧠 Mentor notu: Spike değil; düzenli tırmanış. Takipte kal.")
    return "\n".join(lines)

def _passes_filters(row: Dict[str, Any]) -> Tuple[bool, str]:
    pct_day = row.get("pct_day", None)
    delta_15m = row.get("delta_15m", None)
    vol_spike = row.get("vol_spike_10g", None)
    cons = row.get("bars_up_ratio", None)

    try:
        pct_day_f = float(pct_day)
        delta_f = float(delta_15m)
        vol_f = float(vol_spike)
        cons_f = float(cons)
    except Exception:
        return (False, "bad_data")

    if pct_day_f < STEADY_TREND_MIN_PCT:
        return (False, "pct_low")
    if pct_day_f > STEADY_TREND_MAX_PCT:
        return (False, "pct_too_high")
    if delta_f < STEADY_TREND_MIN_DELTA:
        return (False, "delta_low")
    if vol_f < STEADY_TREND_MIN_VOL_SPIKE:
        return (False, "vol_low")
    if cons_f < STEADY_TREND_MIN_CONSISTENCY:
        return (False, "consistency_low")

    return (True, "ok")

def _steady_score(row: Dict[str, Any]) -> float:
    pct_day = float(row.get("pct_day", 0.0))
    delta_15m = float(row.get("delta_15m", 0.0))
    vol_spike = float(row.get("vol_spike_10g", 0.0))
    cons = float(row.get("bars_up_ratio", 0.0))

    s = 0.0
    s += cons * 3.0
    s += min(delta_15m, 1.0) * 2.0
    s += min(vol_spike, 2.0) * 1.5
    s += min(max((pct_day - STEADY_TREND_MIN_PCT), 0.0), 2.0) * 0.8
    return s

def _safe_symbol(x: Any) -> str:
    s = (str(x or "").strip().upper())
    s = "".join([c for c in s if c.isalnum()])
    return s

# -------------------------
# JOB (PTB JobQueue uyumlu: sadece context alır)
# main.py bot_data adapter'ları:
#   app.bot_data["bist_session_open"] = bist_session_open
#   app.bot_data["fetch_universe_rows"] = fetch_universe_rows
#   app.bot_data["telegram_send"] = telegram_send
#
# PRIME entegrasyon adapter'ları (opsiyonel):
#   app.bot_data["prime_watchlist_list"] = prime_watchlist_list   -> List[str]
#   app.bot_data["prime_watchlist_add"] = prime_watchlist_add     -> (symbol: str) -> None
# -------------------------
async def job_steady_trend_scan(context) -> None:
    if not STEADY_TREND_ENABLED:
        logger.info("STEADY_TREND: disabled -> return")
        return

    if not STEADY_TREND_CHAT_ID:
        logger.warning("STEADY_TREND: missing STEADY_TREND_CHAT_ID -> return")
        return

    bist_session_open = context.application.bot_data.get("bist_session_open")
    fetch_universe_rows = context.application.bot_data.get("fetch_universe_rows")
    telegram_send = context.application.bot_data.get("telegram_send")

    if not bist_session_open or not fetch_universe_rows or not telegram_send:
        logger.warning("STEADY_TREND: missing adapters in bot_data -> return")
        return

    prime_watchlist_list = context.application.bot_data.get("prime_watchlist_list")
    prime_watchlist_add = context.application.bot_data.get("prime_watchlist_add")

    watchlist: List[str] = []
    try:
        if callable(prime_watchlist_list):
            watchlist = [ _safe_symbol(s) for s in (prime_watchlist_list() or []) ]
            watchlist = [s for s in watchlist if s]
    except Exception as e:
        logger.warning("STEADY_TREND: prime_watchlist_list error: %s", e)
        watchlist = []

    watchset = set(watchlist)

    try:
        if not bist_session_open():
            logger.info("STEADY_TREND: session_closed -> return")
            return
    except Exception as e:
        logger.warning("STEADY_TREND: bist_session_open error: %s", e)
        return

    try:
        rows = fetch_universe_rows()
    except Exception as e:
        logger.warning("STEADY_TREND: fetch_universe_rows error: %s", e)
        return

    if not rows:
        logger.info("STEADY_TREND: no rows")
        return

    picks: List[Dict[str, Any]] = []
    for r in rows:
        sym = _safe_symbol(r.get("symbol", ""))
        if not sym:
            continue

        ok, _reason = _passes_filters(r)
        if not ok:
            continue

        base_score = _steady_score(r)
        is_wl = sym in watchset
        if is_wl:
            base_score += float(STEADY_TREND_WATCHLIST_SCORE_BOOST)

        r["symbol"] = sym
        r["steady_score"] = base_score
        r["_is_watchlist"] = int(is_wl)
        picks.append(r)

    if not picks:
        logger.info("STEADY_TREND: no alerts")
        return

    # Sıralama: önce watchlist avantajı, sonra score
    picks.sort(
        key=lambda x: (int(x.get("_is_watchlist", 0)), float(x.get("steady_score", 0.0))),
        reverse=True
    )

    # Daha fazla aday alalım ama sadece ilk 3'ü gönderelim (cooldown elemesi yüzünden boş kalmasın)
    candidates = picks[:10]

    state = _load_state()

    sent_any = 0
    sent_syms: List[str] = []

    for r in candidates:
        sym = r.get("symbol", "")
        is_wl = bool(int(r.get("_is_watchlist", 0)))

        if not sym:
            continue

        if not _cooldown_ok(sym, is_wl, state):
            continue

        msg = _format_msg(r, is_wl)

        try:
            await telegram_send(STEADY_TREND_CHAT_ID, msg)
            sent_any += 1
            sent_syms.append(sym)

            # “ağır tren” yakalanınca PRIME watchlist’e de it
            if STEADY_TREND_AUTO_ADD_TO_PRIME_WATCHLIST and callable(prime_watchlist_add):
                try:
                    prime_watchlist_add(sym)
                except Exception as e:
                    logger.warning("STEADY_TREND: prime_watchlist_add error for %s: %s", sym, e)
        except Exception as e:
            logger.warning("STEADY_TREND: telegram_send error for %s: %s", sym, e)
            continue

        if sent_any >= 3:
            break

    _save_state(state)

    logger.info("STEADY_TREND: sent=%d symbols=%s", sent_any, ",".join(sent_syms))
