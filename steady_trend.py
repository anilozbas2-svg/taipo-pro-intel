import os
import time
import logging
from typing import Dict, Any, List, Tuple, Optional

logger = logging.getLogger(__name__)

# -------------------------
# ENV
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

STEADY_TREND_ENABLED = _env_bool("STEADY_TREND_ENABLED", False)
STEADY_TREND_CHAT_ID = os.getenv("STEADY_TREND_CHAT_ID", "").strip()
STEADY_TREND_INTERVAL_MIN = _env_int("STEADY_TREND_INTERVAL_MIN", 2)

# Ağır tren filtreleri
STEADY_TREND_MIN_PCT = _env_float("STEADY_TREND_MIN_PCT", 0.60)      # intraday toplam %
STEADY_TREND_MAX_PCT = _env_float("STEADY_TREND_MAX_PCT", 2.20)      # çok kaçtıysa artık “tren” değil
STEADY_TREND_MIN_DELTA = _env_float("STEADY_TREND_MIN_DELTA", 0.10)  # son 15dk / bar artış %
STEADY_TREND_MIN_VOL_SPIKE = _env_float("STEADY_TREND_MIN_VOL_SPIKE", 1.05)  # vol/avg10
STEADY_TREND_MIN_CONSISTENCY = _env_float("STEADY_TREND_MIN_CONSISTENCY", 0.62)  # +bar oranı

# Spam önleme
STEADY_TREND_COOLDOWN_MIN = _env_int("STEADY_TREND_COOLDOWN_MIN", 45)

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
# Helpers (adapter pattern)
# main.py içinden sağlanacak fonksiyonlar:
# - bist_session_open() -> bool
# - fetch_universe_rows() -> List[Dict[str, Any]]
#   (her satır: symbol, last, day_open, pct_day, delta_15m, vol_spike_10g, bars_up_ratio, score vb.)
# - telegram_send(chat_id, text)
# -------------------------

def _format_msg(row: Dict[str, Any]) -> str:
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

    lines = []
    lines.append("🚄 STEADY TREND – AĞIR TREN")
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
    """
    Ağır tren skoru: düzen + küçük delta + yeterli hacim + günlük band içinde
    """
    pct_day = float(row.get("pct_day", 0.0))
    delta_15m = float(row.get("delta_15m", 0.0))
    vol_spike = float(row.get("vol_spike_10g", 0.0))
    cons = float(row.get("bars_up_ratio", 0.0))

    # normalize / ağırlıklar
    # Not: bu skor “trend” için; spike gibi 3-5x vol istemiyoruz.
    s = 0.0
    s += cons * 3.0
    s += min(delta_15m, 1.0) * 2.0
    s += min(vol_spike, 2.0) * 1.5
    s += min(max((pct_day - STEADY_TREND_MIN_PCT), 0.0), 2.0) * 0.8
    return s

async def job_steady_trend_scan(context, bist_session_open, fetch_universe_rows, telegram_send) -> None:
    """
    context: PTB Context
    bist_session_open: callable -> bool
    fetch_universe_rows: callable -> List[Dict]
    telegram_send: async callable(chat_id, text)
    """
    if not bist_session_open():
        logger.info("STEADY_TREND: session_closed -> return")
        return

    if not STEADY_TREND_ENABLED:
        logger.info("STEADY_TREND: disabled -> return")
        return

    if not STEADY_TREND_CHAT_ID:
        logger.warning("STEADY_TREND: missing STEADY_TREND_CHAT_ID -> return")
        return

    rows = fetch_universe_rows()
    if not rows:
        logger.info("STEADY_TREND: no rows")
        return

    picks: List[Dict[str, Any]] = []
    for r in rows:
        ok, reason = _passes_filters(r)
        if not ok:
            continue
        r["steady_score"] = _steady_score(r)
        picks.append(r)

    if not picks:
        logger.info("STEADY_TREND: no alerts")
        return

    # En iyi 1-3 taneyi seç (spam olmasın)
    picks.sort(key=lambda x: float(x.get("steady_score", 0.0)), reverse=True)
    top = picks[:3]

    sent_any = 0
    for r in top:
        sym = r.get("symbol", "")
        if not sym:
            continue
        if not _cooldown_ok(sym):
            continue
        msg = _format_msg(r)
        await telegram_send(STEADY_TREND_CHAT_ID, msg)
        sent_any += 1

    logger.info("STEADY_TREND: sent=%d top=%d", sent_any, len(top))

def register_steady_trend(scheduler, make_job_wrapper):
    """
    scheduler: APScheduler instance (senin main.py’de kullandığın)
    make_job_wrapper: main.py’de var olan wrapper (async job’u çalıştıran)
    """
    if not STEADY_TREND_ENABLED:
        logger.info("STEADY_TREND: register skipped (disabled)")
        return

    # interval dakikada bir tarama
    scheduler.add_job(
        make_job_wrapper("steady_trend_scan"),
        "interval",
        minutes=STEADY_TREND_INTERVAL_MIN,
        id="steady_trend_scan_repeating",
        replace_existing=True,
    )
    logger.info("STEADY_TREND: registered interval=%dmin", STEADY_TREND_INTERVAL_MIN)
