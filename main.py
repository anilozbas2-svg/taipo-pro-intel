import os
import re
import math
import time
import json
import logging
import asyncio
from datetime import datetime, timedelta, time as dtime, date
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Tuple, Optional

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# ==============================
# Trade Log (Altın Log)
# ==============================

TRADE_LOG_DIR = os.getenv("TRADE_LOG_DIR", "/var/data/logs")
os.makedirs(TRADE_LOG_DIR, exist_ok=True)

TRADE_LOG_FILE = os.path.join(
    TRADE_LOG_DIR,
    f"trades_{datetime.now().year}.jsonl"
)

# =========================================================
# Config
# =========================================================
TZ = ZoneInfo(os.getenv("TZ", "Europe/Istanbul"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

BOT_VERSION = os.getenv(
    "BOT_VERSION",
    "v1.7.0-premium-yahoo-bootstrap-tradingdaykey-torpil-faz2-whale-stable-rejim"
).strip() or "v1.7.0-premium-yahoo-bootstrap-tradingdaykey-torpil-faz2-whale-stable-rejim"

TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12

# -----------------------------
# Alarm config
# -----------------------------
ALARM_ENABLED = os.getenv("ALARM_ENABLED", "1").strip() == "1"
ALARM_CHAT_ID = os.getenv("ALARM_CHAT_ID", "").strip()  # -100...
ALARM_INTERVAL_MIN = int(os.getenv("ALARM_INTERVAL_MIN", "30"))
ALARM_COOLDOWN_MIN = int(os.getenv("ALARM_COOLDOWN_MIN", "60"))

ALARM_START_HOUR = int(os.getenv("ALARM_START_HOUR", "10"))
ALARM_START_MIN = int(os.getenv("ALARM_START_MIN", "0"))
ALARM_END_HOUR = int(os.getenv("ALARM_END_HOUR", "17"))
ALARM_END_MIN = int(os.getenv("ALARM_END_MIN", "30"))

# EOD / Tomorrow
EOD_HOUR = int(os.getenv("EOD_HOUR", "17"))
EOD_MINUTE = int(os.getenv("EOD_MINUTE", "50"))
TOMORROW_DELAY_MIN = int(os.getenv("TOMORROW_DELAY_MIN", "2"))

# =========================================================
# Tomorrow Follow (2-day chain tracking)
# =========================================================
TOMORROW_FOLLOW_ENABLED = int(os.getenv("TOMORROW_FOLLOW_ENABLED", "1")) == 1
TOMORROW_FOLLOW_INTERVAL_MIN = int(os.getenv("TOMORROW_FOLLOW_INTERVAL_MIN", "60"))

# ===========================
# Tomorrow chain RAM cache
# ===========================
TOMORROW_CHAINS: dict = {}

TOMORROW_FOLLOW_START_HOUR = int(os.getenv("TOMORROW_FOLLOW_START_HOUR", "10"))
TOMORROW_FOLLOW_START_MIN = int(os.getenv("TOMORROW_FOLLOW_START_MIN", "30"))
TOMORROW_FOLLOW_END_HOUR = int(os.getenv("TOMORROW_FOLLOW_END_HOUR", "17"))
TOMORROW_FOLLOW_END_MIN = int(os.getenv("TOMORROW_FOLLOW_END_MIN", "30"))

# 2 gün kuralı (T+1 ve T+2) -> sonra kapanır
TOMORROW_CHAIN_MAX_AGE = int(os.getenv("TOMORROW_CHAIN_MAX_AGE", "2"))

WATCHLIST_MAX = int(os.getenv("WATCHLIST_MAX", "12"))
VOLUME_TOP_N = int(os.getenv("VOLUME_TOP_N", "50"))

DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "400"))
ALARM_NOTE_MAX = int(os.getenv("ALARM_NOTE_MAX", "6"))

# Tomorrow list filtreleri
TOMORROW_MAX = int(os.getenv("TOMORROW_MAX", "12"))
TOMORROW_MIN_VOL_RATIO = float(os.getenv("TOMORROW_MIN_VOL_RATIO", "1.20"))
TOMORROW_MAX_BAND = float(os.getenv("TOMORROW_MAX_BAND", "65"))
TOMORROW_INCLUDE_AYRISMA = os.getenv("TOMORROW_INCLUDE_AYRISMA", "0").strip() == "1"

CANDIDATE_MAX = int(os.getenv("CANDIDATE_MAX", "20"))
CANDIDATE_MIN_VOL_RATIO = float(os.getenv("CANDIDATE_MIN_VOL_RATIO", "1.10"))
CANDIDATE_MAX_BAND = float(os.getenv("CANDIDATE_MAX_BAND", "75"))
CANDIDATE_INCLUDE_AYRISMA = os.getenv("CANDIDATE_INCLUDE_AYRISMA", "0").strip() == "1"

# Torpil (disk veri azken)
TORPIL_ENABLED = os.getenv("TORPIL_ENABLED", "1").strip() == "1"
TORPIL_MIN_SAMPLES = int(os.getenv("TORPIL_MIN_SAMPLES", "10"))
TORPIL_MIN_VOL_RATIO = float(os.getenv("TORPIL_MIN_VOL_RATIO", "1.05"))
TORPIL_MAX_BAND = float(os.getenv("TORPIL_MAX_BAND", "75"))

# Yahoo bootstrap
BOOTSTRAP_ON_START = os.getenv("BOOTSTRAP_ON_START", "1").strip() == "1"
BOOTSTRAP_DAYS = int(os.getenv("BOOTSTRAP_DAYS", "400"))
BOOTSTRAP_FORCE = os.getenv("BOOTSTRAP_FORCE", "0").strip() == "1"
YAHOO_TIMEOUT = int(os.getenv("YAHOO_TIMEOUT", "15"))
YAHOO_SLEEP_SEC = float(os.getenv("YAHOO_SLEEP_SEC", "0.15"))

# Whale
WHALE_ENABLED = os.getenv("WHALE_ENABLED", "1").strip() == "1"
WHALE_INTERVAL_MIN = int(os.getenv("WHALE_INTERVAL_MIN", "30"))
WHALE_START_HOUR = int(os.getenv("WHALE_START_HOUR", "10"))
WHALE_START_MIN = int(os.getenv("WHALE_START_MIN", "5"))
WHALE_END_HOUR = int(os.getenv("WHALE_END_HOUR", "11"))
WHALE_END_MIN = int(os.getenv("WHALE_END_MIN", "30"))

WHALE_MIN_VOL_RATIO = float(os.getenv("WHALE_MIN_VOL_RATIO", "1.10"))
WHALE_MAX_DRAWDOWN_PCT = float(os.getenv("WHALE_MAX_DRAWDOWN_PCT", "-0.70"))
WHALE_INDEX_BONUS = os.getenv("WHALE_INDEX_BONUS", "1").strip() == "1"
WHALE_MIN_POSITIVE_WHEN_INDEX_BAD = float(os.getenv("WHALE_MIN_POSITIVE_WHEN_INDEX_BAD", "0.30"))

# -----------------------------
# ✅ REJİM MODU (Endeks + Vol + Gap + Trend)
# -----------------------------
REJIM_ENABLED = os.getenv("REJIM_ENABLED", "1").strip() == "1"
REJIM_LOOKBACK = int(os.getenv("REJIM_LOOKBACK", "50"))
REJIM_VOL_HIGH = float(os.getenv("REJIM_VOL_HIGH", "1.80"))
REJIM_TREND_SMA_FAST = int(os.getenv("REJIM_TREND_SMA_FAST", "20"))
REJIM_TREND_SMA_SLOW = int(os.getenv("REJIM_TREND_SMA_SLOW", "50"))
REJIM_GAP_PCT = float(os.getenv("REJIM_GAP_PCT", "1.20"))
REJIM_PREV_DAY_BAD = float(os.getenv("REJIM_PREV_DAY_BAD", "-1.00"))
REJIM_BLOCK_ON = [x.strip().upper() for x in os.getenv("REJIM_BLOCK_ON", "RISK_OFF").split(",") if x.strip()]

# Gate’ler (blok gününde hangi modül susturulsun)
REJIM_GATE_ALARM = os.getenv("REJIM_GATE_ALARM", "1").strip() == "1"
REJIM_GATE_TOMORROW = os.getenv("REJIM_GATE_TOMORROW", "1").strip() == "1"
REJIM_GATE_RADAR = os.getenv("REJIM_GATE_RADAR", "1").strip() == "1"
REJIM_GATE_EOD = os.getenv("REJIM_GATE_EOD", "0").strip() == "1"
REJIM_GATE_WHALE = os.getenv("REJIM_GATE_WHALE", "1").strip() == "1"

# “Uçanlar” için bilgi etiketi (blok DEĞİL)
REJIM_MOMO_UP_CHG = float(os.getenv("REJIM_MOMO_UP_CHG", "2.20"))   # endeks günlük +%2.2 üstü
REJIM_MOMO_UP_GAP = float(os.getenv("REJIM_MOMO_UP_GAP", "0.80"))   # gap +%0.8 üstü

# ================================
# R0 – EARLY BREAKOUT (Uçanları erken yakalama)
# ================================
R0_ENABLED = int(os.getenv("R0_ENABLED", "1")) == 1

# Gün içi erken hareket eşiği (henüz uçmamışken)
R0_MIN_CHG = float(os.getenv("R0_MIN_CHG", "0.80"))     # %0.8+
R0_MAX_CHG = float(os.getenv("R0_MAX_CHG", "3.50"))     # %3.5 altı (uçmuş sayılmasın)

# Gap filtresi (çok gapliyse alma)
R0_MAX_GAP = float(os.getenv("R0_MAX_GAP", "1.50"))     # %1.5 üstü istemiyoruz

# Hacim patlaması
R0_MIN_VOL_RATIO = float(os.getenv("R0_MIN_VOL_RATIO", "1.25"))  # 1.25x hacim

# Volatilite sıkışması → patlama
R0_VOL_STD_MAX = float(os.getenv("R0_VOL_STD_MAX", "1.10"))      # düşük vol = sıkışma

# R0 hangi rejimlerde aktif olsun
R0_ALLOW_REGIMES = [r.strip().upper() for r in os.getenv(
    "R0_ALLOW_REGIMES", "R1,R2"
).split(",") if r.strip()]

# =========================================================
# In-memory stores
# =========================================================
LAST_ALARM_TS: Dict[str, float] = {}
WHALE_SENT_DAY: Dict[str, int] = {}
LAST_REGIME: Optional[Dict[str, Any]] = None
TOMORROW_CHAINS: Dict[str, Any] = {}

# =========================================================
# Helpers
# =========================================================
def write_trade_log(record: dict) -> None:
    """
    Appends one JSON object per line into TRADE_LOG_FILE (jsonl).
    """
    try:
        record["logged_at"] = datetime.now(tz=TZ).isoformat()
        os.makedirs(TRADE_LOG_DIR, exist_ok=True)
        with open(TRADE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.exception("Trade log write error: %s", e)

def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")
def build_tomorrow_altin_perf_section(all_rows, TOMORROW_CHAINS) -> str:
    """
    Tomorrow zincirindeki ALTIN listesini alır ve ref_close -> now_close % farkını basar.
    /tomorrow ve alarm mesajlarında kullanılabilir.
    """
    try:
        all_map = {
            (r.get("ticker") or "").strip(): r
            for r in (all_rows or [])
            if (r.get("ticker") or "").strip()
        }

        if not TOMORROW_CHAINS:
            return ""

        latest_key = max(
            TOMORROW_CHAINS.keys(),
            key=lambda k: (TOMORROW_CHAINS.get(k, {}) or {}).get("ts", 0),
        )
        chain = TOMORROW_CHAINS.get(latest_key, {}) or {}

        # ALTIN tickers
        altin_tickers = []
        t_rows = chain.get("rows", []) or []
        for rr in t_rows:
            t = (rr.get("ticker") or "").strip()
            if not t:
                continue
            kind = (rr.get("kind") or rr.get("list") or rr.get("bucket") or "").strip().upper()
            if "ALTIN" in kind:
                altin_tickers.append(t)

        ref_close_map = chain.get("ref_close", {}) or {}
        if not altin_tickers:
            altin_tickers = list(ref_close_map.keys())[:6]

        perf_lines = []
        for t in altin_tickers[:6]:
            ref_close = safe_float(ref_close_map.get(t))
            now_row = all_map.get(t) or {}
            now_close = safe_float(now_row.get("close"))

            dd = pct_change(now_close, ref_close)
            dd_s = f"{dd:+.2f}%" if dd == dd else "n/a"

            perf_lines.append((t, dd_s))

        if not perf_lines:
            return ""

        header = "\n\n<b>ALTIN • Canlı Performans</b>\n"
        body = "\n".join([f"• {t}: {dd_s}" for (t, dd_s) in perf_lines])
        return header + body

    except Exception as e:
        logger.exception("Tomorrow ALTIN perf section error: %s", e)
        return ""

def format_volume(v: Any) -> str:
    try:
        n = float(v)
    except Exception:
        return "n/a"
    absn = abs(n)
    if absn >= 1_000_000_000:
        s = f"{n/1_000_000_000:.1f}B"
        return s.replace(".0B", "B")
    if absn >= 1_000_000:
        return f"{n/1_000_000:.0f}M"
    if absn >= 1_000:
        return f"{n/1_000:.0f}K"
    return f"{n:.0f}"

def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default)
    if raw is None:
        return []
    raw = raw.strip()
    if not raw:
        return []
    return [p.strip().upper() for p in raw.split(",") if p.strip()]

def env_csv_fallback(primary: str, fallback: str, default: str = "") -> List[str]:
    lst = env_csv(primary, default)
    if lst:
        return lst
    return env_csv(fallback, default)

def normalize_is_ticker(t: str) -> str:
    t = t.strip().upper()
    if not t:
        return t
    if t.startswith("BIST:"):
        base = t.replace("BIST:", "")
    else:
        base = t
    if base.endswith(".IS"):
        base = base[:-3]
    return f"BIST:{base}"

# =========================================================
# Tomorrow (Altın Liste) - Message section
# =========================================================

def _pct_price(ref_close: float, pct: float) -> float:
    try:
        if ref_close != ref_close:
            return float("nan")
        return float(ref_close) * (1.0 + pct / 100.0)
    except Exception:
        return float("nan")

def make_chain_id(base_key: str) -> str:
    dt = now_tr()
    hhmm = f"{dt.hour:02d}{dt.minute:02d}"
    return f"{base_key}_{hhmm}"

def format_tomorrow_section(
    tomorrow_rows,
    chain_id: str,
    ref_day_key: str,
    follow_day: int = 0,
    max_items: int = 12,
) -> str:

    if not tomorrow_rows:
        return ""

    if follow_day == 1:
        day_tag = "GÜN 1 TAKİP"
    elif follow_day == 2:
        day_tag = "GÜN 2 TAKİP (KAPANIŞ)"
    else:
        day_tag = "TOMORROW (İLK ÜRETİM)"

    lines = []
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"✅ <b>ALTIN LİSTE – TOMORROW</b>  <code>#{chain_id}</code>")
    lines.append(f"📌 <b>Ref</b>: <code>{ref_day_key} 17:50</code> • <b>{day_tag}</b>")
    lines.append("")

    for i, r in enumerate(tomorrow_rows[:max_items], start=1):
        t = (r.get("ticker") or "n/a").upper()
        ref_close = safe_float(r.get("ref_close", r.get("close")))
        cur_close = safe_float(r.get("close"))

        p1 = _pct_price(ref_close, 1.0)
        p2 = _pct_price(ref_close, 2.0)

        ref_s = "n/a" if ref_close != ref_close else f"{ref_close:.2f}"
        cur_s = "n/a" if cur_close != cur_close else f"{cur_close:.2f}"
        p1_s = "n/a" if p1 != p1 else f"{p1:.2f}"
        p2_s = "n/a" if p2 != p2 else f"{p2:.2f}"

        ch = safe_float(r.get("change", float("nan")))
        ch_s = "" if ch != ch else f" • %{ch:+.2f}"

        lines.append(
            f"{i}) <b>{t}</b>  Ref:<b>{ref_s}</b>  "
            f"Hedef:<b>{p1_s}</b>/<b>{p2_s}</b>  "
            f"Şimdi:<b>{cur_s}</b>{ch_s}"
        )

    lines.append("")
    lines.append("🧩 <b>2 Gün Kuralı:</b> Gün1 ve Gün2 takip edilir; Gün2 sonunda chain kapanır.")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)

def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def now_tr() -> datetime:
    return datetime.now(tz=TZ)

def next_aligned_run(minutes: int) -> datetime:
    n = now_tr()
    m = n.minute
    step = max(1, int(minutes))
    next_m = ((m // step) + 1) * step
    if next_m >= 60:
        nn = (n.replace(second=0, microsecond=0, minute=0) + timedelta(hours=1))
        return nn
    return n.replace(second=0, microsecond=0, minute=next_m)

def within_alarm_window(dt: datetime) -> bool:
    start = dtime(ALARM_START_HOUR, ALARM_START_MIN)
    end = dtime(ALARM_END_HOUR, ALARM_END_MIN)
    t = dt.timetz().replace(tzinfo=None)
    return start <= t <= end

def within_whale_window(dt: datetime) -> bool:
    start = dtime(WHALE_START_HOUR, WHHALE_START_MIN) if False else dtime(WHALE_START_HOUR, WHALE_START_MIN)
    end = dtime(WHALE_END_HOUR, WHALE_END_MIN)
    t = dt.timetz().replace(tzinfo=None)
    return start <= t <= end

def st_short(sig_text: str) -> str:
    if sig_text == "TOPLAMA":
        return "TOP"
    if sig_text == "DİP TOPLAMA":
        return "DIP"
    if sig_text == "AYRIŞMA":
        return "AYR"
    if sig_text == "KÂR KORUMA":
        return "KAR"
    if sig_text == "REJIM BLOK":
        return "BLK"
    return ""

# =========================
# R0 – EARLY BREAKOUT (Uçanları erken yakalama)
# =========================
def detect_r0_early_breakout(
    rows: List[Dict[str, Any]],
    reg: Dict[str, Any],
    xu_change: float
) -> None:
    """
    R0: Gün içi erken momentum + hacim + düşük gap + sıkışma
    Ana sinyali bozmaz, sadece ETİKET basar.
    """
    if not R0_ENABLED:
        return

    regime_tag = reg.get("regime")
    if regime_tag not in R0_ALLOW_REGIMES:
        return

    for r in rows:
        try:
            chg = safe_float(r.get("change"))
            gap = safe_float(r.get("gap_pct", 0.0))
            vol_ratio = safe_float(r.get("vol_ratio", 1.0))
            vol_std = safe_float(r.get("vol_std", 0.0))

            if chg != chg:
                continue

            if not (R0_MIN_CHG <= chg <= R0_MAX_CHG):
                continue

            if abs(gap) > R0_MAX_GAP:
                continue

            if vol_ratio < R0_MIN_VOL_RATIO:
                continue

            if vol_std > R0_VOL_STD_MAX:
                continue

            if xu_change <= -1.2:
                continue

            r["signal"] = "🚀"
            r["signal_text"] = "UÇAN (R0)"
        except Exception:
            continue

# =========================================================
# Trading-day key
# =========================================================
def prev_business_day(d: date) -> date:
    dd = d
    while True:
        dd = dd - timedelta(days=1)
        if dd.weekday() < 5:
            return dd

def trading_day_for_snapshot(dt: datetime) -> date:
    if dt.weekday() == 5:  # Sat
        return (dt.date() - timedelta(days=1))
    if dt.weekday() == 6:  # Sun
        return (dt.date() - timedelta(days=2))
    if dt.timetz().replace(tzinfo=None) < dtime(ALARM_START_HOUR, ALARM_START_MIN):
        return prev_business_day(dt.date())
    return dt.date()

def today_key_tradingday() -> str:
    return trading_day_for_snapshot(now_tr()).strftime("%Y-%m-%d")

def yesterday_key_tradingday() -> str:
    td = trading_day_for_snapshot(now_tr())
    y = prev_business_day(td)
    return y.strftime("%Y-%m-%d")

# =========================================================
# Disk storage
# =========================================================
def _ensure_data_dir() -> str:
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        test_path = os.path.join(DATA_DIR, ".write_test")
        with open(test_path, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(test_path)
        return DATA_DIR
    except Exception:
        fallback = "/tmp/taipo_data"
        os.makedirs(fallback, exist_ok=True)
        return fallback

EFFECTIVE_DATA_DIR = _ensure_data_dir()
PRICE_HISTORY_FILE = os.path.join(EFFECTIVE_DATA_DIR, "price_history.json")
VOLUME_HISTORY_FILE = os.path.join(EFFECTIVE_DATA_DIR, "volume_history.json")
INDEX_HISTORY_FILE = os.path.join(EFFECTIVE_DATA_DIR, "index_history.json")
LAST_ALARM_FILE = os.path.join(EFFECTIVE_DATA_DIR, "last_alarm_ts.json")
TOMORROW_SNAPSHOT_FILE = os.path.join(EFFECTIVE_DATA_DIR, "tomorrow_snapshot.json")
WHALE_SENT_FILE = os.path.join(EFFECTIVE_DATA_DIR, "whale_sent_day.json")
TOMORROW_CHAIN_FILE = os.path.join(EFFECTIVE_DATA_DIR, "tomorrow_chains.json")

def _load_json(path: str) -> Dict[str, Any]:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception as e:
        logger.warning("History load failed (%s): %s", path, e)
        return {}

def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning("History write failed (%s): %s", path, e)

def _prune_days(d: Dict[str, Any], keep_days: int) -> Dict[str, Any]:
    if not isinstance(d, dict):
        return {}
    keys = sorted(d.keys())
    if len(keys) <= keep_days:
        return d
    cut = keys[:-keep_days]
    for k in cut:
        d.pop(k, None)
    return d

def load_last_alarm_ts() -> None:
    global LAST_ALARM_TS
    raw = _load_json(LAST_ALARM_FILE)
    out: Dict[str, float] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            kk = (k or "").strip().upper()
            vv = safe_float(v)
            if kk and vv == vv:
                out[kk] = float(vv)
    LAST_ALARM_TS = out
    logger.info("Loaded LAST_ALARM_TS: %d tickers", len(LAST_ALARM_TS))

def save_last_alarm_ts() -> None:
    try:
        data = {k: float(v) for k, v in (LAST_ALARM_TS or {}).items()}
        _atomic_write_json(LAST_ALARM_FILE, data)
    except Exception as e:
        logger.warning("save_last_alarm_ts failed: %s", e)

def load_whale_sent_day() -> None:
    global WHALE_SENT_DAY
    raw = _load_json(WHALE_SENT_FILE)
    out: Dict[str, int] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            kk = (k or "").strip()
            if not kk:
                continue
            try:
                out[kk] = int(v)
            except Exception:
                continue
    WHALE_SENT_DAY = out
    logger.info("Loaded WHALE_SENT_DAY: %d days", len(WHALE_SENT_DAY))

def save_whale_sent_day() -> None:
    try:
        _atomic_write_json(WHALE_SENT_FILE, WHALE_SENT_DAY or {})
    except Exception as e:
        logger.warning("save_whale_sent_day failed: %s", e)

def load_tomorrow_chains() -> None:
    global TOMORROW_CHAINS
    try:
        data = _load_json(TOMORROW_CHAIN_FILE)
        TOMORROW_CHAINS = data if isinstance(data, dict) else {}
        logger.info("Loaded TOMORROW_CHAINS: %d chain", len(TOMORROW_CHAINS))
    except Exception as e:
        logger.warning("load_tomorrow_chains failed: %s", e)
        TOMORROW_CHAINS = {}


def save_tomorrow_chains() -> None:
    global TOMORROW_CHAINS
    try:
        _atomic_write_json(TOMORROW_CHAIN_FILE, TOMORROW_CHAINS or {})
    except Exception as e:
        logger.warning("save_tomorrow_chains failed: %s", e)

def within_tomorrow_follow_window(dt: datetime) -> bool:
    h, m = dt.hour, dt.minute
    start = (TOMORROW_FOLLOW_START_HOUR, TOMORROW_FOLLOW_START_MIN)
    end = (TOMORROW_FOLLOW_END_HOUR, TOMORROW_FOLLOW_END_MIN)
    cur = (h, m)
    return start <= cur <= end
    
def update_history_from_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    day = today_key_tradingday()
    price_hist = _load_json(PRICE_HISTORY_FILE)
    vol_hist = _load_json(VOLUME_HISTORY_FILE)
    if not isinstance(price_hist, dict):
        price_hist = {}
    if not isinstance(vol_hist, dict):
        vol_hist = {}
    price_hist.setdefault(day, {})
    vol_hist.setdefault(day, {})
    for r in rows:
        t = (r.get("ticker") or "").strip().upper()
        cl = r.get("close", float("nan"))
        vol = r.get("volume", float("nan"))
        if not t:
            continue
        if cl != cl or vol != vol:
            continue
        price_hist[day][t] = float(cl)
        vol_hist[day][t] = float(vol)
    _prune_days(price_hist, HISTORY_DAYS)
    _prune_days(vol_hist, HISTORY_DAYS)
    _atomic_write_json(PRICE_HISTORY_FILE, price_hist)
    _atomic_write_json(VOLUME_HISTORY_FILE, vol_hist)

# =========================================================
# Index (XU100) history + regime
# =========================================================
def update_index_history(day_key: str, close: float, change: float, volume: float, open_: float) -> None:
    try:
        d = _load_json(INDEX_HISTORY_FILE)
        if not isinstance(d, dict):
            d = {}
        d.setdefault(day_key, {})
        d[day_key] = {
            "close": float(close) if close == close else None,
            "change": float(change) if change == change else None,
            "volume": float(volume) if volume == volume else None,
            "open": float(open_) if open_ == open_ else None,
            "saved_at": now_tr().isoformat(),
        }
        _prune_days(d, max(HISTORY_DAYS, REJIM_LOOKBACK + 10))
        _atomic_write_json(INDEX_HISTORY_FILE, d)
    except Exception as e:
        logger.warning("update_index_history failed: %s", e)

def _sma(vals: List[float], n: int) -> float:
    if n <= 0 or len(vals) < n:
        return float("nan")
    s = sum(vals[-n:])
    return s / float(n)

def _std(vals: List[float]) -> float:
    if len(vals) < 5:
        return float("nan")
    m = sum(vals) / len(vals)
    v = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
    return math.sqrt(v)

def compute_regime(xu_close: float, xu_change: float, xu_vol: float, xu_open: float) -> Dict[str, Any]:
    """
    Rejim: Endeks yön + volatilite + gap + (opsiyonel) kötü önceki gün
    + uçanları yakalamak için MOMO_UP etiketi (blok değil)
    """
    reg: Dict[str, Any] = {
        "enabled": REJIM_ENABLED,
        "name": "UNKNOWN",
        "block": False,
        "reason": "",
        "volatility": float("nan"),
        "gap_pct": float("nan"),
        "trend": "n/a",
        "vol_ok": True,
        "gap_ok": True,
        "allow_trade": True,
        "regime": "R2",
        "momo": False,
    }
    if not REJIM_ENABLED:
        reg["name"] = "OFF"
        reg["allow_trade"] = True
        reg["regime"] = "R1"
        return reg

    idx_hist = _load_json(INDEX_HISTORY_FILE)
    keys = sorted(idx_hist.keys()) if isinstance(idx_hist, dict) else []

    closes: List[float] = []
    changes: List[float] = []
    prev_close = float("nan")
    prev_change = float("nan")

    for k in keys[-max(10, REJIM_LOOKBACK + 3):]:
        item = idx_hist.get(k, {}) if isinstance(idx_hist, dict) else {}
        c = safe_float(item.get("close"))
        ch = safe_float(item.get("change"))
        if c == c:
            closes.append(float(c))
        if ch == ch:
            changes.append(float(ch))

    if len(closes) >= 2:
        prev_close = closes[-2]
    if len(changes) >= 2:
        prev_change = changes[-2]

    if (xu_open == xu_open) and (prev_close == prev_close) and prev_close > 0:
        reg["gap_pct"] = (xu_open / prev_close - 1.0) * 100.0

    look_changes = changes[-REJIM_LOOKBACK:] if len(changes) >= 5 else []
    reg["volatility"] = _std(look_changes) if look_changes else float("nan")

    fast = _sma(closes, REJIM_TREND_SMA_FAST)
    slow = _sma(closes, REJIM_TREND_SMA_SLOW)
    trend = "n/a"
    if fast == fast and slow == slow and xu_close == xu_close:
        if fast > slow and xu_close > slow:
            trend = "UP"
        elif fast < slow and xu_close < slow:
            trend = "DOWN"
        else:
            trend = "SIDE"
    reg["trend"] = trend

    vol_hi = (reg["volatility"] == reg["volatility"]) and (reg["volatility"] >= REJIM_VOL_HIGH)
    gap_down_risk = (reg["gap_pct"] == reg["gap_pct"]) and (reg["gap_pct"] <= -abs(REJIM_GAP_PCT))
    prev_day_bad = (prev_change == prev_change) and (prev_change <= REJIM_PREV_DAY_BAD)

    reg["vol_ok"] = (not vol_hi)
    reg["gap_ok"] = (not gap_down_risk)

    gap_up = (reg["gap_pct"] == reg["gap_pct"]) and (reg["gap_pct"] >= abs(REJIM_MOMO_UP_GAP))
    momo = (xu_change == xu_change) and (xu_change >= REJIM_MOMO_UP_CHG) and gap_up
    reg["momo"] = bool(momo)

    if gap_down_risk and prev_day_bad:
        reg["name"] = "RISK_OFF"
        reg["reason"] = f"GAP_DOWN({reg['gap_pct']:+.2f}%) + PREV_BAD({prev_change:+.2f}%)"
    elif trend == "DOWN" and (xu_change == xu_change and xu_change < 0) and (vol_hi or (xu_change <= -1.50)):
        reg["name"] = "RISK_OFF"
        reg["reason"] = f"DOWN + VOL({reg['volatility']:.2f})"
    elif momo:
        reg["name"] = "MOMO_UP"
        reg["reason"] = f"UP + GAP_UP({reg['gap_pct']:+.2f}%) + CHG({xu_change:+.2f}%)"
    elif trend == "UP" and (xu_change == xu_change and xu_change > 0.0) and not vol_hi:
        reg["name"] = "TREND_UP"
        reg["reason"] = "UP + normal vol"
    elif vol_hi or (xu_change == xu_change and abs(xu_change) >= 2.20):
        reg["name"] = "VOLATILE"
        reg["reason"] = "High vol"
    else:
        reg["name"] = "CHOP"
        reg["reason"] = "Side / mixed"

    reg["block"] = (reg["name"].upper() in (REJIM_BLOCK_ON or []))
    reg["allow_trade"] = (not reg["block"])

    if reg["block"]:
        reg["regime"] = "R3"
    elif reg["allow_trade"] and reg["vol_ok"] and reg["gap_ok"]:
        reg["regime"] = "R1"
    else:
        reg["regime"] = "R2"

    return reg

def format_regime_line(reg: Dict[str, Any]) -> str:
    if not reg or not reg.get("enabled", False):
        return "🧭 <b>Rejim</b>: <b>OFF</b>"
    nm = reg.get("name", "n/a")
    tr = reg.get("trend", "n/a")
    vol = reg.get("volatility", float("nan"))
    gap = reg.get("gap_pct", float("nan"))
    vol_s = "n/a" if vol != vol else f"{vol:.2f}"
    gap_s = "n/a" if gap != gap else f"{gap:+.2f}%"
    blk = "⛔️ BLOK" if reg.get("block") else "✅ OK"
    rsn = reg.get("reason", "")
    rsn_s = f" • <i>{rsn}</i>" if rsn else ""
    momo = " 🚀" if reg.get("name") == "MOMO_UP" else ""
    return f"🧭 <b>Rejim</b>: <b>{nm}</b>{momo} (trend={tr}, vol={vol_s}, gap={gap_s}) • <b>{blk}</b>{rsn_s}"

def apply_regime_gate_to_rows(rows: List[Dict[str, Any]], reg: Dict[str, Any]) -> None:
    if not REJIM_ENABLED or not reg or not reg.get("block"):
        return
    for r in rows:
        r["signal"] = "⛔"
        r["signal_text"] = "REJIM BLOK"

# =========================================================
# Stats (ticker) over HISTORY_DAYS
# =========================================================
def compute_30d_stats(ticker: str) -> Optional[Dict[str, Any]]:
    t = (ticker or "").strip().upper()
    if not t:
        return None
    price_hist = _load_json(PRICE_HISTORY_FILE)
    vol_hist = _load_json(VOLUME_HISTORY_FILE)
    if not isinstance(price_hist, dict) or not isinstance(vol_hist, dict):
        return None
    days = sorted(set(list(price_hist.keys()) + list(vol_hist.keys())))
    if not days:
        return None
    days = days[-HISTORY_DAYS:]
    closes: List[float] = []
    vols: List[float] = []
    today = today_key_tradingday()
    today_close = None
    today_vol = None
    for d in days:
        pd = price_hist.get(d, {})
        vd = vol_hist.get(d, {})
        if isinstance(pd, dict) and t in pd:
            c = safe_float(pd.get(t))
            if c == c:
                closes.append(c)
                if d == today:
                    today_close = c
        if isinstance(vd, dict) and t in vd:
            v = safe_float(vd.get(t))
            if v == v:
                vols.append(v)
                if d == today:
                    today_vol = v
    if len(closes) < 5 or len(vols) < 5:
        return None
    mn = float(min(closes))
    mx = float(max(closes))
    avg_close = float(sum(closes) / len(closes))
    avg_vol = float(sum(vols) / len(vols))
    if today_close is None:
        today_close = closes[-1]
    if today_vol is None:
        today_vol = vols[-1]
    ratio = (today_vol / avg_vol) if avg_vol > 0 else float("nan")
    if mx > mn:
        band_pct = (today_close - mn) / (mx - mn) * 100.0
        band_pct = max(0.0, min(100.0, band_pct))
    else:
        band_pct = 50.0
    return {
        "min": mn,
        "max": mx,
        "avg_close": avg_close,
        "avg_vol": avg_vol,
        "today_close": float(today_close),
        "today_vol": float(today_vol),
        "ratio": float(ratio),
        "band_pct": float(band_pct),
        "days_used": len(days),
        "samples_close": len(closes),
        "samples_vol": len(vols),
    }

def soft_plan_line(stats: Dict[str, Any], current_close: float) -> str:
    if not stats:
        return "Plan: Veri yetersiz (arşiv dolsun)."
    band = stats.get("band_pct", 50.0)
    ratio = stats.get("ratio", float("nan"))
    if band <= 25:
        band_tag = "ALT BANT (dip bölgesi)"
        base_plan = "Sakin açılışta takip; +%2–%4 kademeli kâr mantıklı."
    elif band <= 60:
        band_tag = "ORTA BANT"
        base_plan = "Trend teyidi bekle; hacim sürerse +%2–%4 hedeflenebilir."
    else:
        band_tag = "ÜST BANT (kâr bölgesi)"
        base_plan = "Kâr koruma modu; sert dönüşte temkin."
    if ratio == ratio:
        if ratio >= 2.0:
            vol_tag = f"Hacim {ratio:.2f}x (anormal güçlü)"
        elif ratio >= 1.2:
            vol_tag = f"Hacim {ratio:.2f}x (güçlü)"
        else:
            vol_tag = f"Hacim {ratio:.2f}x (normal)"
    else:
        vol_tag = "Hacim n/a"
    return f"{band_tag} | {vol_tag} | {base_plan}"

def format_30d_note(ticker: str, current_close: float) -> str:
    st = compute_30d_stats(ticker)
    if not st:
        return f"• <b>{ticker}</b>: Arşiv veri yok (disk yeni) ⏳"
    mn = st["min"]; mx = st["max"]; avc = st["avg_close"]; avv = st["avg_vol"]
    tv = st["today_vol"]; ratio = st["ratio"]; band = st["band_pct"]
    ratio_s = "n/a" if (ratio != ratio) else f"{ratio:.2f}x"
    plan = soft_plan_line(st, current_close)
    return (
        f"• <b>{ticker}</b>: Arşiv Close min/avg/max <b>{mn:.2f}</b>/<b>{avc:.2f}</b>/<b>{mx:.2f}</b> • "
        f"Ort.Hcm <b>{format_volume(avv)}</b> • Bugün <b>{format_volume(tv)}</b> • "
        f"<b>{ratio_s}</b> • Band <b>%{band:.0f}</b>\n"
        f"  ↳ <i>{plan}</i>"
    )

# =========================================================
# TradingView Scanner
# =========================================================
def tv_scan_symbols_sync(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    payload = {"symbols": {"tickers": symbols}, "columns": ["close", "change", "volume", "open"]}
    for attempt in range(3):
        try:
            r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
            if r.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            out: Dict[str, Dict[str, Any]] = {}
            for it in data.get("data", []):
                sym = it.get("symbol") or it.get("s")
                d = it.get("d", [])
                if not sym or not isinstance(d, list) or len(d) < 4:
                    continue
                short = sym.split(":")[-1].strip().upper()
                out[short] = {
                    "close": safe_float(d[0]),
                    "change": safe_float(d[1]),
                    "volume": safe_float(d[2]),
                    "open": safe_float(d[3]),
                }
            return out
        except Exception as e:
            logger.exception("TradingView scan error: %s", e)
            time.sleep(1.0 * (attempt + 1))
    return {}

async def tv_scan_symbols(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    return await asyncio.to_thread(tv_scan_symbols_sync, symbols)

async def get_xu100_summary() -> Tuple[float, float, float, float]:
    m = await tv_scan_symbols(["BIST:XU100"])
    d = m.get("XU100", {})
    return (
        d.get("close", float("nan")),
        d.get("change", float("nan")),
        d.get("volume", float("nan")),
        d.get("open", float("nan")),
    )

# ✅ DÜZELTİLDİ: xu100_change parametresi eklendi (NameError biter)
async def build_rows_from_is_list(is_list: List[str], xu100_change: float = float("nan")) -> List[Dict[str, Any]]:
    tv_symbols = [normalize_is_ticker(t) for t in is_list if t.strip()]
    tv_map = await tv_scan_symbols(tv_symbols)

    rows: List[Dict[str, Any]] = []
    for original in is_list:
        short = normalize_is_ticker(original).split(":")[-1]
        d = tv_map.get(short, {})
        if not d:
            rows.append(
                {
                    "ticker": short,
                    "close": float("nan"),
                    "change": float("nan"),
                    "volume": float("nan"),
                    "signal": "-",
                    "signal_text": "",
                }
            )
        else:
            rows.append(
                {
                    "ticker": short,
                    "close": d.get("close", float("nan")),
                    "change": d.get("change", float("nan")),
                    "volume": d.get("volume", float("nan")),
                    "signal": "-",
                    "signal_text": "",
                }
            )

    # R0 etiketi fail-safe
    try:
        detect_r0_early_breakout(rows, LAST_REGIME or {}, xu100_change)
    except Exception:
        pass

    return rows

# =========================================================
# Signal logic (TopN threshold)
# =========================================================
def compute_volume_threshold(rows: List[Dict[str, Any]], top_n: int) -> float:
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    if not rows_with_vol:
        return float("inf")
    n = max(1, int(top_n))
    ranked = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)
    top = ranked[:n]
    return float(top[-1]["volume"]) if top else float("inf")

def compute_signal_rows(rows: List[Dict[str, Any]], xu100_change: float, top_n: int) -> float:
    threshold = compute_volume_threshold(rows, top_n)
    _apply_signals_with_threshold(rows, xu100_change, threshold)
    return threshold

def _apply_signals_with_threshold(rows: List[Dict[str, Any]], xu100_change: float, min_vol_threshold: float) -> None:
    for r in rows:
        # R0 yakalandıysa üstüne yazma (opsiyonel ama güzel)
        if r.get("signal_text") == "UÇAN (R0)":
            continue

        ch = r.get("change", float("nan"))
        vol = r.get("volume", float("nan"))
        if ch != ch:
            r["signal"] = "-"
            r["signal_text"] = ""
            continue
        if ch >= 4.0:
            r["signal"] = "⚠️"
            r["signal_text"] = "KÂR KORUMA"
            continue

        in_topN = (vol == vol) and (vol >= min_vol_threshold)

        if in_topN and (xu100_change == xu100_change) and (xu100_change <= -0.80) and (ch >= 0.40):
            r["signal"] = "🧠"
            r["signal_text"] = "AYRIŞMA"
            continue

        if in_topN and (0.00 <= ch <= 0.60):
            r["signal"] = "🧠"
            r["signal_text"] = "TOPLAMA"
            continue
        if in_topN and (-0.60 <= ch < 0.00):
            r["signal"] = "🧲"
            r["signal_text"] = "DİP TOPLAMA"
            continue

        r["signal"] = "-"
        r["signal_text"] = ""

# =========================================================
# Table view
# =========================================================
def make_table(rows: List[Dict[str, Any]], title: str, include_kind: bool = False) -> str:
    if include_kind:
        header = f"{'HIS':<5} {'S':<1} {'K':<3} {'%':>6} {'FYT':>8} {'HCM':>6}"
    else:
        header = f"{'HIS':<5} {'S':<1} {'%':>6} {'FYT':>8} {'HCM':>6}"
    sep = "-" * len(header)
    lines = [title, "<pre>", header, sep]
    for r in rows:
        t = (r.get("ticker", "n/a") or "n/a")[:5]
        sig = (r.get("signal", "-") or "-")[:1]
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", float("nan"))
        ch_s = "n/a" if (ch != ch) else f"{ch:+.2f}"
        cl_s = "n/a" if (cl != cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)[:6]
        if include_kind:
            k = st_short(r.get("signal_text", ""))
            lines.append(f"{t:<5} {sig:<1} {k:<3} {ch_s:>6} {cl_s:>8} {vol_s:>6}")
        else:
            lines.append(f"{t:<5} {sig:<1} {ch_s:>6} {cl_s:>8} {vol_s:>6}")
    lines.append("</pre>")
    return "\n".join(lines)

def parse_watch_args(args: List[str]) -> List[str]:
    if not args:
        return []
    joined = " ".join(args).strip().replace(";", ",")
    parts: List[str] = []
    for p in joined.split(","):
        p = p.strip()
        if not p:
            continue
        parts.extend(p.split())
    out: List[str] = []
    for t in parts:
        tt = re.sub(r"[^A-Za-z0-9:_\.]", "", t).upper()
        if tt:
            out.append(tt)
    seen = set()
    uniq: List[str] = []
    for t in out:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq

# =========================================================
# ✅ Yahoo Bootstrap
# =========================================================
def _to_yahoo_symbol_bist(ticker: str) -> str:
    t = (ticker or "").strip().upper().replace("BIST:", "")
    if not t:
        return ""
    if t.endswith(".IS"):
        return t
    return f"{t}.IS"

def yahoo_fetch_history_sync(symbol: str, days: int) -> List[Tuple[str, float, float]]:
    sym = (symbol or "").strip()
    if not sym:
        return []
    if days > 365:
        rng = "2y"
    elif days > 180:
        rng = "1y"
    elif days > 90:
        rng = "6mo"
    elif days > 45:
        rng = "3mo"
    else:
        rng = "2mo"

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
    params = {"range": rng, "interval": "1d"}
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=YAHOO_TIMEOUT, headers=headers)
            r.raise_for_status()
            j = r.json() or {}
            chart = (j.get("chart") or {})
            res = (chart.get("result") or [])
            if not res:
                return []
            res0 = res[0]
            ts_list = res0.get("timestamp") or []
            ind = (res0.get("indicators") or {}).get("quote") or []
            if not ind:
                return []
            q0 = ind[0]
            closes = q0.get("close") or []
            vols = q0.get("volume") or []
            out: List[Tuple[str, float, float]] = []
            for i, ts in enumerate(ts_list):
                if i >= len(closes) or i >= len(vols):
                    continue
                c = closes[i]; v = vols[i]
                if c is None or v is None:
                    continue
                dt = datetime.fromtimestamp(int(ts), tz=TZ).date()
                day_s = dt.strftime("%Y-%m-%d")
                out.append((day_s, float(c), float(v)))
            if days > 0 and len(out) > days:
                out = out[-days:]
            return out
        except Exception as e:
            logger.warning("Yahoo fetch error (%s) attempt=%d: %s", sym, attempt + 1, e)
            time.sleep(0.6 * (attempt + 1))
    return []

def yahoo_bootstrap_fill_history(tickers: List[str], days: int) -> Tuple[int, int]:
    if not tickers:
        return (0, 0)
    price_hist = _load_json(PRICE_HISTORY_FILE)
    vol_hist = _load_json(VOLUME_HISTORY_FILE)
    if not isinstance(price_hist, dict):
        price_hist = {}
    if not isinstance(vol_hist, dict):
        vol_hist = {}
    total_points = 0
    filled = 0
    for t in tickers:
        short = (t or "").strip().upper().replace("BIST:", "")
        if not short:
            continue
        sym = _to_yahoo_symbol_bist(short)
        data = yahoo_fetch_history_sync(sym, days)
        if not data:
            time.sleep(YAHOO_SLEEP_SEC)
            continue
        for day_s, close, vol in data:
            price_hist.setdefault(day_s, {})
            vol_hist.setdefault(day_s, {})
            price_hist[day_s][short] = float(close)
            vol_hist[day_s][short] = float(vol)
            total_points += 1
        filled += 1
        time.sleep(YAHOO_SLEEP_SEC)
    _prune_days(price_hist, max(HISTORY_DAYS, days))
    _prune_days(vol_hist, max(HISTORY_DAYS, days))
    _atomic_write_json(PRICE_HISTORY_FILE, price_hist)
    _atomic_write_json(VOLUME_HISTORY_FILE, vol_hist)
    return (filled, total_points)

async def yahoo_bootstrap_if_needed() -> str:
    try:
        ph = _load_json(PRICE_HISTORY_FILE)
        vh = _load_json(VOLUME_HISTORY_FILE)
        empty = (not ph) or (not vh)

        if not BOOTSTRAP_ON_START and not BOOTSTRAP_FORCE:
            return "BOOTSTRAP kapalı (BOOTSTRAP_ON_START=0)."
        if not empty and not BOOTSTRAP_FORCE:
            return "BOOTSTRAP atlandı (history dolu)."

        bist200 = env_csv("BIST200_TICKERS")
        if not bist200:
            return "BOOTSTRAP: BIST200_TICKERS env boş."
        tickers = [normalize_is_ticker(x).split(":")[-1] for x in bist200 if x.strip()]

        logger.info("BOOTSTRAP başlıyor… Yahoo %d gün (hisse=%d)", BOOTSTRAP_DAYS, len(tickers))
        filled, points = await asyncio.to_thread(yahoo_bootstrap_fill_history, tickers, BOOTSTRAP_DAYS)
        done = f"BOOTSTRAP tamam ✅ filled={filled} • points={points}"
        logger.info(done)
        return done
    except Exception as e:
        logger.exception("Bootstrap error: %s", e)
        return f"BOOTSTRAP hata: {e}"

# =========================================================
# Tomorrow List (Altın + Aday)
# =========================================================
def tomorrow_score(row: Dict[str, Any]) -> float:
    t = row.get("ticker", "")
    vol = row.get("volume", float("nan"))
    kind = row.get("signal_text", "")
    st = compute_30d_stats(t) if t else None
    band = st.get("band_pct", 50.0) if st else 50.0

    kind_bonus = 0.0
    if kind == "DİP TOPLAMA":
        kind_bonus = 15.0
    elif kind == "TOPLAMA":
        kind_bonus = 8.0
    elif kind == "AYRIŞMA":
        kind_bonus = 4.0
    elif kind == "UÇAN (R0)":
        kind_bonus = 2.0  # sadece öne aldırır, liste kriteri değil

    vol_term = 0.0
    if vol == vol and vol > 0:
        vol_term = math.log10(vol + 1.0) * 10.0

    band_term = max(0.0, (70.0 - float(band)))
    return vol_term + band_term + kind_bonus


def _tomorrow_thresholds_for(st: Dict[str, Any]) -> Tuple[float, float, bool]:
    if not TORPIL_ENABLED or not st:
        return (TOMORROW_MIN_VOL_RATIO, TOMORROW_MAX_BAND, False)
    samples = min(int(st.get("samples_close", 0)), int(st.get("samples_vol", 0)))
    if samples < TORPIL_MIN_SAMPLES:
        return (TORPIL_MIN_VOL_RATIO, TORPIL_MAX_BAND, True)
    return (TOMORROW_MIN_VOL_RATIO, TOMORROW_MAX_BAND, False)


def _relax_thresholds(min_ratio: float, max_band: float) -> Tuple[float, float]:
    # ufak gevşeme: hacim eşiğini %10 düşür, bandı +10 artır (sınırlı)
    try:
        mr = float(min_ratio) * 0.90
    except Exception:
        mr = min_ratio
    try:
        mb = float(max_band) + 10.0
    except Exception:
        mb = max_band
    mb = max(0.0, min(95.0, mb))
    return mr, mb


def build_tomorrow_rows(all_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _pass(relaxed: bool) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in all_rows:
            kind = r.get("signal_text", "")

            # ALTIN liste: sinyal + BIST200 (liste zaten BIST200 rows)
            # R0 sadece öne aldırır, kriter değil ama "liste boş" olmasın diye aday havuzunda tutulabilir
            if kind not in ("TOPLAMA", "DİP TOPLAMA", "UÇAN (R0)") and not (TOMORROW_INCLUDE_AYRISMA and kind == "AYRIŞMA"):
                continue

            t = r.get("ticker", "")
            if not t:
                continue

            st = compute_30d_stats(t)
            if not st:
                continue

            ratio = st.get("ratio", float("nan"))
            band = st.get("band_pct", 50.0)

            min_ratio, max_band, _ = _tomorrow_thresholds_for(st)
            if relaxed:
                min_ratio, max_band = _relax_thresholds(min_ratio, max_band)

            if ratio != ratio or ratio < min_ratio:
                continue
            if band > max_band:
                continue

            out.append(r)

        out.sort(key=tomorrow_score, reverse=True)
        return out[:max(1, TOMORROW_MAX)]

    # 1) normal
    out = _pass(relaxed=False)

    # 2) ALTIN hiç çıkmadıysa ufak gevşeme
    if not out:
        out = _pass(relaxed=True)

    return out


def build_candidate_rows(all_rows: List[Dict[str, Any]], gold_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    gold_set = set((r.get("ticker") or "").strip().upper() for r in (gold_rows or []))

    def _pass(relaxed: bool) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in all_rows:
            kind = r.get("signal_text", "")
            if kind not in ("TOPLAMA", "DİP TOPLAMA", "UÇAN (R0)") and not (CANDIDATE_INCLUDE_AYRISMA and kind == "AYRIŞMA"):
                continue

            t = (r.get("ticker") or "").strip().upper()
            if not t or t in gold_set:
                continue

            st = compute_30d_stats(t)
            if not st:
                continue

            ratio = st.get("ratio", float("nan"))
            band = st.get("band_pct", 50.0)

            min_ratio = CANDIDATE_MIN_VOL_RATIO
            max_band = CANDIDATE_MAX_BAND
            if relaxed:
                min_ratio, max_band = _relax_thresholds(min_ratio, max_band)

            if ratio != ratio or ratio < min_ratio:
                continue
            if band > max_band:
                continue

            out.append(r)

        out.sort(key=tomorrow_score, reverse=True)
        return out[:max(1, CANDIDATE_MAX)]

    out = _pass(relaxed=False)
    if not out:
        out = _pass(relaxed=True)
    return out


def format_threshold(min_vol: float) -> str:
    if not isinstance(min_vol, (int, float)) or math.isnan(min_vol) or min_vol == float("inf"):
        return "n/a"
    return format_volume(min_vol)


def build_tomorrow_message(
    gold_rows: List[Dict[str, Any]],
    cand_rows: List[Dict[str, Any]],
    xu_close: float,
    xu_change: float,
    thresh_s: str,
    reg: Dict[str, Any],
) -> str:
    now_s = now_tr().strftime("%H:%M")
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    tomorrow = (now_tr().date() + timedelta(days=1)).strftime("%Y-%m-%d")

    torpil_used_any = False
    for r in (gold_rows or []):
        st = compute_30d_stats(r.get("ticker", ""))
        if st:
            _, _, used = _tomorrow_thresholds_for(st)
            if used:
                torpil_used_any = True
                break

    head = (
        f"🌙 <b>ERTESİ GÜNE TOPLAMA – RAPOR</b> • <b>{tomorrow}</b>\n"
        f"🕒 Hazırlandı: <b>{now_s}</b> • <b>{BOT_VERSION}</b>\n"
        f"📊 <b>XU100</b>: {xu_close_s} • {xu_change_s}\n"
        f"{format_regime_line(reg)}\n"
        f"🧱 <b>Top{VOLUME_TOP_N} Eşik</b>: ≥ <b>{thresh_s}</b>\n"
        f"🥇 <b>ALTIN</b>: Band ≤ <b>%{TOMORROW_MAX_BAND:.0f}</b> • Hacim ≥ <b>{TOMORROW_MIN_VOL_RATIO:.2f}x</b> • Max <b>{TOMORROW_MAX}</b>\n"
        f"🥈 <b>ADAY</b>: Band ≤ <b>%{CANDIDATE_MAX_BAND:.0f}</b> • Hacim ≥ <b>{CANDIDATE_MIN_VOL_RATIO:.2f}x</b> • Max <b>{CANDIDATE_MAX}</b>\n"
    )
    if torpil_used_any:
        head += "🧩 <i>Torpil Modu: veri az olan hisselerde geçici yumuşatma aktif.</i>\n"

    gold_table = make_table(gold_rows, "✅ <b>ALTIN LİSTE (Kesin)</b>", include_kind=True) if gold_rows else "❌ <b>ALTIN LİSTE çıkmadı.</b>"
    cand_table = make_table(cand_rows, "🟦 <b>ADAY LİSTE (Radar)</b>", include_kind=True) if cand_rows else "— <b>ADAY LİSTE yok.</b>"

    notes_lines = ["\n📌 <b>Arşiv Notlar (ALTIN öncelikli)</b>"]
    if gold_rows:
        for r in gold_rows[:min(len(gold_rows), ALARM_NOTE_MAX)]:
            t = r.get("ticker", "")
            cl = r.get("close", float("nan"))
            notes_lines.append(format_30d_note(t, cl))
    elif cand_rows:
        for r in cand_rows[:min(len(cand_rows), ALARM_NOTE_MAX)]:
            t = r.get("ticker", "")
            cl = r.get("close", float("nan"))
            notes_lines.append(format_30d_note(t, cl))
    else:
        notes_lines.append("<i>Not yok (liste boş).</i>")
    notes = "\n".join(notes_lines)

    foot = (
        "\n\n🟢 <b>Sabah Planı (Pratik)</b>\n"
        "• Açılışta ilk 5–15 dk “sakin + yeşil” teyidi\n"
        "• +%2–%4 kademeli çıkış\n"
        "• Ters mum gelirse: disiplin (zarar büyütme yok)"
    )
    return head + "\n" + gold_table + "\n\n" + cand_table + "\n" + notes + foot


def save_tomorrow_snapshot(rows: List[Dict[str, Any]], xu_change: float) -> None:
    try:
        day_key = today_key_tradingday()
        snap = _load_json(TOMORROW_SNAPSHOT_FILE)
        if not isinstance(snap, dict):
            snap = {}
        items = []
        for r in rows:
            t = (r.get("ticker") or "").strip().upper()
            cl = r.get("close", float("nan"))
            ch = r.get("change", float("nan"))
            vol = r.get("volume", float("nan"))
            if not t or cl != cl:
                continue
            items.append({
                "ticker": t,
                "ref_close": float(cl),
                "change": float(ch) if ch == ch else None,
                "volume": float(vol) if vol == vol else None,
                "kind": (r.get("signal_text") or ""),
                "saved_at": now_tr().isoformat(),
                "xu100_change": float(xu_change) if xu_change == xu_change else None,
            })
        snap[day_key] = items
        _atomic_write_json(TOMORROW_SNAPSHOT_FILE, snap)
    except Exception as e:
        logger.warning("save_tomorrow_snapshot failed: %s", e)


def load_yesterday_tomorrow_snapshot() -> List[Dict[str, Any]]:
    snap = _load_json(TOMORROW_SNAPSHOT_FILE)
    if not isinstance(snap, dict):
        return []
    yk = yesterday_key_tradingday()
    items = snap.get(yk, [])
    return items if isinstance(items, list) else []


# =========================================================
# Alarm message + logic
# =========================================================
def build_alarm_message(
    alarm_rows: List[Dict[str, Any]],
    watch_rows: List[Dict[str, Any]],
    xu_close: float,
    xu_change: float,
    thresh_s: str,
    top_n: int,
    reg: Dict[str, Any],
) -> str:
    now_s = now_tr().strftime("%H:%M")
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    trig = []
    for r in alarm_rows:
        k = st_short(r.get("signal_text", ""))
        t = r.get("ticker", "")
        if t:
            trig.append(f"{t}({k})")
    trig_s = ", ".join(trig) if trig else "—"
    head = (
        f"🚨 <b>ALARM GELDİ</b> • <b>{now_s}</b> • <b>{BOT_VERSION}</b>\n"
        f"📊 <b>XU100</b>: {xu_close_s} • {xu_change_s}\n"
        f"{format_regime_line(reg)}\n"
        f"🧱 <b>Top{top_n} Eşik</b>: ≥ <b>{thresh_s}</b>\n"
        f"🎯 <b>Tetiklenen</b>: {trig_s}\n"
    )
    alarm_table = make_table(alarm_rows, "🔥 <b>ALARM RADAR</b>", include_kind=True)
    notes_lines = ["\n📌 <b>Arşiv Notlar (Disk)</b>"]
    for r in alarm_rows[:max(1, ALARM_NOTE_MAX)]:
        t = r.get("ticker", "")
        cl = r.get("close", float("nan"))
        if t:
            notes_lines.append(format_30d_note(t, cl))
    notes = "\n".join(notes_lines)
    
    tomorrow_section = ""
    try:
        if TOMORROW_CHAINS:
            latest_key = max(
                TOMORROW_CHAINS.keys(),
                key=lambda k: (TOMORROW_CHAINS.get(k, {}) or {}).get("ts", 0),
            )
            blob = TOMORROW_CHAINS.get(latest_key, {}) or {}
            t_rows = blob.get("rows", []) or []
            t_chain_id = blob.get("chain_id", make_chain_id("TOMORROW"))
            t_ref_day_key = blob.get("ref_day_key", latest_key)

            tomorrow_section = format_tomorrow_section(
                tomorrow_rows=t_rows,
                chain_id=t_chain_id,
                ref_day_key=t_ref_day_key,
                follow_day=0,
                max_items=12,
            )
            if tomorrow_section:
                tomorrow_section = "\n\n" + tomorrow_section
    except Exception as e:
        logger.exception("ALARM -> Tomorrow (Altın Liste) ekleme hatası: %s", e)
        tomorrow_section = ""
    foot = f"\n⏳ <i>Aynı hisse için {ALARM_COOLDOWN_MIN} dk cooldown aktif.</i>"
    if watch_rows:
        watch_table = make_table(watch_rows, "👀 <b>WATCHLIST (Alarm Eki)</b>", include_kind=True)
        return head + "\n" + alarm_table + "\n" + notes + "\n\n" + watch_table + foot + tomorrow_section
    return head + "\n" + alarm_table + "\n" + notes + foot + tomorrow_section


def can_send_alarm_for(ticker: str, now_ts: float) -> bool:
    last = LAST_ALARM_TS.get(ticker)
    if last is None:
        return True
    return (now_ts - last) >= (ALARM_COOLDOWN_MIN * 60)


def mark_alarm_sent(ticker: str, now_ts: float) -> None:
    if ticker:
        LAST_ALARM_TS[ticker] = now_ts


def filter_new_alarms(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now_ts = time.time()
    out: List[Dict[str, Any]] = []
    for r in rows:
        kind = r.get("signal_text", "")
        if kind not in ("TOPLAMA", "DİP TOPLAMA"):
            continue
        t = r.get("ticker", "")
        if not t:
            continue
        if can_send_alarm_for(t, now_ts):
            out.append(r)
    out = sorted(
        out,
        key=lambda x: (x.get("volume") or 0) if (x.get("volume") == x.get("volume")) else 0,
        reverse=True
    )
    return out


# =========================================================
# Whale
# =========================================================
def whale_already_sent_today() -> bool:
    k = today_key_tradingday()
    return int(WHALE_SENT_DAY.get(k, 0)) == 1


def mark_whale_sent_today() -> None:
    k = today_key_tradingday()
    WHALE_SENT_DAY[k] = 1
    save_whale_sent_day()


def pct_change(a: float, b: float) -> float:
    if b == 0 or a != a or b != b:
        return float("nan")
    return (a / b - 1.0) * 100.0


def build_whale_message(items: List[Dict[str, Any]], xu_close: float, xu_change: float, reg: Dict[str, Any]) -> str:
    now_s = now_tr().strftime("%H:%M")
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    yk = yesterday_key_tradingday()
    head = (
        f"🐋 <b>BALİNA DEVAM ALARMI</b> • <b>{now_s}</b> • <b>{BOT_VERSION}</b>\n"
        f"📊 <b>XU100</b>: {xu_close_s} • {xu_change_s}\n"
        f"{format_regime_line(reg)}\n"
        f"🧾 Referans: Dün ALTIN LİSTE (<code>{yk}</code>)\n"
        f"🎯 Filtre: Hacim ≥ <b>{WHALE_MIN_VOL_RATIO:.2f}x</b> • Düşüş ≥ <b>{WHALE_MAX_DRAWDOWN_PCT:.2f}%</b>\n"
    )
    if not items:
        return head + "\n❌ <b>Bugün “balina devam” kriterine uyan hisse çıkmadı.</b>"

    lines = [head, "\n<b>✅ DEVAM EDENLER</b>"]
    for it in items:
        t = it["ticker"]
        volr = it.get("vol_ratio", float("nan"))
        ch = it.get("change", float("nan"))
        dd = it.get("dd_pct", float("nan"))
        mark = it.get("mark", "🐋")
        volr_s = "n/a" if volr != volr else f"{volr:.2f}x"
        ch_s = "n/a" if ch != ch else f"{ch:+.2f}%"
        dd_s = "n/a" if dd != dd else f"{dd:+.2f}%"
        lines.append(f"{mark} <b>{t}</b> → Hacim: <b>{volr_s}</b> | Günlük: <b>{ch_s}</b> | Dün Ref’e göre: <b>{dd_s}</b>")
    lines.append("\n<i>Not: Bu alarm “dün seçilenlerin bugün de bırakılmadığını” yakalar. Spam yok → günde 1.</i>")
    return "\n".join(lines)


# =========================================================
# Telegram Handlers
# =========================================================
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
        f"🤖 <b>TAIPO PRO INTEL</b> • <b>{BOT_VERSION}</b>\n\n"
        "✅ <b>Komutlar</b>\n"
        "• /ping → bot çalışıyor mu?\n"
        "• /chatid → chat id\n"
        "• /watch → watchlist radar (örn: /watch AKBNK,CANTE)\n"
        "• /radar → BIST200 radar parça (örn: /radar 1)\n"
        "• /eod → manuel EOD raporu\n"
        "• /tomorrow → ertesi güne altın + aday liste\n"
        "• /whale → balina devam testi (dün altın listeye göre)\n"
        "• /alarm → alarm durumu/ayarlar\n"
        "• /stats → arşiv istatistik (örn: /stats AKBNK)\n"
        "• /bootstrap → Yahoo’dan geçmiş doldurma (1 defa)\n"
        "• /rejim → rejim durumu (R1/R2/R3 + MOMO)\n"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_help(update, context)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"🏓 Pong! ({BOT_VERSION})")


async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = update.effective_chat.id
    await update.message.reply_text(f"🆔 Chat ID: <code>{cid}</code>", parse_mode=ParseMode.HTML)


async def cmd_rejim(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global LAST_REGIME
    try:
        if LAST_REGIME:
            r = LAST_REGIME
        else:
            xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
            update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)
            r = compute_regime(xu_close, xu_change, xu_vol, xu_open)
            LAST_REGIME = r

        msg = (
            "🧭 <b>REJİM DURUMU</b>\n\n"
            f"• name: <code>{r.get('name')}</code>\n"
            f"• regime: <code>{r.get('regime')}</code>\n"
            f"• vol_ok: <code>{r.get('vol_ok')}</code>\n"
            f"• gap_ok: <code>{r.get('gap_ok')}</code>\n"
            f"• allow_trade: <code>{r.get('allow_trade')}</code>\n"
            f"• block: <code>{r.get('block')}</code>\n"
            f"• momo: <code>{r.get('momo')}</code>\n"
            f"• reason: <code>{r.get('reason')}</code>\n"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Rejim kontrol hatası: {e}", parse_mode=ParseMode.HTML)


async def cmd_alarm_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
    f"🚨 <b>ALARM PANELİ</b>\n"
    f"━━━━━━━━━━━━━━\n"
    f"• {'🟢 <b>AKTİF</b>' if ALARM_ENABLED else '🔴 <b>KAPALI</b>'}\n"
    f"• ⏱ Interval: <b>{ALARM_INTERVAL_MIN} dk</b>\n"
    f"• 🧯 Cooldown: <b>{ALARM_COOLDOWN_MIN} dk</b>\n"
    f"• 🕒 Tarama: <b>{ALARM_START_HOUR:02d}:{ALARM_START_MIN:02d}"
    f"–{ALARM_END_HOUR:02d}:{ALARM_END_MIN:02d}</b>\n"
    f"• ⏰ EOD: <b>{EOD_HOUR:02d}:{EOD_MINUTE:02d}</b>\n"
    f"• 🌍 TZ: <b>{TZ.key}</b>\n"
    f"• 🆔 ChatID: <code>{ALARM_CHAT_ID or 'YOK'}</code>\n"
    f"\n"
    f"🌙 <b>TOMORROW</b>\n"
    f"━━━━━━━━━━━━━━\n"
    f"• 🥇 <b>ALTIN</b>\n"
    f"  Band ≥ <b>%{TOMORROW_MAX_BAND:.0f}</b>\n"
    f"  VolRatio ≥ <b>{TOMORROW_MIN_VOL_RATIO:.2f}x</b>\n"
    f"  Max: <b>{TOMORROW_MAX_PICK}</b>\n"
    f"\n"
    f"• 🧪 <b>ADAY</b>\n"
    f"  Band ≥ <b>%{CANDIDATE_MAX_BAND:.0f}</b>\n"
    f"  VolRatio ≥ <b>{CANDIDATE_MIN_VOL_RATIO:.2f}x</b>\n"
    f"  Max: <b>{CANDIDATE_MAX_PICK}</b>\n"
    f"\n"
    f"🐳 <b>BALİNA</b>\n"
    f"━━━━━━━━━━━━━━\n"
    f"• {'🟢 ON' if WHALE_ENABLED else '🔴 OFF'}\n"
    f"• Window: <b>{WHALE_START_HOUR:02d}:{WHALE_START_MIN:02d}"
    f"–{WHALE_END_HOUR:02d}:{WHALE_END_MIN:02d}</b>\n"
    f"• Interval: <b>{WHALE_INTERVAL_MIN} dk</b>\n"
    f"• MinVolRatio: <b>{WHALE_MIN_VOL_RATIO:.2f}x</b>\n"
    f"• MaxDD: <b>{WHALE_MAX_DRAWDOWN_PCT:.2f}%</b>\n"
    f"\n"
    f"🧭 <b>REJİM</b>\n"
    f"━━━━━━━━━━━━━━\n"
    f"• Enabled: <b>{'1' if REJIM_ENABLED else '0'}</b>\n"
    f"• Lookback: <b>{REJIM_LOOKBACK}</b>\n"
    f"• Vol High: <b>{REJIM_VOL_HIGH:.2f}</b>\n"
    f"• SMA: <b>{REJIM_TREND_SMA_FAST}/{REJIM_TREND_SMA_SLOW}</b>\n"
    f"• GAP: <b>%{REJIM_GAP_PCT:.2f}</b>\n"
    f"• PrevDayBad: <b>%{REJIM_PREV_DAY_BAD:.2f}</b>\n"
    f"• Gate: alarm={int(REJIM_GATE_ALARM)} "
    f"tomorrow={int(REJIM_GATE_TOMORROW)} "
    f"radar={int(REJIM_GATE_RADAR)} "
    f"whale={int(REJIM_GATE_WHALE)} "
    f"eod={int(REJIM_GATE_EOD)}\n"
    f"\n"
    f"📦 <b>DATA</b>\n"
    f"━━━━━━━━━━━━━━\n"
    f"• DIR: <code>{EFFECTIVE_DATA_DIR}</code>\n"
    f"• History: <b>{HISTORY_DAYS}</b>\n"
    f"• Files: <code>{os.path.basename(PRICE_HISTORY_FILE)}</code>, "
    f"<code>{os.path.basename(VOLUME_HISTORY_FILE)}</code>, "
    f"<code>{os.path.basename(INDEX_HISTORY_FILE)}</code>\n"
    f"• LAST_ALARM: <b>{len(LAST_ALARM_TS)}</b>\n"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Kullanım: <code>/stats AKBNK</code>", parse_mode=ParseMode.HTML)
        return
    t = re.sub(r"[^A-Za-z0-9:_\.]", "", context.args[0]).upper().replace("BIST:", "")
    if not t:
        await update.message.reply_text("Kullanım: <code>/stats AKBNK</code>", parse_mode=ParseMode.HTML)
        return
    st = compute_30d_stats(t)
    if not st:
        await update.message.reply_text(f"❌ <b>{t}</b> için arşiv veri yok (disk yeni olabilir).", parse_mode=ParseMode.HTML)
        return
    ratio = st["ratio"]
    ratio_s = "n/a" if (ratio != ratio) else f"{ratio:.2f}x"
    msg = (
        f"📌 <b>{t}</b> • <b>Arşiv İstatistik</b>\n"
        f"• Close min/avg/max: <b>{st['min']:.2f}</b> / <b>{st['avg_close']:.2f}</b> / <b>{st['max']:.2f}</b>\n"
        f"• Ort. Hacim: <b>{format_volume(st['avg_vol'])}</b>\n"
        f"• Bugün Hacim: <b>{format_volume(st['today_vol'])}</b>\n"
        f"• Bugün / Ortalama: <b>{ratio_s}</b>\n"
        f"• Band: <b>%{st['band_pct']:.0f}</b>\n"
        f"• Key: <code>{today_key_tradingday()}</code>"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def cmd_bootstrap(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    days = BOOTSTRAP_DAYS
    if context.args:
        try:
            days = int(re.sub(r"\D+", "", context.args[0]))
        except Exception:
            days = BOOTSTRAP_DAYS
    days = max(20, min(400, days))
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return
    await update.message.reply_text(f"⏳ Bootstrap başlıyor… Yahoo’dan {days} gün çekiyorum (1 defalık).")
    tickers = [normalize_is_ticker(x).split(":")[-1] for x in bist200_list if x.strip()]
    filled, points = await asyncio.to_thread(yahoo_bootstrap_fill_history, tickers, days)
    await update.message.reply_text(
        f"✅ Bootstrap tamam!\n• Dolu hisse: <b>{filled}</b>\n• Nokta: <b>{points}</b>\n• Disk: <code>{EFFECTIVE_DATA_DIR}</code>\n• HISTORY_DAYS: <b>{HISTORY_DAYS}</b>",
        parse_mode=ParseMode.HTML
    )


async def cmd_tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return
    await update.message.reply_text("⏳ Ertesi gün listesi hazırlanıyor...")
    xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
    update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)
    reg = compute_regime(xu_close, xu_change, xu_vol, xu_open)

    global LAST_REGIME
    LAST_REGIME = reg

    rows = await build_rows_from_is_list(bist200_list, xu_change)
    update_history_from_rows(rows)
    min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
    thresh_s = format_threshold(min_vol)

    # ✅ R0 (Uçan) tespit edilenleri ayrı blokta göster
    r0_rows = [r for r in rows if r.get("signal_text") == "UÇAN (R0)"]
    r0_block = ""
    if r0_rows:
        r0_rows = sorted(
            r0_rows,
            key=lambda x: (x.get("volume") or 0) if x.get("volume") == x.get("volume") else 0,
            reverse=True
        )[:8]
        r0_block = make_table(r0_rows, "🚀 <b>R0 – UÇANLAR (Erken Yakalananlar)</b>", include_kind=True) + "\n\n"

    if REJIM_GATE_TOMORROW and reg.get("block"):
        msg = (
            f"🌙 <b>ERTESİ GÜNE TOPLAMA - RAPOR</b>\n"
            f"📊 <b>XU100</b>: {xu_close:,.2f} • {xu_change:+.2f}%\n\n"
            f"{format_regime_line(reg)}\n\n"
            f"⛔ <b>Rejim BLOK olduğu için Tomorrow listesi üretilmedi.</b>\n"
            f"• REJIM_BLOCK_ON: <code>{', '.join(REJIM_BLOCK_ON) if REJIM_BLOCK_ON else 'YOK'}</code>"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        return

    tom_rows = build_tomorrow_rows(rows)
    cand_rows = build_candidate_rows(rows, tom_rows)
    save_tomorrow_snapshot(tom_rows, xu_change)


# ✅ Tomorrow chain aç (ALTIN liste üzerinden takip edilir)
    try:
        ref_day_key = today_key_tradingday()
        open_or_update_tomorrow_chain(ref_day_key, tom_rows)
    except Exception as e:
        logger.warning("open_or_update_tomorrow_chain failed: %s", e)

    msg = r0_block + build_tomorrow_message(
        tom_rows,
        cand_rows,
        xu_close,
        xu_change,
        thresh_s,
        reg,
    )
    
# ✅ ALTIN canlı performans bloğu (/tomorrow'a ek)
    try:
        perf_section = build_tomorrow_altin_perf_section(tom_rows, TOMORROW_CHAINS)
    except Exception:
        perf_section = ""

    if perf_section:
        msg = msg + "\n\n" + perf_section


    await update.message.reply_text(
        msg,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    watch = parse_watch_args(context.args)
    if not watch:
        watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")
    watch = (watch or [])[:WATCHLIST_MAX]
    if not watch:
        await update.message.reply_text(
            "Kullanım: <code>/watch AKBNK,CANTE</code> (ya da WATCHLIST env)",
            parse_mode=ParseMode.HTML,
        )
        return

    xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
    update_index_history(
        today_key_tradingday(),
        xu_close,
        xu_change,
        xu_vol,
        xu_open,
    )
    reg = compute_regime(xu_close, xu_change, xu_vol, xu_open)

    global LAST_REGIME
    LAST_REGIME = reg

    rows = await build_rows_from_is_list(watch, xu_change)
    min_vol = compute_signal_rows(
        rows, xu_change, max(5, min(10, len(rows)))
    )
    if REJIM_GATE_RADAR and reg.get("block"):
        apply_regime_gate_to_rows(rows, reg)

    table = make_table(
        rows,
        f"👁️ <b>WATCHLIST RADAR</b> · TopEşik=<b>{format_threshold(min_vol)}</b>",
        include_kind=True,
    )
    head = (
        f"👁️ <b>WATCHLIST</b> · <b>{BOT_VERSION}</b>\n"
        f"📊 XU100: {xu_close:,.2f} · {xu_change:+.2f}%\n"
        f"{format_regime_line(reg)}\n"
    )

    await update.message.reply_text(
        head + "\n" + table,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    page = 1
    if context.args:
        try:
            page = int(re.sub(r"\D+", "", context.args[0]) or "1")
        except Exception:
            page = 1
    page = max(1, page)

    chunks = chunk_list(bist200_list, 25)
    if page > len(chunks):
        await update.message.reply_text(f"Sayfa yok. Toplam sayfa: {len(chunks)} (örn: /radar 1)")
        return

    xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
    update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)
    reg = compute_regime(xu_close, xu_change, xu_vol, xu_open)

    global LAST_REGIME
    LAST_REGIME = reg

    rows = await build_rows_from_is_list(chunks[page - 1], xu_change)
    update_history_from_rows(rows)
    min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
    thresh_s = format_threshold(min_vol)

    if REJIM_GATE_RADAR and reg.get("block"):
        apply_regime_gate_to_rows(rows, reg)

    # ✅ R0 (Uçan) olanları sayfada ayrı özetle
    r0_rows = [r for r in rows if r.get("signal_text") == "UÇAN (R0)"]
    r0_block = ""
    if r0_rows:
        r0_rows = sorted(
            r0_rows,
            key=lambda x: (x.get("volume") or 0) if x.get("volume") == x.get("volume") else 0,
            reverse=True
        )[:8]
        r0_block = make_table(r0_rows, "🚀 <b>R0 – UÇANLAR (Bu sayfada)</b>", include_kind=True) + "\n\n"

    table = make_table(rows, f"📡 <b>BIST200 RADAR</b> • Sayfa {page}/{len(chunks)} • Top{VOLUME_TOP_N}≥<b>{thresh_s}</b>", include_kind=True)
    head = (
        f"📡 <b>RADAR</b> • <b>{BOT_VERSION}</b>\n"
        f"📊 XU100: {xu_close:,.2f} • {xu_change:+.2f}%\n"
        f"{format_regime_line(reg)}\n"
    )
    await update.message.reply_text(head + "\n" + r0_block + table, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return
    await update.message.reply_text("⏳ EOD raporu hazırlanıyor...")

    xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
    update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)
    reg = compute_regime(xu_close, xu_change, xu_vol, xu_open)

    global LAST_REGIME
    LAST_REGIME = reg

    if REJIM_GATE_EOD and reg.get("block"):
        msg = (
            f"📌 <b>EOD RAPOR</b> • <b>{BOT_VERSION}</b>\n"
            f"📊 <b>XU100</b>: {xu_close:,.2f} • {xu_change:+.2f}%\n"
            f"{format_regime_line(reg)}\n\n"
            f"⛔️ <b>Rejim BLOK (EOD gate açık).</b>"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        return

    rows = await build_rows_from_is_list(bist200_list, xu_change)
    update_history_from_rows(rows)
    min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
    thresh_s = format_threshold(min_vol)

    toplama = [r for r in rows if r.get("signal_text") == "TOPLAMA"]
    dip = [r for r in rows if r.get("signal_text") == "DİP TOPLAMA"]
    ayr = [r for r in rows if r.get("signal_text") == "AYRIŞMA"]
    kar = [r for r in rows if r.get("signal_text") == "KÂR KORUMA"]

    def top_by_vol(lst: List[Dict[str, Any]], n: int = 10) -> List[Dict[str, Any]]:
        return sorted(lst, key=lambda x: (x.get("volume") or 0) if x.get("volume") == x.get("volume") else 0, reverse=True)[:n]

    msg = (
        f"📌 <b>EOD RAPOR</b> • <b>{BOT_VERSION}</b>\n"
        f"📊 <b>XU100</b>: {xu_close:,.2f} • {xu_change:+.2f}%\n"
        f"{format_regime_line(reg)}\n"
        f"🧱 <b>Top{VOLUME_TOP_N} Eşik</b>: ≥ <b>{thresh_s}</b>\n\n"
        f"🧠 TOPLAMA: <b>{len(toplama)}</b> | 🧲 DİP: <b>{len(dip)}</b> | 🧠 AYR: <b>{len(ayr)}</b> | ⚠️ KAR: <b>{len(kar)}</b>\n"
    )
    msg += "\n" + make_table(top_by_vol(toplama, 8), "🧠 <b>TOPLAMA – Top 8</b>", include_kind=True)
    msg += "\n\n" + make_table(top_by_vol(dip, 8), "🧲 <b>DİP TOPLAMA – Top 8</b>", include_kind=True)
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def cmd_whale(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not WHALE_ENABLED:
        await update.message.reply_text("🐋 Whale kapalı (WHALE_ENABLED=0).")
        return
    y_items = load_yesterday_tomorrow_snapshot()
    if not y_items:
        await update.message.reply_text("🐋 Dün için ALTIN snapshot yok. Önce /tomorrow çalıştır (EOD’de otomatik de kaydeder).")
        return

    tickers = [it.get("ticker") for it in y_items if it.get("ticker")]
    xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
    update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)
    reg = compute_regime(xu_close, xu_change, xu_vol, xu_open)

    global LAST_REGIME
    LAST_REGIME = reg

    if REJIM_GATE_WHALE and reg.get("block"):
        await update.message.reply_text(f"{format_regime_line(reg)}\n\n⛔️ Rejim BLOK → whale kontrolü atlandı.", parse_mode=ParseMode.HTML)
        return

    rows = await build_rows_from_is_list(tickers, xu_change)
    update_history_from_rows(rows)

    ref_map = {it["ticker"]: safe_float(it.get("ref_close")) for it in y_items if it.get("ticker")}
    out = []
    for r in rows:
        t = r.get("ticker", "")
        if not t or t not in ref_map:
            continue
        ref_close = ref_map[t]
        st = compute_30d_stats(t)
        if not st:
            continue
        avg_vol = st.get("avg_vol", float("nan"))
        today_vol = r.get("volume", float("nan"))
        today_close = r.get("close", float("nan"))
        ch = r.get("change", float("nan"))

        vol_ratio = (today_vol / avg_vol) if (avg_vol == avg_vol and avg_vol > 0 and today_vol == today_vol) else float("nan")
        dd_pct = pct_change(today_close, ref_close)

        if vol_ratio != vol_ratio or vol_ratio < WHALE_MIN_VOL_RATIO:
            continue
        if dd_pct != dd_pct or dd_pct < WHALE_MAX_DRAWDOWN_PCT:
            continue

        mark = "🐋"
        if WHALE_INDEX_BONUS and (xu_change == xu_change) and (xu_change <= 0) and (ch == ch) and (ch >= WHALE_MIN_POSITIVE_WHEN_INDEX_BAD):
            mark = "🐋🐋"

        out.append({
            "ticker": t,
            "vol_ratio": float(vol_ratio),
            "change": float(ch) if ch == ch else float("nan"),
            "dd_pct": float(dd_pct),
            "mark": mark,
        })

    out.sort(key=lambda x: (x.get("mark") == "🐋🐋", x.get("vol_ratio", 0)), reverse=True)
    msg = build_whale_message(out[:12], xu_close, xu_change, reg)
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


# =========================================================
# Scheduled jobs
# =========================================================
async def job_alarm_scan(context: ContextTypes.DEFAULT_TYPE, force: bool = False) -> None:
    if not ALARM_ENABLED or not ALARM_CHAT_ID:
        return
    if not force and not within_alarm_window(now_tr()):
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return

    try:
        # XU100
        xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
        update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)
        reg = compute_regime(xu_close, xu_change, xu_vol, xu_open)

        global LAST_REGIME
        LAST_REGIME = reg

        if REJIM_GATE_ALARM and reg.get("block"):
            return

        # --- Ana liste (BIST200) ---
        all_rows = await build_rows_from_is_list(bist200_list, xu_change)
        update_history_from_rows(all_rows)
        min_vol = compute_signal_rows(all_rows, xu_change, VOLUME_TOP_N)
        thresh_s = format_threshold(min_vol)

        alarm_rows = filter_new_alarms(all_rows)
        if not alarm_rows:
            return

        ts_now = time.time()
        for r in alarm_rows:
            mark_alarm_sent(r.get("ticker", ""), ts_now)
        save_last_alarm_ts()

        # --- Watchlist ---
        watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")
        watch = (watch or [])[:WATCHLIST_MAX]
        w_rows = await build_rows_from_is_list(watch, xu_change) if watch else []
        if w_rows:
            _apply_signals_with_threshold(w_rows, xu_change, min_vol)
    except Exception as e:
        logger.exception("Alarm job error: %s", e)
        return

async def job_altin_follow(context: ContextTypes.DEFAULT_TYPE, force: bool = False) -> None:
    """Tomorrow ALTIN listesini canlı takip eder ve periyodik mesaj yollar."""
    if not ALARM_ENABLED or not ALARM_CHAT_ID:
        return

    # otomatik job saat penceresine uysun; manuel (force=True) her zaman çalışsın
    if (not force) and (not within_alarm_window(now_tr())):
        return

    try:
        # XU100 özetini al (piyasa kapalıysa değişim 0/None olabilir)
        xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
        update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)

      # Tomorrow zinciri yoksa otomatik üret
        if not TOMORROW_CHAINS:
            logger.info("ALTIN follow: Tomorrow zinciri yok, otomatik üretiliyor.")
            await job_tomorrow_list(context)

        # HÂLÂ yoksa gerçekten hata ver
        if not TOMORROW_CHAINS:
            await context.bot.send_message(
                chat_id=int(ALARM_CHAT_ID),
                text="⚠️ Tomorrow zinciri üretilemedi. /tomorrow komutunu manuel dene.",
                parse_mode=ParseMode.HTML
            )
            return 

        latest_key = max(
            TOMORROW_CHAINS.keys(),
            key=lambda k: (TOMORROW_CHAINS.get(k, {}) or {}).get("ts", 0)
        )
        chain = TOMORROW_CHAINS.get(latest_key, {}) or {}

        rows = chain.get("rows", []) or []
        ref_close_map = chain.get("ref_close", {}) or {}
        # ✅ zinciri RAM'e kaydet (ALTIN follow buradan okuyacak)
        key = latest_key

        TOMORROW_CHAINS[key] = {
        "ts": time.time(),
        "rows": rows,
        "ref_close": ref_close_map,
         }

        logger.info("Tomorrow zinciri kaydedildi | key=%s | rows=%d", key, len(rows))

        altin_tickers = []
        for r in rows:
            t = (r.get("ticker") or "").strip()
            if not t:
                continue
            kind = (r.get("kind") or r.get("list") or r.get("bucket") or "").strip().upper()
            if "ALTIN" in kind:
                altin_tickers.append(t)

        # fallback: ref_close_map'ten ilk 6
        if not altin_tickers:
            altin_tickers = list(ref_close_map.keys())[:6]

        if not altin_tickers:
            await context.bot.send_message(
                chat_id=int(ALARM_CHAT_ID),
                text="⚠️ ALTIN listesi boş görünüyor. /tomorrow çıktısında ALTIN oluşmuş mu kontrol et.",
                parse_mode=ParseMode.HTML
            )
            return

        # ana listeden (BIST200) anlık row'ları çek
        bist200_list = env_csv("BIST200_TICKERS")
        if not bist200_list:
            await context.bot.send_message(
                chat_id=int(ALARM_CHAT_ID),
                text="⚠️ BIST200_TICKERS env boş. Alarm taraması yapılamıyor.",
                parse_mode=ParseMode.HTML
            )
            return

        all_rows = await build_rows_from_is_list(bist200_list, xu_change)
        all_map = { (r.get("ticker") or "").strip(): r for r in (all_rows or []) if (r.get("ticker") or "").strip() }

        # performans satırlarını üret
        perf_lines = []
        for t in altin_tickers[:6]:
            ref_close = safe_float(ref_close_map.get(t))
            now_row = all_map.get(t) or {}
            now_close = safe_float(now_row.get("close"))
            dd = pct_change(now_close, ref_close)  # yüzde değişim

            # emoji
            if dd == dd:  # NaN değilse
                emo = "🟢" if dd > 0 else ("🔴" if dd < 0 else "⚪️")
                dd_s = f"{dd:+.2f}%"
            else:
                emo = "⚪️"
                dd_s = "N/A"

            ref_s = f"{ref_close:.2f}" if ref_close == ref_close else "N/A"
            now_s = f"{now_close:.2f}" if now_close == now_close else "N/A"

            perf_lines.append((t, emo, dd_s, now_s, ref_s))

        header = (
            f"✅ <b>ALTIN CANLI TAKİP</b>\n"
            f"🕒 <b>{now_tr().strftime('%H:%M')}</b>\n"
            f"📌 Referans: <code>{latest_key}</code>\n"
            f"📈 XU100: <b>{xu_close:,.2f}</b>  ({xu_change:+.2f}%)\n\n"
            f"<pre>TICKER  Δ%     NOW     REF\n"
            f"---------------------------\n"
        )

        body = "\n".join([f"{t:<6} {emo} {dd_s:>7} {now_s:>7} {ref_s:>7}" for (t, emo, dd_s, now_s, ref_s) in perf_lines])
        text = header + body + "</pre>"

        await context.bot.send_message(
            chat_id=int(ALARM_CHAT_ID),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )

    except Exception as e:
        logger.exception("ALTIN follow job error: %s", e)
        await context.bot.send_message(
            chat_id=int(ALARM_CHAT_ID),
            text=f"❌ ALTIN takip hata:\n<code>{e}</code>",
            parse_mode=ParseMode.HTML
        )
        
        # =========================================================
        # ✅ Tomorrow ALTIN canlı performans bloğu (Alarm'a ek) + EMOJI
        # =========================================================
        tomorrow_perf_section = ""
        try:
            # all_rows -> hızlı lookup (ticker -> row)
            all_map = {
                (r.get("ticker") or "").strip(): r
                for r in (all_rows or [])
                if (r.get("ticker") or "").strip()
            }

            if TOMORROW_CHAINS:
                latest_key = max(
                    TOMORROW_CHAINS.keys(),
                    key=lambda k: (TOMORROW_CHAINS.get(k, {}) or {}).get("ts", 0),
                )
                chain = TOMORROW_CHAINS.get(latest_key, {}) or {}

                # 1) ALTIN tickers'ı bul (önce chain.rows'tan, yoksa ref_close'tan fallback)
                altin_tickers = []
                t_rows = chain.get("rows", []) or []
                for rr in t_rows:
                    t = (rr.get("ticker") or "").strip()
                    if not t:
                        continue
                    kind = (rr.get("kind") or rr.get("list") or rr.get("bucket") or "").strip().upper()
                    if "ALTIN" in kind:
                        altin_tickers.append(t)

                ref_close_map = chain.get("ref_close", {}) or {}
                if not altin_tickers:
                    altin_tickers = list(ref_close_map.keys())[:6]

                # 2) ref_close ile güncel close kıyasla
                perf_lines = []
                for t in altin_tickers[:6]:
                    ref_close = safe_float(ref_close_map.get(t))
                    now_row = all_map.get(t) or {}
                    now_close = safe_float(now_row.get("close"))

                    # pct_change NaN olabilir
                    dd = pct_change(now_close, ref_close)

                    # sade emoji
                    if dd == dd:
                        if dd > 0:
                            mark = "🟢"
                        elif dd < 0:
                            mark = "🔴"
                        else:
                            mark = "⚪"
                        dd_s = f"{mark} {dd:+.2f}%"
                    else:
                        dd_s = "⚪ n/a"

                    now_s = f"{now_close:.2f}" if now_close == now_close else "n/a"
                    ref_s = f"{ref_close:.2f}" if ref_close == ref_close else "n/a"

                    perf_lines.append((t, dd_s, now_s, ref_s))

                if perf_lines:
                    header = "\n\n🌙 <b>TOMORROW • ALTIN (Canlı)</b>\n"
                    lines = []
                    lines.append("HIS   Δ%          NOW      REF")
                    lines.append("-------------------------------")
                    for (t, dd_s, now_s, ref_s) in perf_lines:
                        # dd_s emoji + yüzde olduğu için genişlik biraz daha fazla
                        lines.append(f"{t:<5} {dd_s:<11}  {now_s:>7}  {ref_s:>7}")
                    tomorrow_perf_section = header + "<pre>" + "\n".join(lines) + "</pre>"

        except Exception as e:
            logger.exception("ALARM -> Tomorrow performans ekleme hatası: %s", e)
            tomorrow_perf_section = ""

        # --- Alarm mesajını üret ---
        text = build_alarm_message(
            alarm_rows=alarm_rows,
            watch_rows=w_rows,
            xu_close=xu_close,
            xu_change=xu_change,
            thresh_s=thresh_s,
            top_n=VOLUME_TOP_N,
            reg=reg,
        )

        # ✅ Alarm mesajının sonuna ekle
        if tomorrow_perf_section:
            text = text + tomorrow_perf_section

        await context.bot.send_message(
            chat_id=int(ALARM_CHAT_ID),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )

    except Exception as e:
        logger.exception("Alarm job error: %s", e)

async def cmd_alarm_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await update.message.reply_text("⏳ ALTIN takip manuel tetikleniyor...")
        await job_altin_follow(context, force=True)
    except Exception as e:
        await update.message.reply_text(
            f"❌ ALTIN takip çalıştırılamadı:\n<code>{e}</code>",
            parse_mode=ParseMode.HTML
        )
        
async def job_tomorrow_list(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not ALARM_ENABLED or not ALARM_CHAT_ID:
        return
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return
    try:
        if TOMORROW_DELAY_MIN > 0:
            await asyncio.sleep(max(0, int(TOMORROW_DELAY_MIN)) * 60)

        xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
        update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)
        reg = compute_regime(xu_close, xu_change, xu_vol, xu_open)

        global LAST_REGIME
        LAST_REGIME = reg

        if REJIM_GATE_TOMORROW and reg.get("block"):
            msg = (
                f"🌙 <b>ERTESİ GÜNE TOPLAMA – RAPOR</b>\n"
                f"📊 <b>XU100</b>: {xu_close:,.2f} • {xu_change:+.2f}%\n"
                f"{format_regime_line(reg)}\n\n"
                f"⛔️ <b>Rejim BLOK olduğu için Tomorrow listesi gönderilmedi.</b>"
            )
            await context.bot.send_message(
                chat_id=int(ALARM_CHAT_ID),
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            return

        rows = await build_rows_from_is_list(bist200_list, xu_change)
        update_history_from_rows(rows)
        min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
        thresh_s = format_threshold(min_vol)

        # ✅ R0 bloğu (otomatik gönderimde de üstte görünsün)
        r0_rows = [r for r in rows if r.get("signal_text") == "UÇAN (R0)"]
        r0_block = ""
        if r0_rows:
            r0_rows = sorted(
                r0_rows,
                key=lambda x: (x.get("volume") or 0) if x.get("volume") == x.get("volume") else 0,
                reverse=True
            )[:8]
            r0_block = make_table(r0_rows, "🚀 <b>R0 – UÇANLAR (Erken Yakalananlar)</b>", include_kind=True) + "\n\n"

        tom_rows = build_tomorrow_rows(rows)
        cand_rows = build_candidate_rows(rows, tom_rows)
        save_tomorrow_snapshot(tom_rows, xu_change)
        
        # ==============================
        # ✅ TOMORROW ZİNCİRİ RAM'E YAZ
        # ==============================
        global TOMORROW_CHAINS

        key = today_key_tradingday()  # follow ile aynı key

    TOMORROW_CHAINS[key] = {
    "ts": time.time(),
    "rows": tom_rows,
    "ref_close": {r["symbol"]: r.get("ref_close") for r in tom_rows if r.get("symbol")},
}

logger.info(
    "Tomorrow zinciri RAM'e yazıldı | key=%s | rows=%d",
    key,
    len(tom_rows)
)

        msg = r0_block + build_tomorrow_message(tom_rows, cand_rows, xu_close, xu_change, thresh_s, reg)
        await context.bot.send_message(
            chat_id=int(ALARM_CHAT_ID),
            text=msg,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.exception("Tomorrow job error: %s", e)

async def job_tomorrow_follow(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not TOMORROW_FOLLOW_ENABLED:
        return

    now = now_tr()
    if not within_tomorrow_follow_window(now):
        return

    chat_id = ALARM_CHAT_ID
    if not chat_id:
        return

    # XU100 bilgisi
    xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()

    reg = LAST_REGIME or {}

    changed = False

    for ref_day_key, chain in list(TOMORROW_CHAINS.items()):
        if not isinstance(chain, dict):
            continue
        if chain.get("closed"):
            continue

        # kaçıncı gün?
        try:
            age = (date.fromisoformat(today_key_tradingday()) - date.fromisoformat(ref_day_key)).days
        except Exception:
            age = 0

        # ✅ 2 gün kuralı (T+1 ve T+2)
        if age >= TOMORROW_CHAIN_MAX_AGE:
            chain["closed"] = True
            changed = True
            continue

        tickers = list((chain.get("ref_close") or {}).keys())
        if not tickers:
            continue

        rows = await build_rows_from_is_list(tickers, xu_change)

        now_prices = {
            (r.get("ticker") or "").strip(): safe_float(r.get("close"))
            for r in rows
            if (r.get("ticker") and r.get("close") is not None)
        }

        prev_prices = None
        checkpoints = chain.get("checkpoints", [])
        if checkpoints:
            prev_prices = checkpoints[-1].get("prices")

        msg = build_tomorrow_follow_message(
            day_key=today_key_tradingday(),
            ref_day=ref_day_key,
            age=age,
            reg=reg,
            xu_close=xu_close,
            xu_change=xu_change,
            now_prices=now_prices,
            prev_prices=prev_prices,
        )

        await context.bot.send_message(
            chat_id=chat_id,
            text=msg,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

        # checkpoint ekle
        checkpoints = chain.get("checkpoints", []) or []
        checkpoints.append({
            "t": now.isoformat(),
            "day_key": today_key_tradingday(),
            "prices": now_prices,
        })
        chain["checkpoints"] = checkpoints
        changed = True

    if changed:
        save_tomorrow_chains()
        
async def job_whale_follow(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not WHALE_ENABLED or not ALARM_CHAT_ID:
        return
    if not within_whale_window(now_tr()):
        return
    if whale_already_sent_today():
        return

    y_items = load_yesterday_tomorrow_snapshot()
    if not y_items:
        return

    try:
        tickers = [it.get("ticker") for it in y_items if it.get("ticker")]
        if not tickers:
            return

        xu_close, xu_change, xu_vol, xu_open = await get_xu100_summary()
        update_index_history(today_key_tradingday(), xu_close, xu_change, xu_vol, xu_open)
        reg = compute_regime(xu_close, xu_change, xu_vol, xu_open)

        global LAST_REGIME
        LAST_REGIME = reg

        if REJIM_GATE_WHALE and reg.get("block"):
            return

        rows = await build_rows_from_is_list(tickers, xu_change)
        update_history_from_rows(rows)

        ref_map = {it["ticker"]: safe_float(it.get("ref_close")) for it in y_items if it.get("ticker")}
        out = []
        for r in rows:
            t = r.get("ticker", "")
            if not t or t not in ref_map:
                continue
            ref_close = ref_map[t]
            st = compute_30d_stats(t)
            if not st:
                continue
            avg_vol = st.get("avg_vol", float("nan"))
            today_vol = r.get("volume", float("nan"))
            today_close = r.get("close", float("nan"))
            ch = r.get("change", float("nan"))

            vol_ratio = (today_vol / avg_vol) if (avg_vol == avg_vol and avg_vol > 0 and today_vol == today_vol) else float("nan")
            dd_pct = pct_change(today_close, ref_close)

            if vol_ratio != vol_ratio or vol_ratio < WHALE_MIN_VOL_RATIO:
                continue
            if dd_pct != dd_pct or dd_pct < WHALE_MAX_DRAWDOWN_PCT:
                continue

            mark = "🐋"
            if WHALE_INDEX_BONUS and (xu_change == xu_change) and (xu_change <= 0) and (ch == ch) and (ch >= WHALE_MIN_POSITIVE_WHEN_INDEX_BAD):
                mark = "🐋🐋"

            out.append({
                "ticker": t,
                "vol_ratio": float(vol_ratio),
                "change": float(ch) if ch == ch else float("nan"),
                "dd_pct": float(dd_pct),
                "mark": mark,
            })

        if not out:
            return

        out.sort(key=lambda x: (x.get("mark") == "🐋🐋", x.get("vol_ratio", 0)), reverse=True)
        msg = build_whale_message(out[:12], xu_close, xu_change, reg)

        await context.bot.send_message(
            chat_id=int(ALARM_CHAT_ID),
            text=msg,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        mark_whale_sent_today()
    except Exception as e:
        logger.exception("Whale job error: %s", e)


def schedule_jobs(app: Application) -> None:
    jq = getattr(app, "job_queue", None)
    if jq is None:
        logger.warning("JobQueue yok → otomatik alarm/tomorrow/whale ÇALIŞMAZ. Komutlar çalışır.")
        return

    if ALARM_ENABLED and ALARM_CHAT_ID:
        first = next_aligned_run(ALARM_INTERVAL_MIN)
        jq.run_repeating(
            job_alarm_scan,
            interval=ALARM_INTERVAL_MIN * 60,
            first=first,
            name="alarm_scan_repeating"
        )
        logger.info("Alarm scan scheduled every %d min. First=%s", ALARM_INTERVAL_MIN, first.isoformat())

        jq.run_daily(
            job_tomorrow_list,
            time=datetime(2000, 1, 1, EOD_HOUR, EOD_MINUTE, tzinfo=TZ).timetz(),
            name="tomorrow_daily_at_eod_time"
        )
        logger.info("Tomorrow scheduled daily at %02d:%02d (+%dmin delay)", EOD_HOUR, EOD_MINUTE, TOMORROW_DELAY_MIN)
    else:
        logger.info("ALARM kapalı veya ALARM_CHAT_ID yok → otomatik alarm/tomorrow gönderilmeyecek.")

    if WHALE_ENABLED and ALARM_CHAT_ID:
        first_w = next_aligned_run(WHALE_INTERVAL_MIN)
        jq.run_repeating(
            job_whale_follow,
            interval=WHALE_INTERVAL_MIN * 60,
            first=first_w,
            name="whale_follow_repeating"
        )
        logger.info("Whale follow scheduled every %d min. First=%s", WHALE_INTERVAL_MIN, first_w.isoformat())
    else:
        logger.info("WHALE kapalı veya ALARM_CHAT_ID yok → whale gönderilmeyecek.")

    # ✅ Tomorrow follow (chain tracking)
    if TOMORROW_FOLLOW_ENABLED and ALARM_CHAT_ID and getattr(app, "job_queue", None) is not None:
        first_tf = next_aligned_run(TOMORROW_FOLLOW_INTERVAL_MIN)
        app.job_queue.run_repeating(
            job_altin_follow,
            interval=TOMORROW_FOLLOW_INTERVAL_MIN * 60,
            first=first_tf,
            name="tomorrow_follow_repeating",
        )
        logger.info(
            "Tomorrow follow scheduled every %d min. First=%s",
            TOMORROW_FOLLOW_INTERVAL_MIN,
            first_tf.isoformat(),
        )
        
# =========================================================
# Global error handler
# =========================================================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error: %s", context.error)


# =========================================================
# Main
# =========================================================
def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip() or os.getenv("TELEGRAM_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env missing")
        

    load_last_alarm_ts()
    load_whale_sent_day()
    load_tomorrow_chains()

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("alarm", cmd_alarm_status))
    app.add_handler(CommandHandler("rejim", cmd_rejim))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("tomorrow", cmd_tomorrow))
    app.add_handler(CommandHandler("whale", cmd_whale))
    app.add_handler(CommandHandler("bootstrap", cmd_bootstrap))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("radar", cmd_radar))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("alarm_run", cmd_alarm_run))
    
    app.add_error_handler(on_error)

    schedule_jobs(app)

    logger.info(
        "Bot starting... version=%s tz=%s data_dir=%s history_days=%s rejim=%s",
        BOT_VERSION, TZ.key, EFFECTIVE_DATA_DIR, HISTORY_DAYS, int(REJIM_ENABLED)
    )

    async def post_start_bootstrap(ctx: ContextTypes.DEFAULT_TYPE) -> None:
        msg = await yahoo_bootstrap_if_needed()
        logger.info("Post-start: %s", msg)

    if getattr(app, "job_queue", None) is not None:
        app.job_queue.run_once(post_start_bootstrap, when=2, name="post_start_bootstrap")
    else:
        logger.warning("JobQueue yok → post-start bootstrap çalışmaz. Gerekirse /bootstrap kullan.")

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
