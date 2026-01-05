# main.py — TAIPO PRO INTEL (FINAL)
# requirements.txt:
#   python-telegram-bot[job-queue]==22.5
#   requests==2.*
# Render: 1 adet Background Worker (polling) çalıştır

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

# -----------------------------
# Config
# -----------------------------
BOT_VERSION = (
    os.getenv("BOT_VERSION", "v1.6.0-premium-yahoo-bootstrap-tradingdaykey-torpil")
    .strip()
    or "v1.6.0-premium-yahoo-bootstrap-tradingdaykey-torpil"
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_NUM = getattr(logging, LOG_LEVEL, logging.INFO)

logging.basicConfig(
    level=LOG_LEVEL_NUM,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = int(os.getenv("TV_TIMEOUT", "12"))
TZ = ZoneInfo(os.getenv("TZ", "Europe/Istanbul"))

# Alarm config
ALARM_ENABLED = os.getenv("ALARM_ENABLED", "1").strip() == "1"
ALARM_CHAT_ID = os.getenv("ALARM_CHAT_ID", "").strip()  # group chat id ex: -100...
ALARM_INTERVAL_MIN = int(os.getenv("ALARM_INTERVAL_MIN", "30"))
ALARM_COOLDOWN_MIN = int(os.getenv("ALARM_COOLDOWN_MIN", "60"))

# Tarama saat aralığı (default: 10:00 - 17:30)
ALARM_START_HOUR = int(os.getenv("ALARM_START_HOUR", "10"))
ALARM_START_MIN = int(os.getenv("ALARM_START_MIN", "0"))
ALARM_END_HOUR = int(os.getenv("ALARM_END_HOUR", "17"))
ALARM_END_MIN = int(os.getenv("ALARM_END_MIN", "30"))

# EOD
EOD_HOUR = int(os.getenv("EOD_HOUR", "17"))
EOD_MINUTE = int(os.getenv("EOD_MINUTE", "50"))

# Tomorrow list – EOD’den kaç dk sonra gitsin?
TOMORROW_DELAY_MIN = int(os.getenv("TOMORROW_DELAY_MIN", "2"))

# Watchlist
WATCHLIST_MAX = int(os.getenv("WATCHLIST_MAX", "12"))

# TopN hacim eşiği (Top10 yerine Top50 default)
VOLUME_TOP_N = int(os.getenv("VOLUME_TOP_N", "50"))

# Disk / 30G arşiv
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "30"))
ALARM_NOTE_MAX = int(os.getenv("ALARM_NOTE_MAX", "6"))

# Tomorrow list filtreleri (PRO)
TOMORROW_MAX = int(os.getenv("TOMORROW_MAX", "12"))
TOMORROW_MIN_VOL_RATIO = float(os.getenv("TOMORROW_MIN_VOL_RATIO", "1.20"))
TOMORROW_MAX_BAND = float(os.getenv("TOMORROW_MAX_BAND", "65"))
TOMORROW_INCLUDE_AYRISMA = os.getenv("TOMORROW_INCLUDE_AYRISMA", "0").strip() == "1"

# Torpil Modu (yalnızca disk veri azken, otomatik kapanır)
TORPIL_ENABLED = os.getenv("TORPIL_ENABLED", "1").strip() == "1"
TORPIL_MIN_SAMPLES = int(os.getenv("TORPIL_MIN_SAMPLES", "10"))
TORPIL_MIN_VOL_RATIO = float(os.getenv("TORPIL_MIN_VOL_RATIO", "1.05"))
TORPIL_MAX_BAND = float(os.getenv("TORPIL_MAX_BAND", "75"))

# Yahoo bootstrap (1 defalık geçmiş doldurma)
BOOTSTRAP_ON_START = os.getenv("BOOTSTRAP_ON_START", "1").strip() == "1"
BOOTSTRAP_DAYS = int(os.getenv("BOOTSTRAP_DAYS", "60"))
BOOTSTRAP_FORCE = os.getenv("BOOTSTRAP_FORCE", "0").strip() == "1"
YAHOO_TIMEOUT = int(os.getenv("YAHOO_TIMEOUT", "15"))
YAHOO_SLEEP_SEC = float(os.getenv("YAHOO_SLEEP_SEC", "0.15"))

# In-memory cooldown store: { "TICKER": last_sent_unix }
LAST_ALARM_TS: Dict[str, float] = {}

# -----------------------------
# Helpers
# -----------------------------
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
    t = (t or "").strip().upper()
    if not t:
        return t
    base = t.replace("BIST:", "") if t.startswith("BIST:") else t
    if base.endswith(".IS"):
        base = base[:-3]
    return f"BIST:{base}"


def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


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


def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


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


def st_short(sig_text: str) -> str:
    if sig_text == "TOPLAMA":
        return "TOP"
    if sig_text == "DİP TOPLAMA":
        return "DIP"
    if sig_text == "AYRIŞMA":
        return "AYR"
    if sig_text == "KÂR KORUMA":
        return "KAR"
    return ""


def safe_int_chat_id(raw: str) -> Optional[int]:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


ALARM_CHAT_ID_INT = safe_int_chat_id(ALARM_CHAT_ID)

# -----------------------------
# Trading-day key (Market kapalıysa son işlem gününü baz al)
# - Cumartesi/Pazar -> Cuma
# - Hafta içi ama 10:00'dan önce -> bir önceki iş günü (Pazartesi sabahı -> Cuma)
# -----------------------------
def prev_business_day(d: date) -> date:
    dd = d
    while True:
        dd = dd - timedelta(days=1)
        if dd.weekday() < 5:  # 0=Mon..4=Fri
            return dd


def trading_day_for_snapshot(dt: datetime) -> date:
    # weekend -> Friday
    if dt.weekday() == 5:  # Sat
        return dt.date() - timedelta(days=1)
    if dt.weekday() == 6:  # Sun
        return dt.date() - timedelta(days=2)

    # weekday but before market open -> prev business day
    if dt.timetz().replace(tzinfo=None) < dtime(ALARM_START_HOUR, ALARM_START_MIN):
        return prev_business_day(dt.date())

    return dt.date()


def today_key_tradingday() -> str:
    return trading_day_for_snapshot(now_tr()).strftime("%Y-%m-%d")


# -----------------------------
# Disk storage (30G daily history)
# -----------------------------
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


def update_history_from_rows(rows: List[Dict[str, Any]]) -> None:
    """
    Her taramada/EOD'de:
    - price_history.json: { "YYYY-MM-DD": {"AKBNK": close, ...} }
    - volume_history.json: { "YYYY-MM-DD": {"AKBNK": volume, ...} }
    Market kapalıysa (Pazar) -> Cuma gününe yazar.
    """
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


def compute_30d_stats(ticker: str) -> Optional[Dict[str, Any]]:
    """
    30G:
    - min/max/avg close
    - 30g avg vol
    - today vol & today close (bugün yoksa son gün)
    - today/avg vol ratio
    - band konumu (%0..%100)
    """
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
        return "Plan: Veri yetersiz (30g dolsun)."

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
        return f"• <b>{ticker}</b>: 30G veri yok (disk yeni) ⏳"

    mn = st["min"]
    mx = st["max"]
    avc = st["avg_close"]
    avv = st["avg_vol"]
    tv = st["today_vol"]
    ratio = st["ratio"]
    band = st["band_pct"]

    ratio_s = "n/a" if (ratio != ratio) else f"{ratio:.2f}x"
    plan = soft_plan_line(st, current_close)

    return (
        f"• <b>{ticker}</b>: 30G Close min/avg/max <b>{mn:.2f}</b>/<b>{avc:.2f}</b>/<b>{mx:.2f}</b> • "
        f"30G Ort.Hcm <b>{format_volume(avv)}</b> • Bugün <b>{format_volume(tv)}</b> • "
        f"<b>{ratio_s}</b> • Band <b>%{band:.0f}</b>\n"
        f"  ↳ <i>{plan}</i>"
    )


# -----------------------------
# TradingView Scanner (SYNC -> thread)
# -----------------------------
def tv_scan_symbols_sync(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}

    payload = {"symbols": {"tickers": symbols}, "columns": ["close", "change", "volume"]}

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
                if not sym or not isinstance(d, list) or len(d) < 3:
                    continue
                short = sym.split(":")[-1].strip().upper()
                out[short] = {
                    "close": safe_float(d[0]),
                    "change": safe_float(d[1]),
                    "volume": safe_float(d[2]),
                }
            return out
        except Exception as e:
            logger.exception("TradingView scan error: %s", e)
            time.sleep(1.0 * (attempt + 1))

    return {}


async def tv_scan_symbols(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    return await asyncio.to_thread(tv_scan_symbols_sync, symbols)


async def get_xu100_summary() -> Tuple[float, float]:
    m = await tv_scan_symbols(["BIST:XU100"])
    d = m.get("XU100", {})
    return d.get("close", float("nan")), d.get("change", float("nan"))


async def build_rows_from_is_list(is_list: List[str]) -> List[Dict[str, Any]]:
    tv_symbols = [normalize_is_ticker(t) for t in is_list if t.strip()]
    tv_map = await tv_scan_symbols(tv_symbols)

    rows: List[Dict[str, Any]] = []
    for original in is_list:
        short = normalize_is_ticker(original).split(":")[-1]
        d = tv_map.get(short, {})
        if not d:
            rows.append(
                {"ticker": short, "close": float("nan"), "change": float("nan"), "volume": float("nan"), "signal": "-", "signal_text": ""}
            )
        else:
            rows.append({"ticker": short, "close": d["close"], "change": d["change"], "volume": d["volume"], "signal": "-", "signal_text": ""})
    return rows


# -----------------------------
# Signal logic (TopN threshold)
# -----------------------------
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
        ch = r.get("change", float("nan"))
        vol = r.get("volume", float("nan"))

        if ch != ch:
            r["signal"] = "-"
            r["signal_text"] = ""
            continue

        # çok yükselen: kâr koruma
        if ch >= 4.0:
            r["signal"] = "⚠️"
            r["signal_text"] = "KÂR KORUMA"
            continue

        in_topN = (vol == vol) and (vol >= min_vol_threshold)

        # endeks kırmızı iken yeşil ayrışma
        if in_topN and (xu100_change == xu100_change) and (xu100_change <= -0.80) and (ch >= 0.40):
            r["signal"] = "🧠"
            r["signal_text"] = "AYRIŞMA"
            continue

        # toplama bölgesi: yatay/az artı + hacim
        if in_topN and (0.00 <= ch <= 0.60):
            r["signal"] = "🧠"
            r["signal_text"] = "TOPLAMA"
            continue

        # dip toplama: hafif eksi + hacim
        if in_topN and (-0.60 <= ch < 0.00):
            r["signal"] = "🧲"
            r["signal_text"] = "DİP TOPLAMA"
            continue

        r["signal"] = "-"
        r["signal_text"] = ""


# -----------------------------
# Table view (premium + wrap-safe)
# -----------------------------
def make_table(rows: List[Dict[str, Any]], title: str, include_kind: bool = False) -> str:
    if include_kind:
        header = f"{'HIS':<5} {'S':<1} {'K':<3} {'%':>5} {'FYT':>7} {'HCM':>6}"
    else:
        header = f"{'HIS':<5} {'S':<1} {'%':>5} {'FYT':>7} {'HCM':>6}"

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
            lines.append(f"{t:<5} {sig:<1} {k:<3} {ch_s:>5} {cl_s:>7} {vol_s:>6}")
        else:
            lines.append(f"{t:<5} {sig:<1} {ch_s:>5} {cl_s:>7} {vol_s:>6}")

    lines.append("</pre>")
    return "\n".join(lines)


def pick_candidates(rows: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
    cand = [r for r in rows if r.get("signal_text") == kind]
    return sorted(
        cand,
        key=lambda x: (x.get("volume") or 0) if (x.get("volume") == x.get("volume")) else 0,
        reverse=True,
    )


def signal_summary_compact(rows: List[Dict[str, Any]]) -> str:
    def join(lst: List[str]) -> str:
        return ", ".join(lst) if lst else "—"

    toplama = [r["ticker"] for r in rows if r.get("signal_text") == "TOPLAMA"]
    dip = [r["ticker"] for r in rows if r.get("signal_text") == "DİP TOPLAMA"]
    ayrisma = [r["ticker"] for r in rows if r.get("signal_text") == "AYRIŞMA"]
    kar = [r["ticker"] for r in rows if r.get("signal_text") == "KÂR KORUMA"]

    return (
        f"🧠 <b>Sinyal Özeti ({BOT_VERSION})</b>\n"
        f"• 🧠 TOPLAMA: {join(toplama)}\n"
        f"• 🧲 DİP TOPLAMA: {join(dip)}\n"
        f"• 🧠 AYRIŞMA: {join(ayrisma)}\n"
        f"• ⚠️ KÂR KORUMA: {join(kar)}"
    )


def format_threshold(min_vol: float) -> str:
    if not isinstance(min_vol, (int, float)) or math.isnan(min_vol) or min_vol == float("inf"):
        return "n/a"
    return format_volume(min_vol)


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


# -----------------------------
# Yahoo Bootstrap (1 defalık geçmiş doldurma)
# -----------------------------
def _to_yahoo_symbol_bist(ticker: str) -> str:
    t = (ticker or "").strip().upper().replace("BIST:", "")
    if not t:
        return ""
    if t.endswith(".IS"):
        return t
    return f"{t}.IS"


def yahoo_fetch_history_sync(symbol: str, days: int) -> List[Tuple[str, float, float]]:
    """
    Return list of (YYYY-MM-DD, close, volume)
    """
    sym = (symbol or "").strip()
    if not sym:
        return []

    rng = "6mo" if days > 90 else ("3mo" if days > 45 else "2mo")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
    params = {"range": rng, "interval": "1d"}

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=YAHOO_TIMEOUT)
            r.raise_for_status()
            j = r.json() or {}
            res = ((j.get("chart") or {}).get("result") or [])
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
                c = closes[i]
                v = vols[i]
                if c is None or v is None:
                    continue

                dt_local = datetime.fromtimestamp(int(ts), tz=ZoneInfo("UTC")).astimezone(TZ).date()
                day_s = dt_local.strftime("%Y-%m-%d")
                out.append((day_s, float(c), float(v)))

            if days > 0 and len(out) > days:
                out = out[-days:]
            return out
        except Exception as e:
            logger.warning("Yahoo fetch error (%s) attempt=%d: %s", sym, attempt + 1, e)
            time.sleep(0.6 * (attempt + 1))

    return []


def yahoo_bootstrap_fill_history(tickers: List[str], days: int) -> Tuple[int, int]:
    """
    Fill price_history.json and volume_history.json with historical data.
    Returns (filled_ticker_count, total_points_added)
    """
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
    """
    Run bootstrap only when files are empty/new OR forced.
    """
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
        logger.info("BOOTSTRAP başlıyor… Yahoo’dan %d gün (hisse=%d)", BOOTSTRAP_DAYS, len(tickers))

        filled, points = await asyncio.to_thread(yahoo_bootstrap_fill_history, tickers, BOOTSTRAP_DAYS)
        done = (
            f"BOOTSTRAP tamam ✅ filled={filled} • points={points} • "
            f"files={os.path.basename(PRICE_HISTORY_FILE)},{os.path.basename(VOLUME_HISTORY_FILE)}"
        )
        logger.info(done)
        return done
    except Exception as e:
        logger.exception("Bootstrap error: %s", e)
        return f"BOOTSTRAP hata: {e}"


# -----------------------------
# Tomorrow List (ERTESİ GÜNE TOPLAMA – KESİN LİSTE)
# -----------------------------
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

    vol_term = 0.0
    if vol == vol and vol > 0:
        vol_term = math.log10(vol + 1.0) * 10.0

    band_term = max(0.0, (70.0 - float(band)))
    return vol_term + band_term + kind_bonus


def _tomorrow_thresholds_for(st: Dict[str, Any]) -> Tuple[float, float, bool]:
    """
    Returns (min_vol_ratio, max_band, torpil_used)
    """
    if not TORPIL_ENABLED or not st:
        return (TOMORROW_MIN_VOL_RATIO, TOMORROW_MAX_BAND, False)

    samples = min(int(st.get("samples_close", 0)), int(st.get("samples_vol", 0)))
    if samples < TORPIL_MIN_SAMPLES:
        return (TORPIL_MIN_VOL_RATIO, TORPIL_MAX_BAND, True)

    return (TOMORROW_MIN_VOL_RATIO, TOMORROW_MAX_BAND, False)


def build_tomorrow_rows(all_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in all_rows:
        kind = r.get("signal_text", "")
        if kind not in ("TOPLAMA", "DİP TOPLAMA") and not (TOMORROW_INCLUDE_AYRISMA and kind == "AYRIŞMA"):
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

        if ratio != ratio or ratio < min_ratio:
            continue
        if band > max_band:
            continue

        out.append(r)

    out.sort(key=tomorrow_score, reverse=True)
    return out[: max(1, TOMORROW_MAX)]


def build_tomorrow_message(rows: List[Dict[str, Any]], xu_close: float, xu_change: float, thresh_s: str) -> str:
    now_s = now_tr().strftime("%H:%M")
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    tomorrow = (now_tr().date() + timedelta(days=1)).strftime("%Y-%m-%d")

    torpil_used_any = False
    for r in rows:
        st = compute_30d_stats(r.get("ticker", ""))
        if st:
            _, _, used = _tomorrow_thresholds_for(st)
            if used:
                torpil_used_any = True
                break

    head = (
        f"🌙 <b>ERTESİ GÜNE TOPLAMA – KESİN LİSTE</b> • <b>{tomorrow}</b>\n"
        f"🕒 Hazırlandı: <b>{now_s}</b> • <b>{BOT_VERSION}</b>\n"
        f"📊 <b>XU100</b>: {xu_close_s} • {xu_change_s}\n"
        f"🧱 <b>Top{VOLUME_TOP_N} Eşik</b>: ≥ <b>{thresh_s}</b>\n"
        f"🎯 Filtre (PRO): Band ≤ <b>%{TOMORROW_MAX_BAND:.0f}</b> • Hacim ≥ <b>{TOMORROW_MIN_VOL_RATIO:.2f}x</b>\n"
    )

    if torpil_used_any:
        head += "🧩 <i>Torpil Modu: veri az olan hisselerde geçici yumuşatma aktif.</i>\n"

    if not rows:
        return head + "\n❌ <b>Bugün kriterlere uyan “kesin liste” çıkmadı.</b>\n<i>Disk doldukça ve gün sayısı arttıkça sistem daha keskinleşir.</i>"

    table = make_table(rows, "✅ <b>ALTIN LİSTE (Tomorrow Candidates)</b>", include_kind=True)

    notes_lines = ["\n📌 <b>30G Notlar</b>"]
    for r in rows[: min(len(rows), ALARM_NOTE_MAX)]:
        t = r.get("ticker", "")
        cl = r.get("close", float("nan"))
        notes_lines.append(format_30d_note(t, cl))
    notes = "\n".join(notes_lines)

    foot = (
        "\n\n🟢 <b>Sabah Planı (Pratik)</b>\n"
        "• Açılışta ilk 5–15 dk “sakin+yeşil” teyidi\n"
        "• +%2–%4 kademeli çıkış (hızlı kâr)\n"
        "• Ters mum gelirse: disiplin, zarar büyütme yok"
    )

    return head + "\n" + table + "\n" + notes + foot


# -----------------------------
# Premium Alarm message (+30G note)
# -----------------------------
def build_alarm_message(
    alarm_rows: List[Dict[str, Any]],
    watch_rows: List[Dict[str, Any]],
    xu_close: float,
    xu_change: float,
    thresh_s: str,
    top_n: int,
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
        f"🧱 <b>Top{top_n} Eşik</b>: ≥ <b>{thresh_s}</b>\n"
        f"🎯 <b>Tetiklenen</b>: {trig_s}\n"
    )

    alarm_table = make_table(alarm_rows, "🔥 <b>ALARM RADAR (TOP/DIP)</b>", include_kind=True)

    notes_lines = ["\n📌 <b>30G Notlar (Disk Arşivi)</b>"]
    for r in alarm_rows[: max(1, ALARM_NOTE_MAX)]:
        t = r.get("ticker", "")
        cl = r.get("close", float("nan"))
        if not t:
            continue
        notes_lines.append(format_30d_note(t, cl))
    notes = "\n".join(notes_lines)

    foot = f"\n⏳ <i>Aynı hisse için {ALARM_COOLDOWN_MIN} dk cooldown aktif.</i>"

    if watch_rows:
        watch_table = make_table(watch_rows, "👀 <b>WATCHLIST (Alarm Eki)</b>", include_kind=True)
        return head + "\n" + alarm_table + "\n" + notes + "\n\n" + watch_table + foot

    return head + "\n" + alarm_table + "\n" + notes + foot


# -----------------------------
# Alarm logic
# -----------------------------
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
        reverse=True,
    )
    return out


# -----------------------------
# Telegram Handlers
# -----------------------------
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
        f"🤖 <b>TAIPO PRO INTEL</b> • <b>{BOT_VERSION}</b>\n\n"
        "✅ <b>Komutlar</b>\n"
        "• /ping → bot çalışıyor mu?\n"
        "• /version → sürüm\n"
        "• /chatid → chat id\n"
        "• /watch → watchlist radar (örn: /watch AKBNK,CANTE)\n"
        "• /radar → BIST200 radar parça (örn: /radar 1)\n"
        "• /eod → manuel EOD raporu\n"
        "• /tomorrow → ertesi güne kesin toplama listesi\n"
        "• /alarm → alarm durumu/ayarlar\n"
        "• /stats → 30G istatistik (örn: /stats AKBNK)\n"
        "• /bootstrap → Yahoo’dan geçmiş doldurma (1 defa)\n\n"
        "📌 '/' yazınca komutların görünmesi için menü otomatik güncellenir."
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def cmd_version(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"🧩 Version: <b>{BOT_VERSION}</b>", parse_mode=ParseMode.HTML)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"🏓 Pong! ({BOT_VERSION})")


async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = update.effective_chat.id
    await update.message.reply_text(f"🧾 Chat ID: <code>{cid}</code>", parse_mode=ParseMode.HTML)


async def cmd_alarm_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
        f"🚨 <b>Alarm Durumu</b>\n"
        f"• Enabled: <b>{'ON' if ALARM_ENABLED else 'OFF'}</b>\n"
        f"• Interval: <b>{ALARM_INTERVAL_MIN} dk</b>\n"
        f"• Cooldown: <b>{ALARM_COOLDOWN_MIN} dk</b>\n"
        f"• ChatID env: <code>{ALARM_CHAT_ID or 'YOK'}</code>\n"
        f"• ChatID parsed: <code>{ALARM_CHAT_ID_INT if ALARM_CHAT_ID_INT is not None else 'HATALI'}</code>\n"
        f"• Tarama: <b>{ALARM_START_HOUR:02d}:{ALARM_START_MIN:02d}–{ALARM_END_HOUR:02d}:{ALARM_END_MIN:02d}</b>\n"
        f"• EOD: <b>{EOD_HOUR:02d}:{EOD_MINUTE:02d}</b>\n"
        f"• TZ: <b>{TZ.key}</b>\n"
        f"• WATCHLIST_MAX: <b>{WATCHLIST_MAX}</b>\n"
        f"• VOLUME_TOP_N: <b>{VOLUME_TOP_N}</b>\n"
        f"• DATA_DIR: <code>{EFFECTIVE_DATA_DIR}</code>\n"
        f"• HISTORY_DAYS: <b>{HISTORY_DAYS}</b>\n"
        f"• FILES: <code>{os.path.basename(PRICE_HISTORY_FILE)}</code>, <code>{os.path.basename(VOLUME_HISTORY_FILE)}</code>\n"
        f"• TOMORROW_MAX: <b>{TOMORROW_MAX}</b> | MIN_VOL_RATIO: <b>{TOMORROW_MIN_VOL_RATIO:.2f}x</b> | MAX_BAND: <b>%{TOMORROW_MAX_BAND:.0f}</b>\n"
        f"• TORPIL: <b>{'ON' if TORPIL_ENABLED else 'OFF'}</b> (min sample < {TORPIL_MIN_SAMPLES})\n"
        f"• BOOTSTRAP_ON_START: <b>{'1' if BOOTSTRAP_ON_START else '0'}</b> | BOOTSTRAP_DAYS: <b>{BOOTSTRAP_DAYS}</b>"
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
        await update.message.reply_text(f"❌ <b>{t}</b> için 30G veri yok (disk yeni olabilir).", parse_mode=ParseMode.HTML)
        return

    ratio = st["ratio"]
    ratio_s = "n/a" if (ratio != ratio) else f"{ratio:.2f}x"

    msg = (
        f"📌 <b>{t}</b> • <b>30G İstatistik</b>\n"
        f"• Close min/avg/max: <b>{st['min']:.2f}</b> / <b>{st['avg_close']:.2f}</b> / <b>{st['max']:.2f}</b>\n"
        f"• 30G Ort. Hacim: <b>{format_volume(st['avg_vol'])}</b>\n"
        f"• Bugün Hacim: <b>{format_volume(st['today_vol'])}</b>\n"
        f"• Bugün / Ortalama: <b>{ratio_s}</b>\n"
        f"• Band: <b>%{st['band_pct']:.0f}</b>\n"
        f"• Key (trading-day): <code>{today_key_tradingday()}</code>\n"
        f"• Dosyalar: <code>{PRICE_HISTORY_FILE}</code> & <code>{VOLUME_HISTORY_FILE}</code>"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def cmd_bootstrap(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /bootstrap [60]
    """
    days = BOOTSTRAP_DAYS
    if context.args:
        try:
            days = int(re.sub(r"\D+", "", context.args[0]))
        except Exception:
            days = BOOTSTRAP_DAYS
    days = max(20, min(180, days))

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    await update.message.reply_text(f"⏳ Bootstrap başlıyor… Yahoo’dan {days} gün çekiyorum (1 defalık).")

    tickers = [normalize_is_ticker(x).split(":")[-1] for x in bist200_list if x.strip()]
    filled, points = await asyncio.to_thread(yahoo_bootstrap_fill_history, tickers, days)

    await update.message.reply_text(
        "✅ Bootstrap tamam!\n"
        f"• Dolu hisse: <b>{filled}</b>\n"
        f"• Nokta: <b>{points}</b>\n"
        f"• Disk: <code>{EFFECTIVE_DATA_DIR}</code>\n"
        f"• Files: <code>{os.path.basename(PRICE_HISTORY_FILE)}</code>, <code>{os.path.basename(VOLUME_HISTORY_FILE)}</code>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    await update.message.reply_text("⏳ Ertesi gün listesi hazırlanıyor...")

    xu_close, xu_change = await get_xu100_summary()
    rows = await build_rows_from_is_list(bist200_list)

    update_history_from_rows(rows)

    min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
    thresh_s = format_threshold(min_vol)

    tom_rows = build_tomorrow_rows(rows)
    msg = build_tomorrow_message(tom_rows, xu_close, xu_change, thresh_s)

    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    xu_close, xu_change = await get_xu100_summary()
    rows = await build_rows_from_is_list(bist200_list)

    update_history_from_rows(rows)

    min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
    thresh_s = format_threshold(min_vol)

    first20 = rows[:20]
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]

    toplama_cand = pick_candidates(rows, "TOPLAMA")
    dip_cand = pick_candidates(rows, "DİP TOPLAMA")

    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    await update.message.reply_text(
        f"🧱 <b>Kriter</b>: Top{VOLUME_TOP_N} hacim eşiği ≥ <b>{thresh_s}</b>\n"
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}\n"
        f"🗓️ <b>Key</b> (trading-day): <code>{today_key_tradingday()}</code>\n"
        f"💾 <b>Disk</b>: <code>{EFFECTIVE_DATA_DIR}</code>\n"
        f"📁 <i>price_history.json + volume_history.json güncellendi</i>",
        parse_mode=ParseMode.HTML,
    )

    await update.message.reply_text(make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>", include_kind=True), parse_mode=ParseMode.HTML)

    await update.message.reply_text(
        make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>", include_kind=True) if top10_vol else "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>\n—",
        parse_mode=ParseMode.HTML,
    )

    await update.message.reply_text(
        make_table(toplama_cand, "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>", include_kind=True) if toplama_cand else "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>\n—",
        parse_mode=ParseMode.HTML,
    )

    await update.message.reply_text(
        make_table(dip_cand, "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>", include_kind=True) if dip_cand else "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>\n—",
        parse_mode=ParseMode.HTML,
    )

    await update.message.reply_text(signal_summary_compact(rows), parse_mode=ParseMode.HTML)


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    n = 1
    if context.args:
        try:
            n = int(re.sub(r"\D+", "", context.args[0]))
        except Exception:
            n = 1
    if n < 1:
        n = 1

    chunks = chunk_list(bist200_list, 20)
    total_parts = len(chunks)
    if n > total_parts:
        await update.message.reply_text(f"❌ /radar 1–{total_parts} arası. (Sen: {n})")
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    part_list = chunks[n - 1]
    xu_close, xu_change = await get_xu100_summary()

    rows = await build_rows_from_is_list(part_list)

    all_rows = await build_rows_from_is_list(bist200_list)
    min_vol = compute_signal_rows(all_rows, xu_change, VOLUME_TOP_N)
    _apply_signals_with_threshold(rows, xu_change, min_vol)

    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}"
    await update.message.reply_text(make_table(rows, title, include_kind=True), parse_mode=ParseMode.HTML)


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    arg_list = parse_watch_args(context.args or [])
    watch = arg_list if arg_list else env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")

    if not watch:
        await update.message.reply_text(
            "❌ WATCHLIST env boş.\nÖrnek: WATCHLIST=AKBNK,CANTE,EREGL\n(Alternatif: WATCHLIST_BIST=AKBNK,CANTE,EREGL)\n\n"
            "Veya: /watch AKBNK,CANTE,EREGL",
            parse_mode=ParseMode.HTML,
        )
        return

    watch = watch[:WATCHLIST_MAX]
    await update.message.reply_text("⏳ Veriler çekiliyor...")

    xu_close, xu_change = await get_xu100_summary()
    rows = await build_rows_from_is_list(watch)

    bist200_list = env_csv("BIST200_TICKERS")
    if bist200_list:
        all_rows = await build_rows_from_is_list(bist200_list)
        min_vol = compute_signal_rows(all_rows, xu_change, VOLUME_TOP_N)
        _apply_signals_with_threshold(rows, xu_change, min_vol)
        thresh_s = format_threshold(min_vol)
    else:
        min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
        thresh_s = format_threshold(min_vol)

    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    await update.message.reply_text(
        f"👀 <b>WATCHLIST</b> (Top{VOLUME_TOP_N} Eşik ≥ <b>{thresh_s}</b>)\n"
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}",
        parse_mode=ParseMode.HTML,
    )
    await update.message.reply_text(make_table(rows, "📌 <b>Watchlist Radar</b>", include_kind=True), parse_mode=ParseMode.HTML)


# -----------------------------
# Scheduled jobs
# -----------------------------
async def job_alarm_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not ALARM_ENABLED or not ALARM_CHAT_ID_INT:
        return
    if not within_alarm_window(now_tr()):
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return

    try:
        xu_close, xu_change = await get_xu100_summary()
        all_rows = await build_rows_from_is_list(bist200_list)

        update_history_from_rows(all_rows)

        min_vol = compute_signal_rows(all_rows, xu_change, VOLUME_TOP_N)
        thresh_s = format_threshold(min_vol)

        alarm_rows = filter_new_alarms(all_rows)
        if not alarm_rows:
            return

        ts_now = time.time()
        for r in alarm_rows:
            mark_alarm_sent(r.get("ticker", ""), ts_now)

        watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")
        watch = (watch or [])[:WATCHLIST_MAX]
        w_rows = await build_rows_from_is_list(watch) if watch else []
        if w_rows:
            _apply_signals_with_threshold(w_rows, xu_change, min_vol)

        text = build_alarm_message(
            alarm_rows=alarm_rows,
            watch_rows=w_rows,
            xu_close=xu_close,
            xu_change=xu_change,
            thresh_s=thresh_s,
            top_n=VOLUME_TOP_N,
        )

        await context.bot.send_message(
            chat_id=ALARM_CHAT_ID_INT,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.exception("Alarm job error: %s", e)


async def job_tomorrow_list(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not ALARM_ENABLED or not ALARM_CHAT_ID_INT:
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return

    try:
        xu_close, xu_change = await get_xu100_summary()
        rows = await build_rows_from_is_list(bist200_list)

        update_history_from_rows(rows)

        min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
        thresh_s = format_threshold(min_vol)

        tom_rows = build_tomorrow_rows(rows)
        msg = build_tomorrow_message(tom_rows, xu_close, xu_change, thresh_s)

        await context.bot.send_message(
            chat_id=ALARM_CHAT_ID_INT,
            text=msg,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.exception("Tomorrow job error: %s", e)


async def job_eod_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not ALARM_ENABLED or not ALARM_CHAT_ID_INT:
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return

    try:
        xu_close, xu_change = await get_xu100_summary()
        rows = await build_rows_from_is_list(bist200_list)

        update_history_from_rows(rows)

        min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
        thresh_s = format_threshold(min_vol)

        first20 = rows[:20]
        rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
        top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]
        toplama_cand = pick_candidates(rows, "TOPLAMA")
        dip_cand = pick_candidates(rows, "DİP TOPLAMA")

        xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
        xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

        header = (
            f"📌 <b>EOD RAPOR</b> • <b>{BOT_VERSION}</b>\n"
            f"🕒 {now_tr().strftime('%H:%M')}  |  🧱 Top{VOLUME_TOP_N} Eşik ≥ <b>{thresh_s}</b>\n"
            f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}\n"
            f"🗓️ <b>Key</b> (trading-day): <code>{today_key_tradingday()}</code>\n"
            f"💾 <b>Disk</b>: <code>{EFFECTIVE_DATA_DIR}</code>\n"
            f"📁 <i>price_history.json + volume_history.json güncellendi</i>"
        )

        parts = [
            header,
            make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>", include_kind=True),
            make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>", include_kind=True) if top10_vol else "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>\n—",
            make_table(toplama_cand, "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>", include_kind=True) if toplama_cand else "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>\n—",
            make_table(dip_cand, "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>", include_kind=True) if dip_cand else "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>\n—",
            signal_summary_compact(rows),
        ]

        buf = ""
        for p in parts:
            chunk = p + "\n\n"
            if len(buf) + len(chunk) > 3500:
                await context.bot.send_message(chat_id=ALARM_CHAT_ID_INT, text=buf.strip(), parse_mode=ParseMode.HTML)
                buf = ""
            buf += chunk
        if buf.strip():
            await context.bot.send_message(chat_id=ALARM_CHAT_ID_INT, text=buf.strip(), parse_mode=ParseMode.HTML)

        watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")
        watch = (watch or [])[:WATCHLIST_MAX]
        if watch:
            w_rows = await build_rows_from_is_list(watch)
            _apply_signals_with_threshold(w_rows, xu_change, min_vol)
            await context.bot.send_message(
                chat_id=ALARM_CHAT_ID_INT,
                text=make_table(w_rows, "👀 <b>WATCHLIST (EOD Eki)</b>", include_kind=True),
                parse_mode=ParseMode.HTML,
            )
    except Exception as e:
        logger.exception("EOD job error: %s", e)


def schedule_jobs(app: Application) -> None:
    jq = getattr(app, "job_queue", None)
    if jq is None:
        logger.warning("JobQueue yok. requirements.txt: python-telegram-bot[job-queue]==22.5 olmalı.")
        return

    if not ALARM_ENABLED:
        logger.info("Alarm disabled by env.")
        return

    if not ALARM_CHAT_ID_INT:
        logger.info("ALARM_CHAT_ID env yok / hatalı. Alarm/EOD gönderilmeyecek.")
        return

    first = next_aligned_run(ALARM_INTERVAL_MIN)
    jq.run_repeating(
        job_alarm_scan,
        interval=ALARM_INTERVAL_MIN * 60,
        first=first,
        name="alarm_scan_repeating",
    )
    logger.info("Alarm scan scheduled every %d min. First=%s", ALARM_INTERVAL_MIN, first.isoformat())

    jq.run_daily(
        job_eod_report,
        time=datetime(2000, 1, 1, EOD_HOUR, EOD_MINUTE, tzinfo=TZ).timetz(),
        name="eod_daily",
    )
    logger.info("EOD scheduled daily at %02d:%02d", EOD_HOUR, EOD_MINUTE)

    base = datetime(2000, 1, 1, EOD_HOUR, EOD_MINUTE, tzinfo=TZ) + timedelta(minutes=TOMORROW_DELAY_MIN)
    jq.run_daily(
        job_tomorrow_list,
        time=base.timetz(),
        name="tomorrow_daily",
    )
    logger.info("Tomorrow list scheduled daily at %02d:%02d (+%d min)", base.hour, base.minute, TOMORROW_DELAY_MIN)


# -----------------------------
# Main
# -----------------------------
async def post_init(application: Application) -> None:
    """
    PTB post_init: polling başladıktan sonra bootstrap gibi işleri başlatmak için ideal.
    """
    try:
        msg = await yahoo_bootstrap_if_needed()
        logger.info("Post-init: %s", msg)
    except Exception as e:
        logger.exception("Post-init error: %s", e)


def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env missing")

    app = Application.builder().token(token).post_init(post_init).build()

    # Commands
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("version", cmd_version))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("alarm", cmd_alarm_status))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("tomorrow", cmd_tomorrow))
    app.add_handler(CommandHandler("bootstrap", cmd_bootstrap))

    # Schedule jobs
    schedule_jobs(app)

    logger.info(
        "Bot starting... version=%s enabled_alarm=%s chat_id=%s data_dir=%s files=%s,%s",
        BOT_VERSION,
        ALARM_ENABLED,
        ALARM_CHAT_ID_INT if ALARM_CHAT_ID_INT is not None else "None",
        EFFECTIVE_DATA_DIR,
        os.path.basename(PRICE_HISTORY_FILE),
        os.path.basename(VOLUME_HISTORY_FILE),
    )

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
