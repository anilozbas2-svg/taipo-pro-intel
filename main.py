import os
import re
import math
import time
import logging
import asyncio
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Tuple

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# -----------------------------
# Config
# -----------------------------
BOT_VERSION = os.getenv("BOT_VERSION", "v1.3.7-premium-topN").strip() or "v1.3.7-premium-topN"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12
TZ = ZoneInfo(os.getenv("TZ", "Europe/Istanbul"))

# Alarm config
ALARM_ENABLED = os.getenv("ALARM_ENABLED", "1").strip() == "1"           # 1/0
ALARM_CHAT_ID = os.getenv("ALARM_CHAT_ID", "").strip()                   # group chat id ex: -100...
ALARM_INTERVAL_MIN = int(os.getenv("ALARM_INTERVAL_MIN", "30"))          # 30 dk
ALARM_COOLDOWN_MIN = int(os.getenv("ALARM_COOLDOWN_MIN", "60"))          # aynı hisse 60 dk içinde tekrar yok

# Tarama saat aralığı (default: 10:00 - 17:30)
ALARM_START_HOUR = int(os.getenv("ALARM_START_HOUR", "10"))
ALARM_START_MIN = int(os.getenv("ALARM_START_MIN", "0"))
ALARM_END_HOUR = int(os.getenv("ALARM_END_HOUR", "17"))
ALARM_END_MIN = int(os.getenv("ALARM_END_MIN", "30"))

# EOD
EOD_HOUR = int(os.getenv("EOD_HOUR", "17"))
EOD_MINUTE = int(os.getenv("EOD_MINUTE", "50"))

# Watchlist
WATCHLIST_MAX = int(os.getenv("WATCHLIST_MAX", "12"))

# ✅ TopN hacim eşiği (Top10 yerine Top50 default)
VOLUME_TOP_N = int(os.getenv("VOLUME_TOP_N", "50"))  # 50 önerdiğin hedef

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

def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")

def format_volume(v: Any) -> str:
    """
    Mobil wrap engellemek için K/M/B kısa format.
    Not: uzun gelirse tablo kayıyor, bu yüzden kompakt.
    """
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
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def now_tr() -> datetime:
    return datetime.now(tz=TZ)

def next_aligned_run(minutes: int) -> datetime:
    """Bir sonraki 00/30 gibi dakikaya hizalar."""
    n = now_tr()
    m = n.minute
    step = max(1, int(minutes))
    next_m = ((m // step) + 1) * step
    if next_m >= 60:
        nn = (n.replace(second=0, microsecond=0, minute=0) + timedelta(hours=1))
        return nn
    return n.replace(second=0, microsecond=0, minute=next_m)

def within_alarm_window(dt: datetime) -> bool:
    """Saat aralığı kontrolü (10:00–17:30 default)."""
    start = dtime(ALARM_START_HOUR, ALARM_START_MIN)
    end = dtime(ALARM_END_HOUR, ALARM_END_MIN)
    t = dt.timetz().replace(tzinfo=None)
    return start <= t <= end

def st_short(sig_text: str) -> str:
    # tablo sığsın diye kısa kod
    if sig_text == "TOPLAMA":
        return "TOP"
    if sig_text == "DİP TOPLAMA":
        return "DIP"
    if sig_text == "AYRIŞMA":
        return "AYR"
    if sig_text == "KÂR KORUMA":
        return "KAR"
    return ""

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
            rows.append({"ticker": short, "close": float("nan"), "change": float("nan"), "volume": float("nan"), "signal": "-", "signal_text": ""})
        else:
            rows.append({"ticker": short, "close": d["close"], "change": d["change"], "volume": d["volume"], "signal": "-", "signal_text": ""})
    return rows

# -----------------------------
# Signal logic (TopN threshold) ✅
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

        if ch >= 4.0:
            r["signal"] = "⚠️"
            r["signal_text"] = "KÂR KORUMA"
            continue

        in_topN = (vol == vol) and (vol >= min_vol_threshold)

        # AYRIŞMA
        if in_topN and (xu100_change == xu100_change) and (xu100_change <= -0.80) and (ch >= 0.40):
            r["signal"] = "🧠"
            r["signal_text"] = "AYRIŞMA"
            continue

        # TOPLAMA
        if in_topN and (0.00 <= ch <= 0.60):
            r["signal"] = "🧠"
            r["signal_text"] = "TOPLAMA"
            continue

        # DİP TOPLAMA
        if in_topN and (-0.60 <= ch < 0.00):
            r["signal"] = "🧲"
            r["signal_text"] = "DİP TOPLAMA"
            continue

        r["signal"] = "-"
        r["signal_text"] = ""

# -----------------------------
# Table view (premium + wrap-safe) ✅
# -----------------------------
def make_table(rows: List[Dict[str, Any]], title: str, include_kind: bool = False) -> str:
    """
    Mobil wrap önlemek için dar tablo.
    include_kind=True => K sütunu (TOP/DIP/AYR/KAR)
    """
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

        vol_s = format_volume(vol)
        vol_s = vol_s[:6]  # aşırı uzarsa kırp

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
        reverse=True
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
    """
    /watch AKBNK,CANTE,EREGL
    /watch AKBNK CANTE EREGL
    """
    if not args:
        return []
    joined = " ".join(args).strip()
    joined = joined.replace(";", ",")
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
# Premium Alarm message ✅
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

    if watch_rows:
        watch_table = make_table(watch_rows, "👀 <b>WATCHLIST (Alarm Eki)</b>", include_kind=True)
        foot = f"\n⏳ <i>Aynı hisse için {ALARM_COOLDOWN_MIN} dk cooldown aktif.</i>"
        return head + "\n" + alarm_table + "\n\n" + watch_table + foot

    foot = f"\n⏳ <i>Aynı hisse için {ALARM_COOLDOWN_MIN} dk cooldown aktif.</i>"
    return head + "\n" + alarm_table + foot

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
    """
    Alarm sadece TOPLAMA + DİP TOPLAMA.
    Aynı hisse cooldown.
    """
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

# -----------------------------
# Telegram Handlers
# -----------------------------
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
        f"• Tarama: <b>{ALARM_START_HOUR:02d}:{ALARM_START_MIN:02d}–{ALARM_END_HOUR:02d}:{ALARM_END_MIN:02d}</b>\n"
        f"• EOD: <b>{EOD_HOUR:02d}:{EOD_MINUTE:02d}</b>\n"
        f"• TZ: <b>{TZ.key}</b>\n"
        f"• WATCHLIST_MAX: <b>{WATCHLIST_MAX}</b>\n"
        f"• VOLUME_TOP_N: <b>{VOLUME_TOP_N}</b>"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    xu_close, xu_change = await get_xu100_summary()
    rows = await build_rows_from_is_list(bist200_list)
    min_vol = compute_signal_rows(rows, xu_change, VOLUME_TOP_N)
    thresh_s = format_threshold(min_vol)

    first20 = rows[:20]

    # (Bilgi amaçlı) Top10 hacim listesi de kalsın istiyorsun diye bırakıyorum.
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]

    toplama_cand = pick_candidates(rows, "TOPLAMA")
    dip_cand = pick_candidates(rows, "DİP TOPLAMA")

    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    await update.message.reply_text(
        f"🧱 <b>Kriter</b>: Top{VOLUME_TOP_N} hacim eşiği ≥ <b>{thresh_s}</b>\n"
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}",
        parse_mode=ParseMode.HTML
    )

    await update.message.reply_text(make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>", include_kind=True), parse_mode=ParseMode.HTML)

    await update.message.reply_text(
        make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>", include_kind=True) if top10_vol
        else "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>\n—",
        parse_mode=ParseMode.HTML
    )

    await update.message.reply_text(
        make_table(toplama_cand, "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>", include_kind=True) if toplama_cand
        else "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>\n—",
        parse_mode=ParseMode.HTML
    )

    await update.message.reply_text(
        make_table(dip_cand, "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>", include_kind=True) if dip_cand
        else "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>\n—",
        parse_mode=ParseMode.HTML
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

    # Threshold'ı BIST200 üzerinden alıp stabil uygula
    all_rows = await build_rows_from_is_list(bist200_list)
    min_vol = compute_signal_rows(all_rows, xu_change, VOLUME_TOP_N)
    _apply_signals_with_threshold(rows, xu_change, min_vol)

    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}"
    await update.message.reply_text(make_table(rows, title, include_kind=True), parse_mode=ParseMode.HTML)

async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    arg_list = parse_watch_args(context.args or [])
    if arg_list:
        watch = arg_list
    else:
        watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")

    if not watch:
        await update.message.reply_text(
            "❌ WATCHLIST env boş.\nÖrnek: WATCHLIST=AKBNK,CANTE,EREGL\n(Alternatif: WATCHLIST_BIST=AKBNK,CANTE,EREGL)\n\n"
            "Veya: /watch AKBNK,CANTE,EREGL",
            parse_mode=ParseMode.HTML
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
        parse_mode=ParseMode.HTML
    )
    await update.message.reply_text(make_table(rows, "📌 <b>Watchlist Radar</b>", include_kind=True), parse_mode=ParseMode.HTML)

# -----------------------------
# Scheduled jobs
# -----------------------------
async def job_alarm_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    30 dk’da bir:
    - BIST200 tarar
    - Sadece TOPLAMA / DİP TOPLAMA alarm üretir
    - Premium tek mesaj: Alarm tablosu + Watchlist tablosu
    - Alarm sadece ALARM_CHAT_ID (grup) gider ✅
    - Saat aralığı: 10:00–17:30 (env ile değişir)
    """
    if not ALARM_ENABLED or not ALARM_CHAT_ID:
        return

    if not within_alarm_window(now_tr()):
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return

    try:
        xu_close, xu_change = await get_xu100_summary()

        all_rows = await build_rows_from_is_list(bist200_list)
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
            chat_id=int(ALARM_CHAT_ID),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.exception("Alarm job error: %s", e)

async def job_eod_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Her gün 17:50: EOD raporu (ALARM_CHAT_ID'e gider)"""
    if not ALARM_ENABLED or not ALARM_CHAT_ID:
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return

    try:
        xu_close, xu_change = await get_xu100_summary()
        rows = await build_rows_from_is_list(bist200_list)
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
            f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}"
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
            chunk = (p + "\n\n")
            if len(buf) + len(chunk) > 3500:
                await context.bot.send_message(chat_id=int(ALARM_CHAT_ID), text=buf.strip(), parse_mode=ParseMode.HTML)
                buf = ""
            buf += chunk
        if buf.strip():
            await context.bot.send_message(chat_id=int(ALARM_CHAT_ID), text=buf.strip(), parse_mode=ParseMode.HTML)

        watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")
        watch = (watch or [])[:WATCHLIST_MAX]
        if watch:
            w_rows = await build_rows_from_is_list(watch)
            _apply_signals_with_threshold(w_rows, xu_change, min_vol)
            await context.bot.send_message(
                chat_id=int(ALARM_CHAT_ID),
                text=make_table(w_rows, "👀 <b>WATCHLIST (EOD Eki)</b>", include_kind=True),
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.exception("EOD job error: %s", e)

def schedule_jobs(app: Application) -> None:
    """
    Alarm: 30 dk’da bir (hizalı)
    EOD: 17:50
    """
    jq = getattr(app, "job_queue", None)
    if jq is None:
        # En yaygın sebep: requirements'ta job-queue extras yok
        logger.warning("JobQueue yok. requirements.txt: python-telegram-bot[job-queue]==22.5 olmalı.")
        return

    if not ALARM_ENABLED:
        logger.info("Alarm disabled by env.")
        return

    if not ALARM_CHAT_ID:
        logger.info("ALARM_CHAT_ID env yok. Alarm/EOD gönderilmeyecek.")
        return

    first = next_aligned_run(ALARM_INTERVAL_MIN)
    jq.run_repeating(
        job_alarm_scan,
        interval=ALARM_INTERVAL_MIN * 60,
        first=first,
        name="alarm_scan_repeating"
    )
    logger.info("Alarm scan scheduled every %d min. First=%s", ALARM_INTERVAL_MIN, first.isoformat())

    jq.run_daily(
        job_eod_report,
        time=datetime(2000, 1, 1, EOD_HOUR, EOD_MINUTE, tzinfo=TZ).timetz(),
        name="eod_daily"
    )
    logger.info("EOD scheduled daily at %02d:%02d", EOD_HOUR, EOD_MINUTE)

# -----------------------------
# Main
# -----------------------------
def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env missing")

    app = Application.builder().token(token).build()

    # Commands
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("alarm", cmd_alarm_status))

    # Schedule jobs
    schedule_jobs(app)

    logger.info("Bot starting... version=%s", BOT_VERSION)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
