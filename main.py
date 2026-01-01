import os
import re
import math
import time
import logging
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Tuple, Optional

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# -----------------------------
# Config
# -----------------------------
BOT_VERSION = os.getenv("BOT_VERSION", "v1.3.5-hybrid").strip() or "v1.3.5-hybrid"

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
ALARM_ENABLED = os.getenv("ALARM_ENABLED", "1").strip() == "1"  # 1/0
ALARM_CHAT_ID = os.getenv("ALARM_CHAT_ID", "").strip()          # Telegram chat id (string)
ALARM_INTERVAL_MIN = int(os.getenv("ALARM_INTERVAL_MIN", "30")) # 30 dk
ALARM_COOLDOWN_MIN = int(os.getenv("ALARM_COOLDOWN_MIN", "60")) # aynı hisse 60 dk içinde tekrar yok
EOD_HOUR = int(os.getenv("EOD_HOUR", "17"))
EOD_MINUTE = int(os.getenv("EOD_MINUTE", "50"))

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

# ✅ HACİM KISA FORMAT (wrap engeller)
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
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def now_tr() -> datetime:
    return datetime.now(tz=TZ)

def next_aligned_run(minutes: int) -> datetime:
    """Bir sonraki (00/30 gibi) dakikaya hizalar."""
    n = now_tr()
    # dakika hizalama
    m = n.minute
    step = minutes
    # bir sonraki step'e yuvarla
    next_m = ((m // step) + 1) * step
    if next_m >= 60:
        nn = (n.replace(second=0, microsecond=0, minute=0) + timedelta(hours=1))
        return nn
    return n.replace(second=0, microsecond=0, minute=next_m)

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
# 3'lü sistem (stabil) - Hybrid
# -----------------------------
def compute_signal_rows(rows: List[Dict[str, Any]], xu100_change: float) -> float:
    """
    Hybrid:
    - Top10 hacim eşiğini referans alır (Top10’un 10. sırası)
    - TOPLAMA: Top10 hacimde olup 0.00 ile +0.60 arası -> 🧠
    - DİP TOPLAMA: Top10 hacimde olup -0.60 ile -0.01 arası -> 🧲
    - AYRIŞMA: Endeks sert düşüşte (<= -0.80) iken hisse +0.40 ve üstü + Top10 hacim -> 🧠
    - KÂR KORUMA: hisse >= +4.00 -> ⚠️
    Returns: top10_min_vol (float)
    """
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10 = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]
    top10_min_vol = top10[-1]["volume"] if len(top10) == 10 else (top10[-1]["volume"] if top10 else float("inf"))

    _apply_signals_with_threshold(rows, xu100_change, top10_min_vol)
    return float(top10_min_vol)

def _apply_signals_with_threshold(rows: List[Dict[str, Any]], xu100_change: float, top10_min_vol: float) -> None:
    """BIST200 top10 eşiğiyle sinyal üretir (Watchlist/Alarm için stabil)."""
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

        in_top10 = (vol == vol) and (vol >= top10_min_vol)

        if in_top10 and (xu100_change == xu100_change) and (xu100_change <= -0.80) and (ch >= 0.40):
            r["signal"] = "🧠"
            r["signal_text"] = "AYRIŞMA"
            continue

        if in_top10 and (0.00 <= ch <= 0.60):
            r["signal"] = "🧠"
            r["signal_text"] = "TOPLAMA"
            continue

        if in_top10 and (-0.60 <= ch < 0.00):
            r["signal"] = "🧲"
            r["signal_text"] = "DİP TOPLAMA"
            continue

        r["signal"] = "-"
        r["signal_text"] = ""

# -----------------------------
# Table view (compact, wrap-safe)
# -----------------------------
def make_table(rows: List[Dict[str, Any]], title: str, include_note: bool = False) -> str:
    header = f"{'HİSSE':<6} {'S':<2} {'GÜNLÜK%':>7} {'FİYAT':>8} {'HACİM':>7}" + (f"  {'NOT':<10}" if include_note else "")
    sep = "-" * len(header)
    lines = [title, "<pre>", header, sep]

    for r in rows:
        t = r.get("ticker", "n/a")
        sig = r.get("signal", "-")
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", float("nan"))
        note = r.get("signal_text", "")

        ch_s = "n/a" if (ch != ch) else f"{ch:+.2f}"
        cl_s = "n/a" if (cl != cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        if include_note:
            lines.append(f"{t:<6} {sig:<2} {ch_s:>7} {cl_s:>8} {vol_s:>7}  {note:<10}")
        else:
            lines.append(f"{t:<6} {sig:<2} {ch_s:>7} {cl_s:>8} {vol_s:>7}")

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

def format_top10_threshold(min_vol: float) -> str:
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
    parts = []
    for p in joined.split(","):
        p = p.strip()
        if not p:
            continue
        parts.extend(p.split())
    out = []
    for t in parts:
        tt = re.sub(r"[^A-Za-z0-9:_\.]", "", t).upper()
        if tt:
            out.append(tt)
    # uniq preserve order
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq

# -----------------------------
# Alarm logic
# -----------------------------
def can_send_alarm_for(ticker: str, now_ts: float) -> bool:
    last = LAST_ALARM_TS.get(ticker)
    if last is None:
        return True
    return (now_ts - last) >= (ALARM_COOLDOWN_MIN * 60)

def mark_alarm_sent(ticker: str, now_ts: float) -> None:
    LAST_ALARM_TS[ticker] = now_ts

def filter_new_alarms(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Alarm sadece TOPLAMA + DİP TOPLAMA.
    Aynı hisse 60 dk cooldown.
    """
    now_ts = time.time()
    out = []
    for r in rows:
        kind = r.get("signal_text", "")
        if kind not in ("TOPLAMA", "DİP TOPLAMA"):
            continue
        t = r.get("ticker", "")
        if not t:
            continue
        if can_send_alarm_for(t, now_ts):
            out.append(r)
    # hacme göre sırala
    out = sorted(out, key=lambda x: (x.get("volume") or 0) if (x.get("volume") == x.get("volume")) else 0, reverse=True)
    return out

# -----------------------------
# Telegram Handlers
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"🏓 Pong! ({BOT_VERSION})")

async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = update.effective_chat.id
    await update.message.reply_text(f"🧾 Chat ID: <code>{cid}</code>", parse_mode=ParseMode.HTML)

async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    xu_close, xu_change = await get_xu100_summary()
    rows = await build_rows_from_is_list(bist200_list)
    top10_min_vol = compute_signal_rows(rows, xu_change)

    first20 = rows[:20]
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]

    toplama_cand = pick_candidates(rows, "TOPLAMA")
    dip_cand = pick_candidates(rows, "DİP TOPLAMA")

    # 0) Mini kriter satırı (Top10 eşiği)
    await update.message.reply_text(
        f"🧱 <b>Kriter</b>: Top10 hacim eşiği ≥ <b>{format_top10_threshold(top10_min_vol)}</b>",
        parse_mode=ParseMode.HTML
    )

    # 1) Radar first 20
    await update.message.reply_text(
        make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"),
        parse_mode=ParseMode.HTML
    )

    # 2) Top 10 volume
    if top10_vol:
        await update.message.reply_text(
            make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"),
            parse_mode=ParseMode.HTML
        )

    # 3) Candidates
    await update.message.reply_text(
        make_table(toplama_cand, "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>") if toplama_cand
        else "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>\n—",
        parse_mode=ParseMode.HTML
    )

    await update.message.reply_text(
        make_table(dip_cand, "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>") if dip_cand
        else "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>\n—",
        parse_mode=ParseMode.HTML
    )

    # 4) Compact signal summary
    await update.message.reply_text(signal_summary_compact(rows), parse_mode=ParseMode.HTML)

    # 5) XU100 compact line
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    await update.message.reply_text(
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}",
        parse_mode=ParseMode.HTML
    )

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
    _, xu_change = await get_xu100_summary()
    rows = await build_rows_from_is_list(part_list)

    # Threshold'ı BIST200 üzerinden alıp (stabil), bu 20'liye uygula
    all_rows = await build_rows_from_is_list(bist200_list)
    top10_min_vol = compute_signal_rows(all_rows, xu_change)
    _apply_signals_with_threshold(rows, xu_change, top10_min_vol)

    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n(20 hisse)"
    await update.message.reply_text(make_table(rows, title), parse_mode=ParseMode.HTML)

# ✅ /watch -> ENV WATCHLIST=...  (fallback: WATCHLIST_BIST) + args override
async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Args varsa env'i override etsin
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

    # 10-12 önerin: pratik limit koyalım (istersen env ile değişir)
    max_watch = int(os.getenv("WATCHLIST_MAX", "12"))
    watch = watch[:max_watch]

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    xu_close, xu_change = await get_xu100_summary()

    # watchlist hisselerini çek
    rows = await build_rows_from_is_list(watch)

    # Top10 eşiği: BIST200 üzerinden stabil
    bist200_list = env_csv("BIST200_TICKERS")
    if bist200_list:
        all_rows = await build_rows_from_is_list(bist200_list)
        top10_min_vol = compute_signal_rows(all_rows, xu_change)
        _apply_signals_with_threshold(rows, xu_change, top10_min_vol)
        thresh_s = format_top10_threshold(top10_min_vol)
    else:
        top10_min_vol = compute_signal_rows(rows, xu_change)
        thresh_s = format_top10_threshold(top10_min_vol)

    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    await update.message.reply_text(
        f"👀 <b>WATCHLIST</b> (Top10 hacim eşiği ≥ <b>{thresh_s}</b>)\n"
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}",
        parse_mode=ParseMode.HTML
    )
    await update.message.reply_text(make_table(rows, "📌 <b>Watchlist Radar</b>"), parse_mode=ParseMode.HTML)

async def cmd_alarm_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
        f"🚨 <b>Alarm Durumu</b>\n"
        f"• Enabled: <b>{'ON' if ALARM_ENABLED else 'OFF'}</b>\n"
        f"• Interval: <b>{ALARM_INTERVAL_MIN} dk</b>\n"
        f"• Cooldown: <b>{ALARM_COOLDOWN_MIN} dk</b>\n"
        f"• ChatID env: <code>{ALARM_CHAT_ID or 'YOK'}</code>\n"
        f"• EOD: <b>{EOD_HOUR:02d}:{EOD_MINUTE:02d}</b>\n"
        f"• TZ: <b>{TZ.key}</b>"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

# -----------------------------
# Scheduled jobs
# -----------------------------
async def job_alarm_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    """30 dk’da bir: BIST200 tarar, yalnız TOPLAMA/DİP TOPLAMA alarm üretir + WATCHLIST tablosunu ekler."""
    if not ALARM_ENABLED:
        return
    if not ALARM_CHAT_ID:
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return

    try:
        xu_close, xu_change = await get_xu100_summary()

        all_rows = await build_rows_from_is_list(bist200_list)
        top10_min_vol = compute_signal_rows(all_rows, xu_change)
        thresh_s = format_top10_threshold(top10_min_vol)

        # Alarm adayları (yalnız TOPLAMA + DİP TOPLAMA)
        alarm_rows = filter_new_alarms(all_rows)
        if not alarm_rows:
            return

        # Cooldown işaretle
        ts_now = time.time()
        for r in alarm_rows:
            mark_alarm_sent(r.get("ticker", ""), ts_now)

        # Alarm tablosu (tek tablo, NOT sütunu ile)
        alarm_title = (
            f"🚨🚨 <b>ALARM GELDİ</b> ({BOT_VERSION}) 🚨🚨\n"
            f"🕒 {now_tr().strftime('%H:%M')}  |  Top10 Eşik ≥ <b>{thresh_s}</b>\n"
        )

        xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
        xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
        alarm_title += f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}"

        # Alarm tabloda sadece seçilenler kalsın
        alarm_table = make_table(alarm_rows, alarm_title, include_note=True)

        # WATCHLIST tablosu ekle (alarmla birlikte otomatik)
        watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")
        watch_table = ""
        if watch:
            max_watch = int(os.getenv("WATCHLIST_MAX", "12"))
            watch = watch[:max_watch]
            w_rows = await build_rows_from_is_list(watch)
            _apply_signals_with_threshold(w_rows, xu_change, top10_min_vol)
            watch_table = "\n\n" + make_table(w_rows, "👀 <b>WATCHLIST (Alarm Eki)</b>")

        footer = f"\n\n⏳ <i>Aynı hisse için {ALARM_COOLDOWN_MIN} dk cooldown aktif.</i>"

        text = alarm_table + watch_table + footer

        await context.bot.send_message(
            chat_id=int(ALARM_CHAT_ID),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.exception("Alarm job error: %s", e)

async def job_eod_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Her gün 17:50: EOD raporu atar (env chat id)."""
    if not ALARM_ENABLED:
        return
    if not ALARM_CHAT_ID:
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return

    try:
        xu_close, xu_change = await get_xu100_summary()
        rows = await build_rows_from_is_list(bist200_list)
        top10_min_vol = compute_signal_rows(rows, xu_change)

        first20 = rows[:20]
        rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
        top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]

        toplama_cand = pick_candidates(rows, "TOPLAMA")
        dip_cand = pick_candidates(rows, "DİP TOPLAMA")

        thresh_s = format_top10_threshold(top10_min_vol)

        xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
        xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

        header = (
            f"📌 <b>EOD RAPOR</b> ({BOT_VERSION})\n"
            f"🕒 {now_tr().strftime('%H:%M')}  |  Top10 Eşik ≥ <b>{thresh_s}</b>\n"
            f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}"
        )

        parts = [
            header,
            make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"),
            make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>") if top10_vol else "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>\n—",
            make_table(toplama_cand, "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>") if toplama_cand else "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>\n—",
            make_table(dip_cand, "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>") if dip_cand else "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>\n—",
            signal_summary_compact(rows),
        ]

        # Tek mesaja sığmazsa parçalayalım
        buf = ""
        for p in parts:
            chunk = (p + "\n\n")
            if len(buf) + len(chunk) > 3500:
                await context.bot.send_message(chat_id=int(ALARM_CHAT_ID), text=buf.strip(), parse_mode=ParseMode.HTML)
                buf = ""
            buf += chunk
        if buf.strip():
            await context.bot.send_message(chat_id=int(ALARM_CHAT_ID), text=buf.strip(), parse_mode=ParseMode.HTML)

        # WATCHLIST’i EOD sonunda da ekleyelim
        watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")
        if watch:
            max_watch = int(os.getenv("WATCHLIST_MAX", "12"))
            watch = watch[:max_watch]
            w_rows = await build_rows_from_is_list(watch)
            _apply_signals_with_threshold(w_rows, xu_change, top10_min_vol)
            await context.bot.send_message(
                chat_id=int(ALARM_CHAT_ID),
                text=make_table(w_rows, "👀 <b>WATCHLIST (EOD Eki)</b>"),
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.exception("EOD job error: %s", e)

def schedule_jobs(app: Application) -> None:
    """
    Alarm: 30 dk’da bir (10:00–17:30 arası)
    EOD: 17:50
    """
    jq = getattr(app, "job_queue", None)
    if jq is None:
        # JobQueue yoksa bot çökmesin diye sadece log atalım
        logger.warning("JobQueue bulunamadı. requirements.txt -> python-telegram-bot[job-queue]==22.5 gerekli.")
        return

    if not ALARM_ENABLED:
        logger.info("Alarm disabled by env.")
        return

    if not ALARM_CHAT_ID:
        logger.info("ALARM_CHAT_ID env yok. Alarm/EOD gönderilmeyecek.")
        return

    # 1) Repeating alarm scan (30 dk alignment)
    first = next_aligned_run(ALARM_INTERVAL_MIN)
    jq.run_repeating(
        job_alarm_scan,
        interval=ALARM_INTERVAL_MIN * 60,
        first=first,
        name="alarm_scan_repeating"
    )
    logger.info("Alarm scan scheduled every %d min. First=%s", ALARM_INTERVAL_MIN, first.isoformat())

    # 2) Daily EOD (17:50)
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

    # Schedule jobs (safe)
    schedule_jobs(app)

    logger.info("Bot starting... version=%s", BOT_VERSION)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
