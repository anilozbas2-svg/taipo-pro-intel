import os
import re
import math
import time
import logging
import asyncio
from typing import Dict, List, Any, Tuple, Optional

from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# -----------------------------
# Config
# -----------------------------
BOT_VERSION = os.getenv("BOT_VERSION", "v1.3.4-hybrid").strip() or "v1.3.4-hybrid"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12

TR_TZ = ZoneInfo("Europe/Istanbul")

# Otomatik rapor/alarm ayarları
REPORT_INTERVAL_MIN = int(os.getenv("REPORT_INTERVAL_MIN", "30") or "30")  # 30 dk
ALARM_COOLDOWN_MIN = int(os.getenv("ALARM_COOLDOWN_MIN", "60") or "60")     # aynı sinyal 60 dk'da 1
AUTO_ENABLED = (os.getenv("AUTO_ENABLED", "1").strip() != "0")             # 1 = açık, 0 = kapalı

# Seans (TR)
SESSION_START = dtime(10, 0)   # 10:00
SESSION_END = dtime(17, 50)    # 17:50

# ENV Chat hedefi (öncelik)
# 1) ALARM_CHAT_ID  2) CHAT_ID  3) son konuşulan chat
def env_chat_id() -> Optional[int]:
    for k in ("ALARM_CHAT_ID", "CHAT_ID"):
        v = os.getenv(k, "").strip()
        if v:
            try:
                return int(v)
            except Exception:
                pass
    return None


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
    return datetime.now(TR_TZ)


def is_session_time(dt: Optional[datetime] = None) -> bool:
    dt = dt or now_tr()
    # hafta içi 0-4
    if dt.weekday() > 4:
        return False
    t = dt.time()
    return (t >= SESSION_START) and (t <= SESSION_END)


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
            rows.append({"ticker": short, "close": float("nan"), "change": float("nan"), "volume": float("nan"), "signal": "-"})
        else:
            rows.append({"ticker": short, "close": d["close"], "change": d["change"], "volume": d["volume"], "signal": "-"})
    return rows


# -----------------------------
# Hybrid signal engine
# -----------------------------
def compute_signal_rows(rows: List[Dict[str, Any]], xu100_change: float) -> float:
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10 = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]
    top10_min_vol = top10[-1]["volume"] if len(top10) == 10 else (top10[-1]["volume"] if top10 else float("inf"))

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

    return float(top10_min_vol)


def _apply_signals_with_threshold(rows: List[Dict[str, Any]], xu100_change: float, top10_min_vol: float) -> None:
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
def make_table(rows: List[Dict[str, Any]], title: str) -> str:
    header = f"{'HİSSE':<6} {'S':<2} {'GÜNLÜK%':>7} {'FİYAT':>8} {'HACİM':>7}"
    sep = "-" * len(header)
    lines = [title, "<pre>", header, sep]

    for r in rows:
        t = r.get("ticker", "n/a")
        sig = r.get("signal", "-")
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", float("nan"))

        ch_s = "n/a" if (ch != ch) else f"{ch:+.2f}"
        cl_s = "n/a" if (cl != cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        lines.append(f"{t:<6} {sig:<2} {ch_s:>7} {cl_s:>8} {vol_s:>7}")

    lines.append("</pre>")
    return "\n".join(lines)


def format_top10_threshold(min_vol: float) -> str:
    if not isinstance(min_vol, (int, float)) or math.isnan(min_vol) or min_vol == float("inf"):
        return "n/a"
    return format_volume(min_vol)


# -----------------------------
# Auto Watch Report + Alarm (single message)
# -----------------------------
def _get_target_chat_id(context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    cid = env_chat_id()
    if cid:
        return cid
    # fallback: son chat
    last = context.application.bot_data.get("last_chat_id")
    if isinstance(last, int):
        return last
    return None


def _cooldown_ok(context: ContextTypes.DEFAULT_TYPE, key: str, now_ts: float) -> bool:
    cd = context.application.bot_data.setdefault("alarm_cooldown", {})  # key->last_ts
    last_ts = cd.get(key)
    if last_ts is None:
        cd[key] = now_ts
        return True
    if (now_ts - float(last_ts)) >= (ALARM_COOLDOWN_MIN * 60):
        cd[key] = now_ts
        return True
    return False


async def build_watch_rows_with_threshold() -> Tuple[List[Dict[str, Any]], str, float, float]:
    watch = env_csv_fallback("WATCHLIST", "WATCHLIST_BIST")
    if not watch:
        return [], "WATCHLIST env boş", float("nan"), float("nan")

    xu_close, xu_change = await get_xu100_summary()

    rows = await build_rows_from_is_list(watch)

    # threshold: BIST200 varsa oradan al (daha stabil)
    bist200_list = env_csv("BIST200_TICKERS")
    if bist200_list:
        all_rows = await build_rows_from_is_list(bist200_list)
        top10_min_vol = compute_signal_rows(all_rows, xu_change)
        _apply_signals_with_threshold(rows, xu_change, top10_min_vol)
    else:
        top10_min_vol = compute_signal_rows(rows, xu_change)

    thresh_s = format_top10_threshold(top10_min_vol)
    return rows, thresh_s, xu_close, xu_change


async def job_watch_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not AUTO_ENABLED:
        return

    dt = now_tr()
    if not is_session_time(dt):
        return

    chat_id = _get_target_chat_id(context)
    if not chat_id:
        logger.warning("Auto report: chat_id yok (ALARM_CHAT_ID/CHAT_ID/last_chat_id).")
        return

    rows, thresh_s, xu_close, xu_change = await build_watch_rows_with_threshold()
    if not rows:
        # watchlist yoksa sessiz kalma, bilgi ver
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ WATCHLIST env boş.\nÖrnek: WATCHLIST=AKBNK,CANTE,EREGL\n(Alternatif: WATCHLIST_BIST=AKBNK,CANTE,EREGL)",
            parse_mode=ParseMode.HTML,
        )
        return

    # Alarm sadece TOPLAMA + DİP TOPLAMA
    now_ts = dt.timestamp()
    eligible_alarm = []
    for r in rows:
        st = r.get("signal_text", "")
        if st in ("TOPLAMA", "DİP TOPLAMA"):
            key = f"{r.get('ticker','?')}|{st}"
            if _cooldown_ok(context, key, now_ts):
                eligible_alarm.append(r)

    is_alarm = len(eligible_alarm) > 0

    # Tek mesaj, tek tablo: üstte ALARM / RAPOR başlığı
    title = "🚨 <b>ALARM GELDİ</b>" if is_alarm else "⏱️ <b>RAPOR</b>"
    ts_line = f"<b>{dt.strftime('%d.%m.%Y %H:%M')}</b> • {BOT_VERSION}"
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    header = (
        f"{title}  —  {ts_line}\n"
        f"👀 <b>WATCHLIST</b> (Top10 hacim eşiği ≥ <b>{thresh_s}</b>)\n"
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}\n"
    )

    table = make_table(rows, "📌 <b>Watchlist Radar</b>")

    footer = ""
    if is_alarm:
        # alarm tickers listesi (tek satır, şık)
        names = ", ".join([r["ticker"] for r in eligible_alarm]) if eligible_alarm else "—"
        footer = f"\n<b>Alarm Tetiklenenler</b>: {names}\n<i>(Sadece TOPLAMA/DİP TOPLAMA • Aynı sinyal {ALARM_COOLDOWN_MIN} dk’da 1)</i>"
    else:
        footer = f"\n<i>Rapor periyodu: {REPORT_INTERVAL_MIN} dk</i>"

    await context.bot.send_message(
        chat_id=chat_id,
        text=header + "\n" + table + footer,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# -----------------------------
# Telegram Handlers
# -----------------------------
async def _remember_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        cid = int(update.effective_chat.id)
        context.application.bot_data["last_chat_id"] = cid
    except Exception:
        pass


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _remember_chat(update, context)
    await update.message.reply_text(f"🏓 Pong! ({BOT_VERSION})")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _remember_chat(update, context)

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

    await update.message.reply_text(
        f"🧱 <b>Kriter</b>: Top10 hacim eşiği ≥ <b>{format_top10_threshold(top10_min_vol)}</b>",
        parse_mode=ParseMode.HTML
    )

    await update.message.reply_text(
        make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"),
        parse_mode=ParseMode.HTML
    )

    if top10_vol:
        await update.message.reply_text(
            make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"),
            parse_mode=ParseMode.HTML
        )

    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    await update.message.reply_text(
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}",
        parse_mode=ParseMode.HTML
    )


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _remember_chat(update, context)

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
    compute_signal_rows(rows, xu_change)

    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n(20 hisse)"
    await update.message.reply_text(make_table(rows, title), parse_mode=ParseMode.HTML)


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _remember_chat(update, context)

    rows, thresh_s, xu_close, xu_change = await build_watch_rows_with_threshold()
    if not rows:
        await update.message.reply_text(
            "❌ WATCHLIST env boş.\nÖrnek: WATCHLIST=AKBNK,CANTE,EREGL\n(Alternatif: WATCHLIST_BIST=AKBNK,CANTE,EREGL)",
            parse_mode=ParseMode.HTML
        )
        return

    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    await update.message.reply_text(
        f"👀 <b>WATCHLIST</b> (Top10 hacim eşiği ≥ <b>{thresh_s}</b>)\n"
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}",
        parse_mode=ParseMode.HTML
    )
    await update.message.reply_text(make_table(rows, "📌 <b>Watchlist Radar</b>"), parse_mode=ParseMode.HTML)


async def cmd_auto(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /auto on|off  (runtime toggle - bot restart olunca AUTO_ENABLED env'e döner)
    """
    await _remember_chat(update, context)

    global AUTO_ENABLED
    arg = (context.args[0].lower() if context.args else "").strip()
    if arg in ("on", "1", "aç", "ac"):
        AUTO_ENABLED = True
        await update.message.reply_text("✅ Auto RAPOR/ALARM: AÇIK")
    elif arg in ("off", "0", "kapat"):
        AUTO_ENABLED = False
        await update.message.reply_text("🛑 Auto RAPOR/ALARM: KAPALI")
    else:
        st = "AÇIK" if AUTO_ENABLED else "KAPALI"
        await update.message.reply_text(f"ℹ️ Auto durum: {st}\nKullanım: /auto on  veya  /auto off")


# -----------------------------
# Scheduler
# -----------------------------
def schedule_jobs(app: Application) -> None:
    jq = app.job_queue
    if not jq:
        logger.warning("JobQueue yok. requirements: python-telegram-bot[job-queue]==22.5")
        return

    # 30 dakikada bir rapor/alarm (tek mesaj)
    interval = max(5, REPORT_INTERVAL_MIN) * 60
    jq.run_repeating(job_watch_report, interval=interval, first=10, name="watch_report_alarm")
    logger.info("Scheduled watch report/alarm every %s minutes", REPORT_INTERVAL_MIN)


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env missing")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("auto", cmd_auto))

    schedule_jobs(app)

    logger.info("Bot starting... version=%s", BOT_VERSION)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
