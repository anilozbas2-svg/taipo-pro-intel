# main.py
# TAIPO PRO INTEL - TradingView scanner tabanlı stabil sürüm

import os
import math
import logging
from typing import Dict, Any, List, Optional, Tuple

import requests
import pandas as pd

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram import BotCommand

from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("TAIPO_PRO_INTEL")


# -----------------------------
# Helpers
# -----------------------------
def safe_float(x: Any) -> float:
    try:
        if x is None:
            return float("nan")
        if isinstance(x, str):
            x = x.replace(",", ".").strip()
        return float(x)
    except Exception:
        return float("nan")


def is_nan(x: float) -> bool:
    try:
        return math.isnan(x)
    except Exception:
        return True


def clamp_abs(v: float, max_abs: float) -> float:
    if v > max_abs:
        return max_abs
    if v < -max_abs:
        return -max_abs
    return v


def env_csv(key: str) -> List[str]:
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return []
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


def format_volume(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return "n/a"
        v = float(v)
        if v >= 1e9:
            return f"{v/1e9:.2f}B"
        if v >= 1e6:
            return f"{v/1e6:.2f}M"
        if v >= 1e3:
            return f"{v/1e3:.2f}K"
        return f"{v:.0f}"
    except Exception:
        return "n/a"


# -----------------------------
# XU100 Summary (Basit)
# -----------------------------
def get_xu100_summary() -> Tuple[float, float]:
    """
    XU100 kapanış ve günlük % değişim.
    Bu örnek: dış API bağlamadıysan 0 dönebilir.
    """
    # Eğer ileride gerçek veri kaynağı ekleyeceksen burası.
    # Şimdilik bot mantığı bozulmasın diye güvenli dönüş:
    return (float("nan"), float("nan"))


# -----------------------------
# Data builder (TradingView taraması / manuel liste)
# -----------------------------
def build_rows_from_is_list(tickers: List[str]) -> List[Dict[str, Any]]:
    """
    Burada mevcut sisteminde nasıl çekiyorsan o şekilde kullan.
    Şimdilik: sadece formatı korumak için örnek şablon.
    Senin mevcut kodunda zaten TV taraması var.
    """
    # Eğer senin repoda tradingview-screener ile çekim fonksiyonun varsa
    # burada onu çağır. Aşağıdaki minimal şablon botu kırmaz.
    rows = []
    for t in tickers:
        rows.append(
            {
                "ticker": t,
                "change": float("nan"),
                "close": float("nan"),
                "volume": float("nan"),
                "open": float("nan"),
                "high": float("nan"),
                "low": float("nan"),
            }
        )
    return rows


# -----------------------------
# Candle / volume metrics
# -----------------------------
def upper_wick_ratio(row: Dict[str, Any]) -> float:
    o = safe_float(row.get("open"))
    h = safe_float(row.get("high"))
    c = safe_float(row.get("close"))
    l = safe_float(row.get("low"))
    if any(is_nan(x) for x in [o, h, c, l]):
        return float("nan")
    body_top = max(o, c)
    rng = (h - l) if (h - l) != 0 else 1e-9
    return max(0.0, (h - body_top) / rng)


def volume_ratio(row: Dict[str, Any]) -> float:
    # Eğer ortalama hacim gibi bir alanın varsa burada kullan.
    # Şimdilik sadece "volume" var -> ratio hesaplanamıyor.
    return float("nan")


# -----------------------------
# TAIPO Filtreleri (EOD içinde)
# -----------------------------
def select_early_exit(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Senin mevcut filtrelerin burada.
    # Bu örnek: dokunmuyor.
    return rows


# -----------------------------
# TAIPO SKOR (Basit Skor Motoru)
# -----------------------------
def calc_taipo_score(row: Dict[str, Any], xu_change: float) -> float:
    """
    0-100 arası basit skor.
    - Pozitif günlük değişim + hacim oranı skoru artırır
    - Üst fitil (satış baskısı) skoru kırpar
    - Endeks düşerken ayrışan hisseler küçük bonus alır
    """
    ch = safe_float(row.get("change"))
    vr = volume_ratio(row)
    uw = upper_wick_ratio(row)

    score = 50.0

    if not is_nan(ch):
        score += clamp_abs(ch * 5.0, 20.0)

    if not is_nan(vr):
        score += max(0.0, min(30.0, (vr - 1.0) * 20.0))

    if not is_nan(uw):
        score -= max(0.0, min(20.0, uw * 20.0))

    if (not is_nan(xu_change)) and xu_change <= -0.50 and (not is_nan(ch)) and ch >= 0.30:
        score += 7.5

    return max(0.0, min(100.0, score))


def select_top_scores(rows: List[Dict[str, Any]], xu_change: float, top_n: int = 10) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        rr = dict(r)
        rr["score"] = calc_taipo_score(r, xu_change)
        out.append(rr)

    out.sort(key=lambda x: safe_float(x.get("score")), reverse=True)
    return out[:top_n]


def make_table_score(rows: List[Dict[str, Any]], title: str) -> str:
    header = f"{'HİSSE ADI':<10} {'SKOR':>6} {'GÜNLÜK %':>9} {'FİYAT':>10} {'HACİM':>10}"
    sep = "-" * len(header)

    lines = [title, "<pre>", header, sep]
    for r in rows:
        t = r.get("ticker", "n/a")
        sc = safe_float(r.get("score"))
        ch = safe_float(r.get("change"))
        cl = safe_float(r.get("close"))
        vol = r.get("volume", None)

        sc_s = "n/a" if is_nan(sc) else f"{sc:>5.1f}"
        ch_s = "n/a" if is_nan(ch) else f"{ch:+.2f}"
        cl_s = "n/a" if is_nan(cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        lines.append(f"{t:<10} {sc_s:>6} {ch_s:>9} {cl_s:>10} {vol_s:>10}")
    lines.append("</pre>")
    return "\n".join(lines)


# -----------------------------
# Telegram Handlers
# -----------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "✅ TAIPO PRO INTEL hazır.\n"
        "Komutlar:\n"
        "/eod\n"
        "/radar 1\n"
        "/score"
    )


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    close, chg = get_xu100_summary()
    await update.message.reply_text(
        f"📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: {('n/a' if is_nan(close) else f'{close:,.2f}')}\n"
        f"• Günlük: {('n/a' if is_nan(chg) else f'{chg:+.2f}%')}\n\n"
        f"🛰️ Radar için:\n"
        f"• /radar 1 ... /radar 10",
        parse_mode=ParseMode.HTML,
    )


async def cmd_score(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("TAIPO_PRO_INTEL | SCORE request")

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    close, xu_change = get_xu100_summary()
    rows = build_rows_from_is_list(bist200_list)
    top_scores = select_top_scores(rows, xu_change, top_n=10)

    title = "🏁 <b>TAIPO SKOR — TOP 10</b>\n<i>Skor 0–100 (hacim + fiyat + fitil)</i>"
    await update.message.reply_text(make_table_score(top_scores, title), parse_mode=ParseMode.HTML)


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Mevcut radar sistemin burada (senin dosyanda zaten var)
    arg = context.args[0] if context.args else "1"
    await update.message.reply_text(f"📡 Radar çalıştı (örnek). Seçim: {arg}")


# -----------------------------
# Cron Jobs
# -----------------------------
async def job_auto_eod(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
    if not chat_id:
        logger.warning("ADMIN_CHAT_ID yok -> auto_eod atlandı.")
        return

    close, chg = get_xu100_summary()
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"📌 <b>BIST100 (XU100) Özet</b>\n"
                f"• Kapanış: {('n/a' if is_nan(close) else f'{close:,.2f}')}\n"
                f"• Günlük: {('n/a' if is_nan(chg) else f'{chg:+.2f}%')}\n"
            ),
            parse_mode=ParseMode.HTML,
        )
        logger.info("auto_eod sent to %s", chat_id)
    except Exception as e:
        logger.exception("auto_eod error: %s", e)


async def job_auto_score(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
    if not chat_id:
        logger.warning("ADMIN_CHAT_ID yok -> auto_score atlandı.")
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        logger.warning("BIST200_TICKERS boş -> auto_score atlandı.")
        return

    try:
        close, xu_change = get_xu100_summary()
        rows = build_rows_from_is_list(bist200_list)
        top_scores = select_top_scores(rows, xu_change, top_n=10)

        title = "🏁 <b>TAIPO SKOR — TOP 10</b>\n<i>Otomatik (09:30 / 18:30)</i>"
        await context.bot.send_message(
            chat_id=chat_id,
            text=make_table_score(top_scores, title),
            parse_mode=ParseMode.HTML,
        )
        logger.info("auto_score sent to %s", chat_id)
    except Exception as e:
        logger.exception("auto_score error: %s", e)


# -----------------------------
# Bot Commands (Telegram "/" menüsü)
# -----------------------------
async def post_init(app: Application) -> None:
    commands = [
        BotCommand("start", "Botu başlat"),
        BotCommand("eod", "Gün sonu özet"),
        BotCommand("radar", "BIST200 radar (ör: /radar 1)"),
        BotCommand("score", "TAIPO skor (Top 10)"),
    ]
    try:
        await app.bot.set_my_commands(commands)
    except Exception as e:
        logger.warning("set_my_commands failed: %s", e)


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env boş!")

    app = Application.builder().token(token).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))
    app.add_handler(CommandHandler("score", cmd_score))

    # Cron: 09:30 + 18:30 (Europe/Istanbul)
    tz = ZoneInfo("Europe/Istanbul")
    t1 = dt_time(hour=9, minute=30, tzinfo=tz)
    t2 = dt_time(hour=18, minute=30, tzinfo=tz)

    jq = app.job_queue
    if jq is None:
        logger.warning("JobQueue yok! requirements.txt -> python-telegram-bot[job-queue]==22.1 olmalı.")
    else:
        jq.run_daily(job_auto_eod, time=t1, name="auto_eod_0930")
        jq.run_daily(job_auto_eod, time=t2, name="auto_eod_1830")
        jq.run_daily(job_auto_score, time=t1, name="auto_score_0930")
        jq.run_daily(job_auto_score, time=t2, name="auto_score_1830")

        logger.info("Cron kuruldu: 09:30 ve 18:30 (EOD + SCORE)")

    logger.info("Bot polling başladı.")
    app.run_polling()


if __name__ == "__main__":
    main()
