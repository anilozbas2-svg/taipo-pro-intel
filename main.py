import os
import re
import math
import time
import logging
import asyncio
from typing import Dict, List, Any, Tuple

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# -----------------------------
# Config
# -----------------------------
BOT_VERSION = os.getenv("BOT_VERSION", "v1.1").strip() or "v1.1"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12

# -----------------------------
# Helpers
# -----------------------------
def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


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
        return f"{n/1_000_000_000:.2f}B"
    if absn >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if absn >= 1_000:
        return f"{n/1_000:.2f}K"
    return f"{n:.0f}"


def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


# ✅ /hisse için temiz argüman
def clean_ticker_arg(arg: str) -> str:
    a = (arg or "").strip().upper()
    a = re.sub(r"[^A-Z0-9\.\:]", "", a)
    if not a:
        return ""
    if a.startswith("BIST:"):
        a = a.replace("BIST:", "")
    if a.endswith(".IS"):
        a = a[:-3]
    return a


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
    # requests blocking olmasın diye thread'e alıyoruz
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
# 3'lü sistem (stabil)
# -----------------------------
def compute_signal_rows(rows: List[Dict[str, Any]], xu100_change: float) -> None:
    """
    v1.1 davranışı:
    - Top10 hacim eşiğini referans alır (Top10’un 10. sırası)
    - TOPLAMA: Top10 hacimde olup günlük değişim küçük/orta (|%| <= 0.60)  -> 🧠 + altta "TOPLAMA"
    - AYRIŞMA: Endeks sert düşüşte (<= -0.80) iken hisse +0.40 ve üstü + Top10 hacim -> 🧠 + altta "AYRIŞMA"
    - KÂR KORUMA: hisse >= +4.00 -> 🧠 değil ⚠️
    """
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

        if in_top10 and abs(ch) <= 0.60:
            r["signal"] = "🧠"
            r["signal_text"] = "TOPLAMA"
            continue

        r["signal"] = "-"
        r["signal_text"] = ""


# -----------------------------
# v1.1 tablo görünümü
# -----------------------------
def make_table_v11(rows: List[Dict[str, Any]], title: str) -> str:
    header1 = f"{'HİSSE':<6} {'GÜNLÜK%':>8} {'FİYAT':>10} {'HACİM':>10}"
    header2 = f"{'SİNYAL':<6}"
    sep = "-" * len(header1)

    lines = [title, "<pre>", header1, header2, sep]

    for r in rows:
        t = r.get("ticker", "n/a")
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", float("nan"))

        ch_s = "n/a" if (ch != ch) else f"{ch:+.2f}"
        cl_s = "n/a" if (cl != cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        lines.append(f"{t:<6} {ch_s:>8} {cl_s:>10} {vol_s:>10}")

        sig = r.get("signal", "-")
        sig_text = r.get("signal_text", "")
        if sig != "-" and sig_text:
            lines.append(f"{sig_text:<6} {'':>8} {'':>10} {sig:>10}")

    lines.append("</pre>")
    return "\n".join(lines)


def summarize_signals(rows: List[Dict[str, Any]]) -> str:
    toplama = [r["ticker"] for r in rows if r.get("signal_text") == "TOPLAMA"]
    ayrisma = [r["ticker"] for r in rows if r.get("signal_text") == "AYRIŞMA"]
    kar = [r["ticker"] for r in rows if r.get("signal_text") == "KÂR KORUMA"]

    def join_list(lst: List[str]) -> str:
        return ", ".join(lst) if lst else "—"

    msg = (
        f"🧠 <b>Sinyal Özeti ({BOT_VERSION})</b>\n"
        f"• 🧠 TOPLAMA: {join_list(toplama)}\n"
        f"• 🧠 AYRIŞMA: {join_list(ayrisma)}\n"
        f"• ⚠️ KÂR KORUMA: {join_list(kar)}\n\n"
        "Not: v1.1'de hacim/delta için Top10 hacim ranking kullanılır. (Stabil mod)"
    )
    return msg


# ✅ /hisse kartı
def build_hisse_card(row: Dict[str, Any], xu_close: float, xu_change: float) -> str:
    t = row.get("ticker", "n/a")
    ch = row.get("change", float("nan"))
    cl = row.get("close", float("nan"))
    vol = row.get("volume", float("nan"))

    sig_emoji = row.get("signal", "-")
    sig_text = row.get("signal_text", "") or "—"

    ch_s = "n/a" if (ch != ch) else f"{ch:+.2f}%"
    cl_s = "n/a" if (cl != cl) else f"{cl:.2f}"
    vol_s = format_volume(vol)

    xu_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"

    comment = "—"
    if sig_text == "TOPLAMA":
        comment = "Hacim Top10 + hareket küçük/orta → TOPLAMA adayı."
    elif sig_text == "AYRIŞMA":
        comment = "Endeks sert düşüşteyken pozitif → AYRIŞMA (güç)."
    elif sig_text == "KÂR KORUMA":
        comment = "Günlük %4+ → KÂR KORUMA (kâr kilitleme)."

    msg = (
        f"📌 <b>HİSSE DETAY</b> — <b>{t}</b>\n"
        f"• Fiyat: <b>{cl_s}</b>\n"
        f"• Günlük: <b>{ch_s}</b>\n"
        f"• Hacim: <b>{vol_s}</b>\n"
        f"• Sinyal: <b>{sig_emoji} {sig_text}</b>\n\n"
        f"📊 <b>XU100</b>\n"
        f"• Kapanış: <b>{xu_close_s}</b>\n"
        f"• Günlük: <b>{xu_s}</b>\n\n"
        f"📝 <b>Not</b>: {comment}\n"
        f"⚙️ Sürüm: <b>{BOT_VERSION}</b>"
    )
    return msg


# -----------------------------
# Telegram Handlers
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"🏓 Pong! Bot ayakta. ({BOT_VERSION})")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    close, xu_change = await get_xu100_summary()
    close_s = "n/a" if (close != close) else f"{close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    rows = await build_rows_from_is_list(bist200_list)
    compute_signal_rows(rows, xu_change)

    first20 = rows[:20]
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]

    msg1 = (
        "📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: <b>{close_s}</b>\n"
        f"• Günlük: <b>{xu_change_s}</b>\n\n"
        "📡 Radar için:\n"
        "• /radar 1 … /radar 10\n\n"
        f"⚙️ Sürüm: <b>{BOT_VERSION}</b>"
    )
    await update.message.reply_text(msg1, parse_mode=ParseMode.HTML)

    await update.message.reply_text(
        make_table_v11(first20, "📍 <b>Hisse Radar (ilk 20)</b>"),
        parse_mode=ParseMode.HTML
    )

    if top10_vol:
        await update.message.reply_text(
            make_table_v11(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"),
            parse_mode=ParseMode.HTML
        )

    await update.message.reply_text(summarize_signals(rows), parse_mode=ParseMode.HTML)


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
    compute_signal_rows(rows, xu_change)

    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n(20 hisse)"
    await update.message.reply_text(make_table_v11(rows, title), parse_mode=ParseMode.HTML)


# ✅ /hisse KOMUTU
async def cmd_hisse(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Kullanım: /hisse SASA", parse_mode=ParseMode.HTML)
        return

    wanted = clean_ticker_arg(context.args[0])
    if not wanted:
        await update.message.reply_text("Kullanım: /hisse SASA", parse_mode=ParseMode.HTML)
        return

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    xu_close, xu_change = await get_xu100_summary()

    # Top10 eşiğini doğru hesaplamak için tüm BIST200'ü çekiyoruz
    rows = await build_rows_from_is_list(bist200_list)
    compute_signal_rows(rows, xu_change)

    row = next((r for r in rows if (r.get("ticker") or "").upper() == wanted), None)
    if not row:
        await update.message.reply_text(
            f"❌ Bulunamadı: <b>{wanted}</b>\nÖrnek: /hisse SASA",
            parse_mode=ParseMode.HTML
        )
        return

    await update.message.reply_text(build_hisse_card(row, xu_close, xu_change), parse_mode=ParseMode.HTML)


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
    app.add_handler(CommandHandler("hisse", cmd_hisse))  # ✅ yeni

    logger.info("Bot starting... version=%s", BOT_VERSION)

    # En uyumlu / en az crash riski olan polling
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
