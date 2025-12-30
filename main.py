import os
import re
import math
import time
import logging
from typing import Dict, List, Any, Tuple

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_VERSION = os.getenv("BOT_VERSION", "v1.3")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12


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


def tv_scan_symbols(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
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


def get_xu100_summary() -> Tuple[float, float]:
    m = tv_scan_symbols(["BIST:XU100"])
    d = m.get("XU100", {})
    return d.get("close", float("nan")), d.get("change", float("nan"))


def build_rows_from_is_list(is_list: List[str]) -> List[Dict[str, Any]]:
    tv_symbols = [normalize_is_ticker(t) for t in is_list if t.strip()]
    tv_map = tv_scan_symbols(tv_symbols)

    rows: List[Dict[str, Any]] = []
    for original in is_list:
        short = normalize_is_ticker(original).split(":")[-1]
        d = tv_map.get(short, {})
        if not d:
            rows.append({"ticker": short, "close": float("nan"), "change": float("nan"), "volume": float("nan")})
        else:
            rows.append({"ticker": short, "close": d["close"], "change": d["change"], "volume": d["volume"]})
    return rows


def compute_signal_rows(rows: List[Dict[str, Any]], xu100_change: float) -> None:
    """
    Adım-1 (stabil etiket):
    - Top10 hacim eşiği: Top10’un 10. sırası (min hacim)
    - Endeks Korelasyon Tuzagı (AYRIŞMA): xu100 <= -0.80 ve hisse >= +0.40 ve hacim Top10 eşiği üstü
    - Delta Thinking (TOPLAMA): xu100 <= -0.80 ve hisse <= +0.20 ve hacim Top10 eşiği üstü
    - KÂR KORUMA: hisse >= +4.00
    """
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10 = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]
    top10_min_vol = top10[-1]["volume"] if len(top10) == 10 else (top10[-1]["volume"] if top10 else float("inf"))

    for r in rows:
        ch = r.get("change", float("nan"))
        vol = r.get("volume", float("nan"))

        if ch != ch:  # nan
            r["signal"] = "-"
            continue

        if ch >= 4.0:
            r["signal"] = "⚠️ KÂR"
            continue

        if (vol == vol) and (vol >= top10_min_vol) and (xu100_change == xu100_change) and (xu100_change <= -0.80):
            if ch >= 0.40:
                r["signal"] = "🧠 AYRIŞMA"
                continue
            if ch <= 0.20:
                r["signal"] = "🧠 TOPLAMA"
                continue

        r["signal"] = "-"


def make_table(rows: List[Dict[str, Any]], title: str) -> str:
    header = f"{'HİSSE':<8} {'SİNYAL':<11} {'GÜNLÜK%':>8} {'FİYAT':>10} {'HACİM':>10}"
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

        lines.append(f"{t:<8} {sig:<11} {ch_s:>8} {cl_s:>10} {vol_s:>10}")
    lines.append("</pre>")
    return "\n".join(lines)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"🏓 Pong! Bot ayakta. ({BOT_VERSION})")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    close, xu_change = get_xu100_summary()
    close_s = "n/a" if (close != close) else f"{close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    rows = build_rows_from_is_list(bist200_list)
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

    await update.message.reply_text(make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"), parse_mode=ParseMode.HTML)

    if top10_vol:
        await update.message.reply_text(make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"), parse_mode=ParseMode.HTML)


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

    part_list = chunks[n - 1]
    rows = build_rows_from_is_list(part_list)
    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n(20 hisse)"
    await update.message.reply_text(make_table(rows, title), parse_mode=ParseMode.HTML)


def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env missing")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))

    logger.info("Bot starting... version=%s", BOT_VERSION)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
