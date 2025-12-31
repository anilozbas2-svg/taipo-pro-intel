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
BOT_VERSION = os.getenv("BOT_VERSION", "v1.2").strip() or "v1.2"

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


# -----------------------------
# TradingView Scanner
# -----------------------------
def tv_scan_symbols_sync(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}

    payload = {
        "symbols": {"tickers": symbols},
        "columns": ["close", "change", "volume"]
    }

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
                if not sym or len(d) < 3:
                    continue
                short = sym.split(":")[-1].upper()
                out[short] = {
                    "close": safe_float(d[0]),
                    "change": safe_float(d[1]),
                    "volume": safe_float(d[2]),
                }
            return out
        except Exception:
            time.sleep(1.0 * (attempt + 1))
    return {}


async def tv_scan_symbols(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    return await asyncio.to_thread(tv_scan_symbols_sync, symbols)


async def get_xu100_summary() -> Tuple[float, float]:
    m = await tv_scan_symbols(["BIST:XU100"])
    d = m.get("XU100", {})
    return d.get("close", float("nan")), d.get("change", float("nan"))


async def build_rows_from_is_list(is_list: List[str]) -> List[Dict[str, Any]]:
    tv_symbols = [normalize_is_ticker(t) for t in is_list]
    tv_map = await tv_scan_symbols(tv_symbols)

    rows = []
    for t in is_list:
        short = normalize_is_ticker(t).split(":")[-1]
        d = tv_map.get(short, {})
        rows.append({
            "ticker": short,
            "close": d.get("close", float("nan")),
            "change": d.get("change", float("nan")),
            "volume": d.get("volume", float("nan")),
            "signal": "-",
            "signal_text": ""
        })
    return rows


# -----------------------------
# Signal Logic (STABLE)
# -----------------------------
def compute_signal_rows(rows: List[Dict[str, Any]], xu100_change: float) -> None:
    rows_with_vol = [r for r in rows if r["volume"] == r["volume"]]
    top10 = sorted(rows_with_vol, key=lambda x: x["volume"], reverse=True)[:10]
    min_top10_vol = top10[-1]["volume"] if top10 else float("inf")

    for r in rows:
        ch = r["change"]
        vol = r["volume"]
        in_top10 = vol == vol and vol >= min_top10_vol

        if ch != ch:
            continue

        if ch >= 4.0:
            r["signal"] = "⚠️"
            r["signal_text"] = "KÂR KORUMA"
        elif in_top10 and xu100_change <= -0.80 and ch >= 0.40:
            r["signal"] = "🧠"
            r["signal_text"] = "AYRIŞMA"
        elif in_top10 and 0.00 <= ch <= 0.60:
            r["signal"] = "🧠"
            r["signal_text"] = "TOPLAMA"
        elif in_top10 and -0.60 <= ch < 0.00:
            r["signal"] = "🧲"
            r["signal_text"] = "DİP TOPLAMA"


# -----------------------------
# Table
# -----------------------------
def make_table(rows: List[Dict[str, Any]], title: str) -> str:
    lines = [
        title,
        "<pre>",
        f"{'HİSSE':<6} {'S':<2} {'GÜNLÜK%':>8} {'FİYAT':>10} {'HACİM':>10}",
        "-" * 42
    ]
    for r in rows:
        lines.append(
            f"{r['ticker']:<6} {r['signal']:<2} "
            f"{'n/a' if r['change']!=r['change'] else f'{r['change']:+.2f}':>8} "
            f"{'n/a' if r['close']!=r['close'] else f'{r['close']:.2f}':>10} "
            f"{format_volume(r['volume']):>10}"
        )
    lines.append("</pre>")
    return "\n".join(lines)


# -----------------------------
# Telegram Commands
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🏓 Pong! ({BOT_VERSION})")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bist = env_csv("BIST200_TICKERS")
    if not bist:
        await update.message.reply_text("❌ BIST200_TICKERS boş.")
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    _, xu_change = await get_xu100_summary()
    rows = await build_rows_from_is_list(bist)
    compute_signal_rows(rows, xu_change)

    await update.message.reply_text(make_table(rows[:20], "📍 <b>Hisse Radar (ilk 20)</b>"), parse_mode=ParseMode.HTML)

    top10 = sorted(rows, key=lambda x: x["volume"] if x["volume"] == x["volume"] else 0, reverse=True)[:10]
    await update.message.reply_text(make_table(top10, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"), parse_mode=ParseMode.HTML)

    toplama = [r for r in rows if r["signal_text"] == "TOPLAMA"]
    dip = [r for r in rows if r["signal_text"] == "DİP TOPLAMA"]

    if toplama:
        await update.message.reply_text(make_table(toplama, "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>"), parse_mode=ParseMode.HTML)
    if dip:
        await update.message.reply_text(make_table(dip, "🧲 <b>DİP TOPLAMA ADAYLAR</b>"), parse_mode=ParseMode.HTML)


def main():
    token = os.getenv("BOT_TOKEN")
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
