import os
import re
import math
import time
import json
import logging
from typing import Dict, List, Any, Tuple, Optional

import requests
from telegram import Update, BotCommand
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]

def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")

def is_nan(x: Any) -> bool:
    try:
        return math.isnan(float(x))
    except Exception:
        return True

def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]

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

def make_table(rows: List[Dict[str, Any]], title: str) -> str:
    header = f"{'HİSSE':<10} {'GÜNLÜK %':>9} {'FİYAT':>10} {'HACİM':>10}"
    sep = "-" * len(header)
    lines = [title, "<pre>", header, sep]
    for r in rows:
        t = r.get("ticker", "n/a")
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", None)

        ch_s = "n/a" if is_nan(ch) else f"{ch:+.2f}"
        cl_s = "n/a" if is_nan(cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        lines.append(f"{t:<10} {ch_s:>9} {cl_s:>10} {vol_s:>10}")
    lines.append("</pre>")
    return "\n".join(lines)

# -----------------------
# DATA SOURCE (Yahoo)
# -----------------------
YH_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

def to_yahoo_symbol(t: str) -> str:
    t = t.strip().upper()
    if not t:
        return t
    if t.startswith("BIST:"):
        t = t.split(":", 1)[1]
    if not t.endswith(".IS"):
        t = f"{t}.IS"
    return t

def yahoo_quote_map(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    params = {"symbols": ",".join(symbols)}
    headers = {"User-Agent": UA, "Accept": "application/json"}
    try:
        r = requests.get(YH_URL, params=params, headers=headers, timeout=12)
        if r.status_code != 200:
            logger.warning("Yahoo status=%s body=%s", r.status_code, r.text[:160])
            return {}
        js = r.json()
        res = js.get("quoteResponse", {}).get("result", []) or []
        out = {}
        for it in res:
            sym = (it.get("symbol") or "").upper()
            out[sym] = {
                "close": safe_float(it.get("regularMarketPrice")),
                "change": safe_float(it.get("regularMarketChangePercent")),
                "volume": safe_float(it.get("regularMarketVolume")),
            }
        return out
    except Exception as e:
        logger.exception("Yahoo error: %s", e)
        return {}

def get_xu100_summary() -> Tuple[float, float]:
    # Yahoo’da XU100 bazen farklı geçiyor; birkaç aday deniyoruz
    candidates = ["^XU100", "XU100.IS", "XU100.TI"]
    m = yahoo_quote_map([c.upper() for c in candidates])
    for c in candidates:
        d = m.get(c.upper(), {})
        cl = d.get("close", float("nan"))
        ch = d.get("change", float("nan"))
        if not is_nan(cl) and not is_nan(ch):
            return cl, ch
    return float("nan"), float("nan")

def build_rows_from_list(lst: List[str]) -> List[Dict[str, Any]]:
    ysyms = [to_yahoo_symbol(x) for x in lst]
    m = yahoo_quote_map([s.upper() for s in ysyms])
    rows = []
    for x in lst:
        short = to_yahoo_symbol(x).replace(".IS", "")
        d = m.get(to_yahoo_symbol(x).upper(), {})
        rows.append({
            "ticker": short,
            "close": d.get("close", float("nan")),
            "change": d.get("change", float("nan")),
            "volume": d.get("volume", float("nan")),
        })
    return rows

# -----------------------
# Telegram
# -----------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong! Bot ayakta.")

async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş.")
        return

    close, xu_change = get_xu100_summary()
    close_s = "n/a" if is_nan(close) else f"{close:,.2f}"
    ch_s = "n/a" if is_nan(xu_change) else f"{xu_change:+.2f}%"

    rows = build_rows_from_list(bist200_list)
    first20 = rows[:20]

    msg1 = (
        "📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: <b>{close_s}</b>\n"
        f"• Günlük: <b>{ch_s}</b>\n\n"
        "📡 Radar için:\n"
        "• /radar 1 … /radar 10"
    )
    await update.message.reply_text(msg1, parse_mode=ParseMode.HTML)
    await update.message.reply_text(make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"), parse_mode=ParseMode.HTML)

async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş.")
        return

    n = 1
    if context.args:
        try:
            n = int(re.sub(r"\D+", "", context.args[0]) or "1")
        except Exception:
            n = 1

    chunks = chunk_list(bist200_list, 20)
    total = len(chunks)
    if n < 1 or n > total:
        await update.message.reply_text(f"❌ /radar 1–{total} arası. (Sen: {n})")
        return

    part = chunks[n - 1]
    rows = build_rows_from_list(part)
    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total}</b>\n(20 hisse)"
    await update.message.reply_text(make_table(rows, title), parse_mode=ParseMode.HTML)

async def post_init(app: Application) -> None:
    try:
        commands = [
            BotCommand("ping", "Bot ayakta mı kontrol"),
            BotCommand("eod", "BIST100 özet + radar"),
            BotCommand("radar", "BIST200 radar (ör: /radar 1)"),
        ]
        await app.bot.set_my_commands(commands)
    except Exception as e:
        logger.warning("set_my_commands failed: %s", e)

def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env missing")

    app = Application.builder().token(token).post_init(post_init).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
