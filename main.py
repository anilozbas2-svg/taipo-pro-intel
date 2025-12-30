# main.py
# TAIPO PRO INTEL - TradingView scanner tabanlı stabil sürüm
# Komutlar: /ping, /eod, /radar <1-10>
#
# ENV:
#   BOT_TOKEN=...
#   BIST200_TICKERS=THYAO.IS,ASELS.IS,AKBNK.IS,...
#   WATCHLIST_BIST=... (opsiyonel)
#   MODE=prod (opsiyonel)
#   BIST_CURRENCY=TRY (opsiyonel)
#   BOT_VERSION=v1.2 (opsiyonel)

import os
import re
import math
import time
import sys
import logging
from typing import Dict, List, Any, Tuple

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# -----------------------------
# Version / Instance (teşhis)
# -----------------------------
BOT_VERSION = os.getenv("BOT_VERSION", "v1.2")
INSTANCE_ID = os.getenv("RENDER_INSTANCE_ID", str(os.getpid()))

# -----------------------------
# Lock (tek instance garanti) -> Conflict fix
# -----------------------------
LOCK_PATH = "/tmp/taipo_bot.lock"


def acquire_lock_or_exit() -> None:
    """
    Aynı container içinde ikinci process start ederse Telegram 'Conflict' atabiliyor.
    Bu lock ikinci instance'ı sessizce kapatır => sistemi bozmaz, sadece çakışmayı önler.
    """
    if os.path.exists(LOCK_PATH):
        print("LOCK exists -> another instance is running. Exiting.")
        sys.exit(0)

    with open(LOCK_PATH, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

    import atexit

    def _cleanup() -> None:
        try:
            os.remove(LOCK_PATH)
        except Exception:
            pass

    atexit.register(_cleanup)


# -----------------------------
# Logging
# -----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

# -----------------------------
# Helpers
# -----------------------------
def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p]
    return parts


def normalize_is_ticker(t: str) -> str:
    t = t.strip().upper()
    if not t:
        return t
    # Accept: ASELS, ASELS.IS, BIST:ASELS
    if t.startswith("BIST:"):
        base = t.replace("BIST:", "")
    else:
        base = t
    # Remove .IS if exists for TradingView symbol format
    if base.endswith(".IS"):
        base = base[:-3]
    # final format: BIST:ASELS
    return f"BIST:{base}"


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


def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def make_table(rows: List[Dict[str, Any]], title: str) -> str:
    header = f"{'HİSSE ADI':<10} {'GÜNLÜK %':>9} {'FİYAT':>10} {'HACİM':>10}"
    sep = "-" * len(header)

    lines = [title, "<pre>", header, sep]
    for r in rows:
        t = r.get("ticker", "n/a")
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", None)

        ch_s = "n/a" if (ch != ch) else f"{ch:+.2f}"
        cl_s = "n/a" if (cl != cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        lines.append(f"{t:<10} {ch_s:>9} {cl_s:>10} {vol_s:>10}")
    lines.append("</pre>")
    return "\n".join(lines)


# -----------------------------
# TradingView Scanner Client
# -----------------------------
TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12


def tv_scan_symbols(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Tek request ile çok sembol çekiyoruz => rate limit’e daha dayanıklı.
    Dönen map: { 'ASELS': {'close':..., 'change':..., 'volume':...}, ... }
    """
    if not symbols:
        return {}

    payload = {
        "symbols": {"tickers": symbols},
        "columns": ["close", "change", "volume"],
    }

    for attempt in range(3):
        try:
            r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
            if r.status_code == 429:
                sleep_s = 1.5 * (attempt + 1)
                logger.warning("TradingView rate limit (429). Sleep %.1fs", sleep_s)
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            data = r.json()
            out: Dict[str, Dict[str, Any]] = {}

            items = data.get("data", [])
            for it in items:
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
    """
    XU100 için close + günlük değişim. TradingView'de genelde BIST:XU100.
    """
    m = tv_scan_symbols(["BIST:XU100"])
    d = m.get("XU100", {})
    close = d.get("close", float("nan"))
    change = d.get("change", float("nan"))
    return close, change


def build_rows_from_is_list(is_list: List[str]) -> List[Dict[str, Any]]:
    tv_symbols = [normalize_is_ticker(t) for t in is_list if t.strip()]
    tv_map = tv_scan_symbols(tv_symbols)

    rows: List[Dict[str, Any]] = []
    for original in is_list:
        short = normalize_is_ticker(original).split(":")[-1]
        d = tv_map.get(short, {})
        if not d:
            rows.append({"ticker": short, "close": float("nan"), "change": float("nan"), "volume": None})
        else:
            rows.append({"ticker": short, "close": d["close"], "change": d["change"], "volume": d["volume"]})
    return rows


# -----------------------------
# Telegram Handlers
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"🏓 Pong! Bot ayakta. ({BOT_VERSION}) | instance={INSTANCE_ID}")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("TAIPO_PRO_INTEL | EOD request")

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    close, change = get_xu100_summary()
    close_s = "n/a" if (close != close) else f"{close:,.2f}"
    change_s = "n/a" if (change != change) else f"{change:+.2f}%"

    rows = build_rows_from_is_list(bist200_list)
    first20 = rows[:20]

    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]

    msg1 = (
        "📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: <b>{close_s}</b>\n"
        f"• Günlük: <b>{change_s}</b>\n\n"
        "📡 Radar için:\n"
        "• /radar 1 … /radar 10\n\n"
        f"⚙️ Sürüm: <b>{BOT_VERSION}</b>"
    )
    await update.message.reply_text(msg1, parse_mode=ParseMode.HTML)

    await update.message.reply_text(
        make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"),
        parse_mode=ParseMode.HTML
    )

    if top10_vol:
        await update.message.reply_text(
            make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"),
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("⚠️ Hacim verisi bulunamadı (TOP10 üretilemedi).")


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("TAIPO_PRO_INTEL | RADAR request: %s", update.message.text)

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    n = 1
    if context.args and len(context.args) >= 1:
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


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    # Conflict hatasını bitiren kilit
    acquire_lock_or_exit()

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env missing")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))

    logger.info("Bot starting... version=%s instance=%s", BOT_VERSION, INSTANCE_ID)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
