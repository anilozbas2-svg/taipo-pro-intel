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
#   LOG_LEVEL=INFO (opsiyonel)

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

# -----------------------------
# Logging
# -----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

VERSION = "v1.2"

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
    if t.startswith("BIST:"):
        base = t.replace("BIST:", "")
    else:
        base = t
    if base.endswith(".IS"):
        base = base[:-3]
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
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not math.isnan(float(x))

def volume_strength_map(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Her hissenin hacmini, tüm liste içindeki göreli güce çevir.
    0.0 - 1.0 arası skor: 1.0 en yüksek hacim tarafı.
    """
    vols = []
    for r in rows:
        v = r.get("volume")
        if is_num(v):
            vols.append(float(v))
    vols = sorted(vols)
    if not vols:
        return {}

    def percentile(v: float) -> float:
        # basit percentile
        # kaç tanesi <= v
        lo = 0
        hi = len(vols)
        while lo < hi:
            mid = (lo + hi) // 2
            if vols[mid] <= v:
                lo = mid + 1
            else:
                hi = mid
        return lo / len(vols)

    out = {}
    for r in rows:
        t = r.get("ticker")
        v = r.get("volume")
        if t and is_num(v):
            out[t] = percentile(float(v))
    return out

def classify_signal(
    ticker: str,
    stock_change: float,
    stock_vol_strength: float,
    xu100_change: float,
) -> str:
    """
    Sinyaller:
    🧠 AYRIŞMA: Endeks <= -0.80 ve hisse >= +0.40 ve hacim güçlü (>= 0.80)
    🐜 TOPLAMA: |hisse değişim| <= 0.35 ve hacim güçlü (>= 0.85)
    """
    if not is_num(stock_change):
        return "—"

    # Güçlü ayrışma (endeks düşerken hisse + hacim)
    if is_num(xu100_change) and xu100_change <= -0.80:
        if stock_change >= 0.40 and stock_vol_strength >= 0.80:
            return "🧠 AYRIŞMA"

    # Toplama (fiyat çok oynamıyor ama hacim yüksek)
    if abs(stock_change) <= 0.35 and stock_vol_strength >= 0.85:
        return "🐜 TOPLAMA"

    return "—"

def make_table(rows: List[Dict[str, Any]], title: str) -> str:
    header = f"{'HİSSE':<8} {'SİNYAL':<10} {'GÜNLÜK%':>8} {'FİYAT':>10} {'HACİM':>10}"
    sep = "-" * len(header)

    lines = [title, "<pre>", header, sep]
    for r in rows:
        t = r.get("ticker", "n/a")
        sig = r.get("signal", "—")
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", None)

        ch_s = "n/a" if (ch != ch) else f"{ch:+.2f}"
        cl_s = "n/a" if (cl != cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        lines.append(f"{t:<8} {sig:<10} {ch_s:>8} {cl_s:>10} {vol_s:>10}")
    lines.append("</pre>")
    return "\n".join(lines)

# -----------------------------
# TradingView Scanner Client
# -----------------------------
TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12

def tv_scan_symbols(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
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
            rows.append({"ticker": short, "close": float("nan"), "change": float("nan"), "volume": float("nan")})
        else:
            rows.append({"ticker": short, "close": d["close"], "change": d["change"], "volume": d["volume"]})
    return rows

# -----------------------------
# Telegram Handlers
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"🏓 Pong! Bot ayakta. ({VERSION})")

async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("TAIPO_PRO_INTEL | EOD request")

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    xu_close, xu_change = get_xu100_summary()
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

    rows = build_rows_from_is_list(bist200_list)

    # hacim gücü (0-1)
    vmap = volume_strength_map(rows)

    # sinyal hesapla
    for r in rows:
        t = r.get("ticker", "")
        ch = r.get("change", float("nan"))
        vs = vmap.get(t, 0.0)
        r["signal"] = classify_signal(t, ch, vs, xu_change)

    first20 = rows[:20]

    rows_with_vol = [r for r in rows if is_num(r.get("volume"))]
    top10_vol = sorted(rows_with_vol, key=lambda x: float(x.get("volume", 0.0)), reverse=True)[:10]

    # Top10 tablosunda da sinyal göstermek için:
    top10_vmap = volume_strength_map(top10_vol)
    for r in top10_vol:
        t = r.get("ticker", "")
        ch = r.get("change", float("nan"))
        vs = top10_vmap.get(t, 0.0)  # küçük listede göreli
        r["signal"] = classify_signal(t, ch, vs, xu_change)

    msg1 = (
        "📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: <b>{xu_close_s}</b>\n"
        f"• Günlük: <b>{xu_change_s}</b>\n\n"
        "📡 Radar için:\n"
        "• /radar 1 … /radar 10\n\n"
        f"⚙️ Sürüm: <b>{VERSION}</b>"
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

    # Sinyal özeti
    ayrisma = [r["ticker"] for r in rows if r.get("signal") == "🧠 AYRIŞMA"]
    toplama = [r["ticker"] for r in rows if r.get("signal") == "🐜 TOPLAMA"]

    msg2 = (
        f"🧠 <b>Sinyal Özeti ({VERSION})</b>\n"
        f"• 🐜 TOPLAMA: {', '.join(toplama) if toplama else '—'}\n"
        f"• 🧠 AYRIŞMA: {', '.join(ayrisma) if ayrisma else '—'}\n\n"
        f"Not: {VERSION}’de “AYRIŞMA”, endeks düşüşünde (+) hisse ve güçlü hacim koşuluyla üretilir."
    )
    await update.message.reply_text(msg2, parse_mode=ParseMode.HTML)

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

    xu_close, xu_change = get_xu100_summary()

    part_list = chunks[n - 1]
    rows = build_rows_from_is_list(part_list)

    vmap = volume_strength_map(rows)
    for r in rows:
        t = r.get("ticker", "")
        ch = r.get("change", float("nan"))
        vs = vmap.get(t, 0.0)
        r["signal"] = classify_signal(t, ch, vs, xu_change)

    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n(20 hisse)"
    await update.message.reply_text(make_table(rows, title), parse_mode=ParseMode.HTML)

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

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
