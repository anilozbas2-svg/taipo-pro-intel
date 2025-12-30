# main.py
# TAIPO PRO INTEL - TradingView scanner tabanlı stabil sürüm (v1.1)
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
from typing import Dict, List, Any, Tuple, Set

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

VERSION = "v1.1"

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
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def is_nan(x: Any) -> bool:
    try:
        return math.isnan(float(x))
    except Exception:
        return True

# -----------------------------
# Signal Engine (Step 1 - ranking based, safe)
# -----------------------------
def compute_signals(
    rows: List[Dict[str, Any]],
    xu100_change: float,
    top10_by_volume: Set[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
    """
    rows içine 'signal' alanı ekler.
    Ayrıca özet dict döner: {"TOPLAMA":[...], "AYRISHMA":[...], "KAR_KORUMA":[...]}
    Kriterler (v1.1):
      🧠 TOPLAMA:
        - abs(daily%) <= 0.40
        - Top10 hacimde
      🧠 AYRIŞMA:
        - xu100_change <= -0.80
        - daily% >= +0.40
        - Top10 hacimde
      ⚠️ KÂR KORUMA:
        - (daily% >= +5.00 and xu100_change <= -0.50)  OR
        - (Top10 hacimde and abs(daily%) <= 0.20)
    """
    summary = {"TOPLAMA": [], "AYRISHMA": [], "KAR_KORUMA": []}

    for r in rows:
        t = (r.get("ticker") or "").strip().upper()
        ch = r.get("change", float("nan"))

        labels: List[str] = []

        if not is_nan(ch):
            # TOPLAMA
            if (abs(ch) <= 0.40) and (t in top10_by_volume):
                labels.append("🧠 TOPLAMA")
                summary["TOPLAMA"].append(t)

            # AYRIŞMA
            if (not is_nan(xu100_change)) and (xu100_change <= -0.80) and (ch >= 0.40) and (t in top10_by_volume):
                labels.append("🧠 AYRIŞMA")
                summary["AYRISHMA"].append(t)

            # KÂR KORUMA (minimal v1)
            kar_koruma = False
            if (not is_nan(xu100_change)) and (ch >= 5.00) and (xu100_change <= -0.50):
                kar_koruma = True
            if (t in top10_by_volume) and (abs(ch) <= 0.20):
                kar_koruma = True

            if kar_koruma:
                labels.append("⚠️ KÂR KORUMA")
                summary["KAR_KORUMA"].append(t)

        # uniq (aynı etiket iki kez olmasın)
        uniq_labels = []
        seen = set()
        for lb in labels:
            if lb not in seen:
                uniq_labels.append(lb)
                seen.add(lb)

        r["signal"] = " | ".join(uniq_labels) if uniq_labels else ""
    return rows, summary

def make_table(rows: List[Dict[str, Any]], title: str, include_signal: bool = True) -> str:
    # Monospace tablo
    if include_signal:
        header = f"{'HİSSE':<7} {'GÜNLÜK%':>8} {'FİYAT':>10} {'HACİM':>10}  {'SİNYAL':<22}"
    else:
        header = f"{'HİSSE':<7} {'GÜNLÜK%':>8} {'FİYAT':>10} {'HACİM':>10}"
    sep = "-" * len(header)

    lines = [title, "<pre>", header, sep]
    for r in rows:
        t = r.get("ticker", "n/a")
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", None)
        sig = r.get("signal", "")

        ch_s = "n/a" if is_nan(ch) else f"{ch:+.2f}"
        cl_s = "n/a" if is_nan(cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        if include_signal:
            # sinyali biraz kısaltalım (çok uzarsa tablo kayar)
            sig_short = sig
            if len(sig_short) > 22:
                sig_short = sig_short[:21] + "…"
            lines.append(f"{t:<7} {ch_s:>8} {cl_s:>10} {vol_s:>10}  {sig_short:<22}")
        else:
            lines.append(f"{t:<7} {ch_s:>8} {cl_s:>10} {vol_s:>10}")

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
    XU100 için close + günlük değişim.
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

    close, xu100_change = get_xu100_summary()
    close_s = "n/a" if is_nan(close) else f"{close:,.2f}"
    change_s = "n/a" if is_nan(xu100_change) else f"{xu100_change:+.2f}%"

    # Verileri çek
    rows = build_rows_from_is_list(bist200_list)

    # Top10 hacim
    rows_with_vol = [r for r in rows if not is_nan(r.get("volume"))]
    top10_vol_rows = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]
    top10_set = set([r["ticker"] for r in top10_vol_rows if r.get("ticker")])

    # Sinyalleri hesapla (rows içine signal ekler)
    rows, summary = compute_signals(rows, xu100_change=xu100_change, top10_by_volume=top10_set)

    # İlk 20 (radar preview)
    first20 = rows[:20]

    # Özet mesaj
    msg1 = (
        "📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: <b>{close_s}</b>\n"
        f"• Günlük: <b>{change_s}</b>\n\n"
        "📡 Radar için:\n"
        "• /radar 1 … /radar 10\n\n"
        f"⚙️ Sürüm: <b>{VERSION}</b>"
    )
    await update.message.reply_text(msg1, parse_mode=ParseMode.HTML)

    # İlk 20 tablo (etiketli)
    await update.message.reply_text(
        make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>", include_signal=True),
        parse_mode=ParseMode.HTML
    )

    # Top 10 hacim tablo (etiketli)
    if top10_vol_rows:
        await update.message.reply_text(
            make_table(top10_vol_rows, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>", include_signal=True),
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("⚠️ Hacim verisi bulunamadı (TOP10 üretilemedi).")

    # Sinyal özeti (kısa)
    def fmt_list(xs: List[str]) -> str:
        xs = sorted(list(dict.fromkeys(xs)))  # uniq + stable
        if not xs:
            return "—"
        return ", ".join(xs[:15]) + (" …" if len(xs) > 15 else "")

    msg_sig = (
        "🧠 <b>Sinyal Özeti (v1.1)</b>\n"
        f"• 🧠 TOPLAMA: <b>{fmt_list(summary['TOPLAMA'])}</b>\n"
        f"• 🧠 AYRIŞMA: <b>{fmt_list(summary['AYRISHMA'])}</b>\n"
        f"• ⚠️ KÂR KORUMA: <b>{fmt_list(summary['KAR_KORUMA'])}</b>\n\n"
        "<i>Not: v1.1’de hacim/delta için Top10 hacim ranking kullanılır. (Stabil mod)</i>"
    )
    await update.message.reply_text(msg_sig, parse_mode=ParseMode.HTML)

async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /radar 1..10 => BIST200 listesi 20'lik paketler
    """
    logger.info("TAIPO_PRO_INTEL | RADAR request: %s", update.message.text)

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    # XU100 change'i al (AYRIŞMA için lazım)
    _, xu100_change = get_xu100_summary()

    # Arg parse
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

    # Bu parçayı çek
    part_list = chunks[n - 1]
    rows = build_rows_from_is_list(part_list)

    # Radar içinde de “top10” yerine “bu parça içindeki top5 hacim” kullan (stabil ve anlamlı)
    rows_with_vol = [r for r in rows if not is_nan(r.get("volume"))]
    top5 = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:5]
    top_set = set([r["ticker"] for r in top5 if r.get("ticker")])

    rows, _ = compute_signals(rows, xu100_change=xu100_change, top10_by_volume=top_set)

    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n(20 hisse)  |  ⚙️ {VERSION}"
    await update.message.reply_text(make_table(rows, title, include_signal=True), parse_mode=ParseMode.HTML)

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

    logger.info("Bot starting... (%s)", VERSION)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
