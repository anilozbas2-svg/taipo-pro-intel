# main.py
# TAIPO PRO INTEL - Stabil sürüm
# Komutlar: /ping, /eod, /radar <1-10>
#
# ENV:
#   BOT_TOKEN=...
#   BIST200_TICKERS=THYAO.IS,ASELS.IS,AKBNK.IS,...
#   ADMIN_CHAT_ID=123456789   (otomatik 09:30 & 18:30 EOD için)
#   LOG_LEVEL=INFO (opsiyonel)

import os
import re
import math
import time
import json
import logging
import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Tuple, Optional

import requests
from telegram import Update, BotCommand
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

# -----------------------------
# Helpers
# -----------------------------
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
    header = f"{'HİSSE ADI':<10} {'GÜNLÜK %':>9} {'FİYAT':>10} {'HACİM':>10}"
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

def make_table_reason(rows: List[Dict[str, Any]], title: str) -> str:
    header = f"{'HİSSE ADI':<10} {'GÜNLÜK %':>9} {'FİYAT':>10} {'HACİM':>10}  NOT"
    sep = "-" * len(header)
    lines = [title, "<pre>", header, sep]
    for r in rows:
        t = r.get("ticker", "n/a")
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", None)
        note = r.get("note", "")

        ch_s = "n/a" if is_nan(ch) else f"{ch:+.2f}"
        cl_s = "n/a" if is_nan(cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        lines.append(f"{t:<10} {ch_s:>9} {cl_s:>10} {vol_s:>10}  {note}")
    lines.append("</pre>")
    return "\n".join(lines)

def to_yahoo_symbol(t: str) -> str:
    t = t.strip().upper()
    if not t:
        return t
    # zaten THYAO.IS formatındaysa dokunma
    if t.endswith(".IS"):
        return t
    # BIST:ASELS gibi geldiyse
    if t.startswith("BIST:"):
        t = t.split(":", 1)[1]
    return f"{t}.IS"

# -----------------------------
# TradingView Scanner (primary)
# -----------------------------
TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12

DEFAULT_COLUMNS = [
    "close", "change", "volume",
    "open", "high", "low",
    "average_volume_10d_calc",
    "relative_volume_10d_calc",
]

TV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
}

def normalize_tv_ticker(t: str) -> str:
    t = t.strip().upper()
    if not t:
        return t
    if t.startswith("BIST:"):
        base = t.split(":", 1)[1]
    else:
        base = t
    if base.endswith(".IS"):
        base = base[:-3]
    return f"BIST:{base}"

def tv_scan_symbols(symbols: List[str], columns: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}

    cols = columns or DEFAULT_COLUMNS
    payload = {"symbols": {"tickers": symbols}, "columns": cols}

    s = requests.Session()
    for attempt in range(3):
        try:
            r = s.post(TV_SCAN_URL, data=json.dumps(payload), headers=TV_HEADERS, timeout=TV_TIMEOUT)
            if r.status_code == 429:
                sleep_s = 1.5 * (attempt + 1)
                logger.warning("TradingView rate limit (429). Sleep %.1fs", sleep_s)
                time.sleep(sleep_s)
                continue

            # burada “boş dönme”yi loglayalım
            if r.status_code != 200:
                logger.warning("TradingView status=%s body_head=%s", r.status_code, r.text[:160])
                r.raise_for_status()

            data = r.json()
            items = data.get("data", [])
            if not items:
                logger.warning("TradingView empty data. body_head=%s", str(data)[:160])
                return {}

            out: Dict[str, Dict[str, Any]] = {}
            for it in items:
                sym = it.get("symbol")
                d = it.get("d", [])
                if not sym or not isinstance(d, list):
                    continue
                short = sym.split(":")[-1].strip().upper()
                row: Dict[str, Any] = {}
                for i, c in enumerate(cols):
                    row[c] = safe_float(d[i]) if i < len(d) else float("nan")
                out[short] = row
            return out

        except Exception as e:
            logger.exception("TradingView scan error: %s", e)
            time.sleep(1.0 * (attempt + 1))
    return {}

# -----------------------------
# Yahoo Finance (fallback)
# -----------------------------
YH_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

def yahoo_quote_map(yahoo_symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not yahoo_symbols:
        return {}
    # tek çağrıda çek
    params = {"symbols": ",".join(yahoo_symbols)}
    headers = {"User-Agent": TV_HEADERS["User-Agent"], "Accept": "application/json"}
    try:
        r = requests.get(YH_URL, params=params, headers=headers, timeout=12)
        if r.status_code != 200:
            logger.warning("Yahoo status=%s body_head=%s", r.status_code, r.text[:160])
            return {}
        js = r.json()
        res = js.get("quoteResponse", {}).get("result", []) or []
        out: Dict[str, Dict[str, Any]] = {}
        for it in res:
            sym = (it.get("symbol") or "").upper()
            if not sym:
                continue
            out[sym] = {
                "close": safe_float(it.get("regularMarketPrice")),
                "change": safe_float(it.get("regularMarketChangePercent")),
                "volume": safe_float(it.get("regularMarketVolume")),
            }
        return out
    except Exception as e:
        logger.exception("Yahoo fetch error: %s", e)
        return {}

def get_xu100_summary() -> Tuple[float, float]:
    # önce TradingView
    m = tv_scan_symbols([normalize_tv_ticker("XU100")], columns=["close", "change"])
    d = m.get("XU100", {})
    close = d.get("close", float("nan"))
    change = d.get("change", float("nan"))
    if not is_nan(close) and not is_nan(change):
        return close, change

    # fallback Yahoo: BIST100 için farklı sembol ihtimali var, birkaç deniyoruz
    candidates = ["XU100.IS", "^XU100", "^XU100.IS", "XU100.TI"]
    y = yahoo_quote_map(candidates)
    for c in candidates:
        dd = y.get(c.upper(), {})
        cl = dd.get("close", float("nan"))
        ch = dd.get("change", float("nan"))
        if not is_nan(cl) and not is_nan(ch):
            return cl, ch

    return float("nan"), float("nan")

def build_rows_from_is_list(is_list: List[str]) -> List[Dict[str, Any]]:
    # 1) TradingView dene
    tv_symbols = [normalize_tv_ticker(t) for t in is_list if t.strip()]
    tv_map = tv_scan_symbols(tv_symbols)

    rows: List[Dict[str, Any]] = []
    if tv_map:
        for original in is_list:
            short = normalize_tv_ticker(original).split(":")[-1]
            d = tv_map.get(short, {})
            rows.append({
                "ticker": short,
                "close": d.get("close", float("nan")),
                "change": d.get("change", float("nan")),
                "volume": d.get("volume", float("nan")),
                "open": d.get("open", float("nan")),
                "high": d.get("high", float("nan")),
                "low": d.get("low", float("nan")),
                "average_volume_10d_calc": d.get("average_volume_10d_calc", float("nan")),
                "relative_volume_10d_calc": d.get("relative_volume_10d_calc", float("nan")),
            })
        return rows

    # 2) Fallback Yahoo
    yahoo_syms = [to_yahoo_symbol(t) for t in is_list]
    ymap = yahoo_quote_map(yahoo_syms)

    for original in is_list:
        ys = to_yahoo_symbol(original).upper()
        short = normalize_tv_ticker(original).split(":")[-1]
        d = ymap.get(ys, {})
        rows.append({
            "ticker": short,
            "close": d.get("close", float("nan")),
            "change": d.get("change", float("nan")),
            "volume": d.get("volume", float("nan")),
            "open": float("nan"),
            "high": float("nan"),
            "low": float("nan"),
            "average_volume_10d_calc": float("nan"),
            "relative_volume_10d_calc": float("nan"),
        })
    return rows

# -----------------------------
# TAIPO Filtreleri
# -----------------------------
def volume_ratio(row: Dict[str, Any]) -> float:
    vol = safe_float(row.get("volume"))
    avg = safe_float(row.get("average_volume_10d_calc"))
    if is_nan(vol) or is_nan(avg) or avg == 0:
        return float("nan")
    return vol / avg

def upper_wick_ratio(row: Dict[str, Any]) -> float:
    o = safe_float(row.get("open"))
    h = safe_float(row.get("high"))
    l = safe_float(row.get("low"))
    c = safe_float(row.get("close"))
    if any(is_nan(x) for x in [o, h, l, c]) or (h - l) == 0:
        return float("nan")
    return (h - max(o, c)) / (h - l)

def select_correlation_trap(rows: List[Dict[str, Any]], xu_change: float) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        ch = safe_float(r.get("change"))
        vr = volume_ratio(r)
        if is_nan(ch) or is_nan(xu_change):
            continue
        if xu_change <= -0.80 and ch >= 0.40:
            if (not is_nan(vr) and vr >= 1.20) or is_nan(vr):
                rr = dict(r)
                note = "GİZLİ GÜÇ (endeks↓ hisse↑)"
                if not is_nan(vr):
                    note += f" | VR:{vr:.2f}x"
                rr["note"] = note
                out.append(rr)
    out.sort(key=lambda x: (safe_float(x.get("change")), safe_float(x.get("volume"))), reverse=True)
    return out[:10]

def select_delta_thinking(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    accumulation = []
    fake = []
    for r in rows:
        ch = safe_float(r.get("change"))
        vr = volume_ratio(r)
        if is_nan(ch):
            continue
        if abs(ch) <= 0.50 and (not is_nan(vr) and vr >= 1.80):
            rr = dict(r)
            rr["note"] = f"GÖRÜNMEYEN TOPLAMA | VR:{vr:.2f}x"
            accumulation.append(rr)
        if ch >= 0.80 and (not is_nan(vr) and vr <= 0.85):
            rr = dict(r)
            rr["note"] = f"SAHTE YÜKSELİŞ | VR:{vr:.2f}x"
            fake.append(rr)
    accumulation.sort(key=lambda x: volume_ratio(x), reverse=True)
    fake.sort(key=lambda x: safe_float(x.get("change")), reverse=True)
    return accumulation[:10], fake[:10]

def select_early_exit(rows: List[Dict[str, Any]], xu_change: float) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        ch = safe_float(r.get("change"))
        vr = volume_ratio(r)
        uw = upper_wick_ratio(r)
        if is_nan(ch):
            continue

        reasons = []
        if (not is_nan(vr) and vr >= 2.00) and abs(ch) <= 0.40:
            reasons.append(f"HACİM↑ FİYAT→ (VR:{vr:.2f}x)")
        if not is_nan(uw) and uw >= 0.60 and (not is_nan(vr) and vr >= 1.30):
            reasons.append(f"ÜST FİTİL↑ (uw:{uw:.2f})")
        if (not is_nan(xu_change) and xu_change < 0) and abs(ch) <= 0.30 and (not is_nan(vr) and vr >= 1.50):
            reasons.append("ENDEKS↓ + MOMENTUM YAVAŞ")

        if reasons:
            rr = dict(r)
            rr["note"] = " | ".join(reasons)[:120]
            out.append(rr)

    out.sort(key=lambda x: (
        0 if is_nan(volume_ratio(x)) else volume_ratio(x),
        0 if is_nan(upper_wick_ratio(x)) else upper_wick_ratio(x)
    ), reverse=True)
    return out[:10]

# -----------------------------
# EOD build (command + cron ortak)
# -----------------------------
def build_eod_payload() -> List[Tuple[str, str]]:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return [("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.", ParseMode.HTML)]

    close, xu_change = get_xu100_summary()
    close_s = "n/a" if is_nan(close) else f"{close:,.2f}"
    xu_change_s = "n/a" if is_nan(xu_change) else f"{xu_change:+.2f}%"

    rows = build_rows_from_is_list(bist200_list)
    first20 = rows[:20]

    rows_with_vol = [r for r in rows if not is_nan(r.get("volume"))]
    top10_vol = sorted(rows_with_vol, key=lambda x: safe_float(x.get("volume")), reverse=True)[:10]

    messages: List[Tuple[str, str]] = []

    msg1 = (
        "📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: <b>{close_s}</b>\n"
        f"• Günlük: <b>{xu_change_s}</b>\n\n"
        "📡 Radar için:\n"
        "• /radar 1 … /radar 10"
    )
    messages.append((msg1, ParseMode.HTML))
    messages.append((make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"), ParseMode.HTML))

    if top10_vol:
        messages.append((make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"), ParseMode.HTML))

    corr = select_correlation_trap(rows, xu_change)
    acc, fake = select_delta_thinking(rows)
    exit_warn = select_early_exit(rows, xu_change)

    if corr:
        messages.append((make_table_reason(corr, "🧠 <b>KORELASYON (GİZLİ GÜÇ / AYRIŞMA)</b>\n<i>Endeks düşerken hisse + hacim artışı</i>"), ParseMode.HTML))
    else:
        messages.append(("🧠 <b>KORELASYON</b>: Bugün kriterlere uyan net aday yok.", ParseMode.HTML))

    if acc:
        messages.append((make_table_reason(acc, "🧠 <b>DELTA THINKING — GÖRÜNMEYEN TOPLAMA</b>\n<i>Fiyat sabit/az oynuyor, hacim yükseliyor</i>"), ParseMode.HTML))
    else:
        messages.append(("🧠 <b>DELTA THINKING</b>: Sessiz toplama filtresi bugün boş.", ParseMode.HTML))

    if fake:
        messages.append((make_table_reason(fake, "⚠️ <b>DELTA THINKING — SAHTE YÜKSELİŞ</b>\n<i>Fiyat ↑ ama hacim zayıf</i>"), ParseMode.HTML))

    if exit_warn:
        messages.append((make_table_reason(exit_warn, "🚪 <b>ERKEN ÇIKIŞ (KÂR KORUMA / UZAK DUR)</b>\n<i>Hacim↑ fiyat→ / üst fitil / momentum yavaş</i>"), ParseMode.HTML))
    else:
        messages.append(("🚪 <b>ERKEN ÇIKIŞ</b>: Bugün acil uyarı üreten aday yok.", ParseMode.HTML))

    return messages

# -----------------------------
# Telegram Handlers
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong! Bot ayakta.")

async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("TAIPO_PRO_INTEL | EOD request")
    for text, pm in build_eod_payload():
        await update.message.reply_text(text, parse_mode=pm)

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
# Cron job (09:30 & 18:30)
# -----------------------------
async def job_send_eod(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
    if not chat_id:
        return
    try:
        for text, pm in build_eod_payload():
            await context.bot.send_message(chat_id=int(chat_id), text=text, parse_mode=pm)
    except Exception as e:
        logger.exception("job_send_eod error: %s", e)

# -----------------------------
# Bot Commands + post_init
# -----------------------------
async def post_init(app: Application) -> None:
    # "/" menüsü
    try:
        commands = [
            BotCommand("ping", "Bot ayakta mı kontrol"),
            BotCommand("eod", "BIST100 özet + radar + TAIPO tabloları"),
            BotCommand("radar", "BIST200 radar (ör: /radar 1)"),
        ]
        await app.bot.set_my_commands(commands)
        logger.info("Bot commands registered.")
    except Exception as e:
        logger.warning("set_my_commands failed: %s", e)

    # cron (ADMIN_CHAT_ID varsa)
    chat_id = os.getenv("ADMIN_CHAT_ID", "").strip()
    if chat_id:
        tz = ZoneInfo("Europe/Istanbul")
        t1 = datetime.time(hour=9, minute=30, tzinfo=tz)
        t2 = datetime.time(hour=18, minute=30, tzinfo=tz)
        app.job_queue.run_daily(job_send_eod, time=t1, name="eod_0930")
        app.job_queue.run_daily(job_send_eod, time=t2, name="eod_1830")
        logger.info("Cron scheduled: 09:30 & 18:30 (Europe/Istanbul) to ADMIN_CHAT_ID=%s", chat_id)

# -----------------------------
# Main
# -----------------------------
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
