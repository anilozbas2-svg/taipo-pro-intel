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

import os
import re
import math
import time
import logging
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

def is_nan(x: Any) -> bool:
    try:
        return math.isnan(float(x))
    except Exception:
        return True

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

# -----------------------------
# TradingView Scanner Client
# -----------------------------
TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12

BULK_COLUMNS = ["close", "change", "volume"]
DETAIL_COLUMNS = [
    "close", "change", "volume",
    "open", "high", "low",
    "average_volume_10d_calc",
    "relative_volume_10d_calc",
]

def tv_scan_symbols(symbols: List[str], columns: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    """
    FIX v2:
    - TradingView bazen 'symbol' yerine 's' alanı döndürüyor.
    - Bazen symbol hiç dönmüyor -> index ile eşliyoruz.
    - Bazı günler response formatı değişebiliyor -> debug log basıyoruz.
    """
    if not symbols:
        return {}

    cols = columns or BULK_COLUMNS
    payload = {"symbols": {"tickers": symbols}, "columns": cols}

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tradingview.com/",
        "Origin": "https://www.tradingview.com",
        "Content-Type": "application/json",
    }

    def parse_items(items: list, used_cols: list) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}

        # 1) symbol/s varsa
        for it in items:
            sym = it.get("symbol") or it.get("s")  # <-- en kritik fix
            d = it.get("d", [])
            if not sym or not isinstance(d, list):
                continue
            # bazı durumlarda sym "ok" gibi gelebiliyor, onu atla
            if isinstance(sym, str) and sym.lower() == "ok":
                continue

            short = str(sym).split(":")[-1].strip().upper()
            row: Dict[str, Any] = {}
            for i, c in enumerate(used_cols):
                row[c] = safe_float(d[i]) if i < len(d) else float("nan")
            out[short] = row

        # 2) hala boşsa ve uzunluk uyuyorsa index eşle
        if not out and items and len(items) == len(symbols):
            for idx, it in enumerate(items):
                req_sym = symbols[idx]
                short = str(req_sym).split(":")[-1].strip().upper()
                d = it.get("d", [])
                row: Dict[str, Any] = {}
                for i, c in enumerate(used_cols):
                    row[c] = safe_float(d[i]) if i < len(d) else float("nan")
                out[short] = row

        return out

    def looks_broken(out: Dict[str, Dict[str, Any]]) -> bool:
        if not out:
            return True
        sample = list(out.values())[:20]
        if not sample:
            return True
        bad = 0
        for r in sample:
            c = r.get("close", float("nan"))
            ch = r.get("change", float("nan"))
            v = r.get("volume", float("nan"))
            if is_nan(c) and is_nan(ch) and is_nan(v):
                bad += 1
        return bad >= max(5, int(len(sample) * 0.6))

    for attempt in range(3):
        try:
            r = requests.post(TV_SCAN_URL, json=payload, headers=headers, timeout=TV_TIMEOUT)

            if r.status_code == 429:
                sleep_s = 1.5 * (attempt + 1)
                logger.warning("TV 429 rate limit. Sleep %.1fs", sleep_s)
                time.sleep(sleep_s)
                continue

            if r.status_code != 200:
                logger.warning("TV status=%s body=%s", r.status_code, r.text[:300])

            r.raise_for_status()
            data = r.json()
            items = data.get("data", [])

            # DEBUG (bugün bozulduysa burada yakalarız)
            if attempt == 0:
                sample_keys = list(items[0].keys()) if items else []
                logger.info("TV scan ok. items=%s first_keys=%s cols=%s", len(items), sample_keys, cols)

            out = parse_items(items, cols)

            # Eğer detail ile patlıyorsa bulk'a düş
            if cols != BULK_COLUMNS and looks_broken(out):
                logger.warning("TV scan looks broken with DETAIL. Falling back to BULK.")
                payload2 = {"symbols": {"tickers": symbols}, "columns": BULK_COLUMNS}
                r2 = requests.post(TV_SCAN_URL, json=payload2, headers=headers, timeout=TV_TIMEOUT)
                if r2.status_code != 200:
                    logger.warning("TV fallback status=%s body=%s", r2.status_code, r2.text[:300])
                r2.raise_for_status()
                data2 = r2.json()
                items2 = data2.get("data", [])
                out2 = parse_items(items2, BULK_COLUMNS)
                return out2

            return out

        except Exception as e:
            logger.exception("TradingView scan error: %s", e)
            time.sleep(1.0 * (attempt + 1))

    return {}

def get_xu100_summary() -> Tuple[float, float]:
    m = tv_scan_symbols(["BIST:XU100"], columns=["close", "change"])
    d = m.get("XU100", {})
    close = d.get("close", float("nan"))
    change = d.get("change", float("nan"))
    return close, change

def build_rows_from_is_list(is_list: List[str]) -> List[Dict[str, Any]]:
    """
    ✅ Stabil:
    - Toplu listeyi BULK ile çek (close/change/volume kesin dolsun)
    - Detay kolonlar: istersen daha sonra “adaylara özel” DETAIL tarama ile doldururuz.
    """
    tv_symbols = [normalize_is_ticker(t) for t in is_list if t.strip()]
    tv_map = tv_scan_symbols(tv_symbols, columns=BULK_COLUMNS)

    rows: List[Dict[str, Any]] = []
    for original in is_list:
        short = normalize_is_ticker(original).split(":")[-1]
        d = tv_map.get(short, {})

        rows.append({
            "ticker": short,
            "close": d.get("close", float("nan")),
            "change": d.get("change", float("nan")),
            "volume": d.get("volume", float("nan")),
            # detay (şimdilik boş)
            "open": float("nan"),
            "high": float("nan"),
            "low": float("nan"),
            "average_volume_10d_calc": float("nan"),
            "relative_volume_10d_calc": float("nan"),
        })

    # DEBUG: ilk 3 hisse gerçekten dolmuş mu?
    if rows:
        logger.info("Rows built. sample=%s", rows[:3])

    return rows

# -----------------------------
# TAIPO Filtreleri (EOD içinde)
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
# Telegram Handlers
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong! Bot ayakta.")

async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("TAIPO_PRO_INTEL | EOD request")

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    close, xu_change = get_xu100_summary()
    close_s = "n/a" if is_nan(close) else f"{close:,.2f}"
    xu_change_s = "n/a" if is_nan(xu_change) else f"{xu_change:+.2f}%"

    rows = build_rows_from_is_list(bist200_list)
    first20 = rows[:20]

    rows_with_vol = [r for r in rows if not is_nan(r.get("volume"))]
    top10_vol = sorted(rows_with_vol, key=lambda x: safe_float(x.get("volume")), reverse=True)[:10]

    msg1 = (
        "📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: <b>{close_s}</b>\n"
        f"• Günlük: <b>{xu_change_s}</b>\n\n"
        "📡 Radar için:\n"
        "• /radar 1 … /radar 10"
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

    corr = select_correlation_trap(rows, xu_change)
    acc, fake = select_delta_thinking(rows)
    exit_warn = select_early_exit(rows, xu_change)

    if corr:
        await update.message.reply_text(
            make_table_reason(corr, "🧠 <b>KORELASYON (GİZLİ GÜÇ / AYRIŞMA)</b>\n<i>Endeks düşerken hisse + hacim artışı</i>"),
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("🧠 <b>KORELASYON</b>: Bugün kriterlere uyan net aday yok.", parse_mode=ParseMode.HTML)

    if acc:
        await update.message.reply_text(
            make_table_reason(acc, "🧠 <b>DELTA THINKING — GÖRÜNMEYEN TOPLAMA</b>\n<i>Fiyat sabit/az oynuyor, hacim yükseliyor</i>"),
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("🧠 <b>DELTA THINKING</b>: Sessiz toplama filtresi bugün boş.", parse_mode=ParseMode.HTML)

    if fake:
        await update.message.reply_text(
            make_table_reason(fake, "⚠️ <b>DELTA THINKING — SAHTE YÜKSELİŞ</b>\n<i>Fiyat ↑ ama hacim zayıf</i>"),
            parse_mode=ParseMode.HTML
        )

    if exit_warn:
        await update.message.reply_text(
            make_table_reason(exit_warn, "🚪 <b>ERKEN ÇIKIŞ (KÂR KORUMA / UZAK DUR)</b>\n<i>Hacim↑ fiyat→ / üst fitil / momentum yavaş</i>"),
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("🚪 <b>ERKEN ÇIKIŞ</b>: Bugün acil uyarı üreten aday yok.", parse_mode=ParseMode.HTML)

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
# Bot Commands (Telegram "/" menüsü)
# -----------------------------
async def post_init(app: Application) -> None:
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
