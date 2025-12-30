# main.py
# TAIPO PRO INTEL - TradingView scanner tabanlı stabil sürüm
# Komutlar: /ping, /eod, /radar <1-10>, /chatid
#
# ENV:
#   BOT_TOKEN=...
#   BIST200_TICKERS=THYAO.IS,ASELS.IS,AKBNK.IS,...
#   TAIPO_CHAT_ID=123456789   (otomatik EOD atılacak chat)
#   AUTO_EOD=1                (opsiyonel, default: 1)
#   LOG_LEVEL=INFO            (opsiyonel)

import os
import re
import math
import time
import logging
import datetime as dt
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

TR_TZ = ZoneInfo("Europe/Istanbul")

# -----------------------------
# Helpers
# -----------------------------
def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]

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

def is_nan(x: Any) -> bool:
    try:
        return math.isnan(float(x))
    except Exception:
        return True

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
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def clamp(x: float, lo: float, hi: float) -> float:
    if is_nan(x):
        return lo
    return max(lo, min(hi, x))

# -----------------------------
# Tables
# -----------------------------
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
    header = f"{'HİSSE':<8} {'SKOR':>4} {'GÜNL%':>7} {'FİYAT':>9} {'HACİM':>9}  NOT"
    sep = "-" * len(header)
    lines = [title, "<pre>", header, sep]
    for r in rows:
        t = r.get("ticker", "n/a")[:8]
        sc = r.get("score", 0)
        ch = r.get("change", float("nan"))
        cl = r.get("close", float("nan"))
        vol = r.get("volume", None)
        note = (r.get("note", "") or "")[:60]

        sc_s = f"{int(sc):>4}" if not is_nan(sc) else "   0"
        ch_s = "n/a" if is_nan(ch) else f"{ch:+.2f}"
        cl_s = "n/a" if is_nan(cl) else f"{cl:.2f}"
        vol_s = format_volume(vol)

        lines.append(f"{t:<8} {sc_s:>4} {ch_s:>7} {cl_s:>9} {vol_s:>9}  {note}")
    lines.append("</pre>")
    return "\n".join(lines)

# -----------------------------
# TradingView Scanner Client
# -----------------------------
TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12

DEFAULT_COLUMNS = [
    "close", "change", "volume",
    "open", "high", "low",
    "average_volume_10d_calc",
    "relative_volume_10d_calc",
]

def tv_scan_symbols(symbols: List[str], columns: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    """
    FIX: TradingView bazen item içinde 'symbol' alanını dönmüyor.
         Bu durumda data sırası, request tickers sırası ile aynı olur.
         -> index ile eşleştirip out map üretiyoruz.
    """
    if not symbols:
        return {}

    cols = columns or DEFAULT_COLUMNS
    payload = {"symbols": {"tickers": symbols}, "columns": cols}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tradingview.com/",
        "Origin": "https://www.tradingview.com",
    }

    for attempt in range(3):
        try:
            r = requests.post(TV_SCAN_URL, json=payload, headers=headers, timeout=TV_TIMEOUT)

            if r.status_code == 429:
                sleep_s = 1.5 * (attempt + 1)
                logger.warning("TradingView rate limit (429). Sleep %.1fs", sleep_s)
                time.sleep(sleep_s)
                continue

            if r.status_code != 200:
                logger.warning("TV status=%s body=%s", r.status_code, r.text[:200])
            r.raise_for_status()

            data = r.json()
            items = data.get("data", [])
            out: Dict[str, Dict[str, Any]] = {}

            # 1) symbol varsa normal
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

            # 2) symbol yoksa: index mapping
            if not out and items and len(items) == len(symbols):
                for idx, it in enumerate(items):
                    req_sym = symbols[idx]          # "BIST:ASELS"
                    short = req_sym.split(":")[-1].strip().upper()
                    d = it.get("d", [])
                    row: Dict[str, Any] = {}
                    for i, c in enumerate(cols):
                        row[c] = safe_float(d[i]) if i < len(d) else float("nan")
                    out[short] = row

            return out

        except Exception as e:
            logger.exception("TradingView scan error: %s", e)
            time.sleep(1.0 * (attempt + 1))

    return {}

def get_xu100_summary() -> Tuple[float, float]:
    m = tv_scan_symbols(["BIST:XU100"], columns=["close", "change"])
    d = m.get("XU100", {})
    return d.get("close", float("nan")), d.get("change", float("nan"))

def build_rows_from_is_list(is_list: List[str]) -> List[Dict[str, Any]]:
    tv_symbols = [normalize_is_ticker(t) for t in is_list if t.strip()]
    tv_map = tv_scan_symbols(tv_symbols)

    rows: List[Dict[str, Any]] = []
    for original in is_list:
        short = normalize_is_ticker(original).split(":")[-1]
        d = tv_map.get(short, {})

        base = {
            "ticker": short,
            "close": d.get("close", float("nan")),
            "change": d.get("change", float("nan")),
            "volume": d.get("volume", float("nan")),
            "open": d.get("open", float("nan")),
            "high": d.get("high", float("nan")),
            "low": d.get("low", float("nan")),
            "average_volume_10d_calc": d.get("average_volume_10d_calc", float("nan")),
            "relative_volume_10d_calc": d.get("relative_volume_10d_calc", float("nan")),
        }

        # map’te hiç yoksa nan’lar zaten dolu kalır
        if not d:
            base["close"] = float("nan")
            base["change"] = float("nan")
            base["volume"] = float("nan")
            base["open"] = float("nan")
            base["high"] = float("nan")
            base["low"] = float("nan")
            base["average_volume_10d_calc"] = float("nan")
            base["relative_volume_10d_calc"] = float("nan")

        rows.append(base)
    return rows

# -----------------------------
# TAIPO Metrics
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

# -----------------------------
# TAIPO SCORE (0-100)
# -----------------------------
def taipo_score(row: Dict[str, Any], xu_change: float) -> Tuple[int, str]:
    """
    Skor mantığı:
    - Momentum (change): pozitif bonus, sert negatif ceza
    - Hacim gücü (VR): yüksek VR bonus, düşük VR ceza (varsa)
    - Sessiz toplama: |change| küçük + VR yüksek -> büyük bonus
    - Üst fitil: yüksek üst fitil -> risk cezası
    - Endeks ayrışması: endeks↓ iken hisse↑ -> bonus
    """
    ch = safe_float(row.get("change"))
    vr = volume_ratio(row)
    uw = upper_wick_ratio(row)

    score = 50.0
    notes = []

    # 1) Momentum
    if not is_nan(ch):
        # +0..+3 arası -> 0..+18 bonus (lineer)
        # -0..-3 arası -> 0..-22 ceza
        if ch >= 0:
            bonus = clamp(ch / 3.0 * 18.0, 0, 18)
            score += bonus
            if bonus >= 8:
                notes.append("momentum+")
        else:
            pen = clamp(abs(ch) / 3.0 * 22.0, 0, 22)
            score -= pen
            if pen >= 10:
                notes.append("momentum-")

    # 2) Hacim gücü (VR)
    if not is_nan(vr):
        if vr >= 2.0:
            score += 18
            notes.append(f"VR{vr:.1f}x")
        elif vr >= 1.3:
            score += 10
            notes.append(f"VR{vr:.1f}x")
        elif vr <= 0.8:
            score -= 10
            notes.append("hacim zayıf")

    # 3) Sessiz toplama (en önemli)
    if (not is_nan(ch)) and (not is_nan(vr)):
        if abs(ch) <= 0.50 and vr >= 1.80:
            score += 22
            notes.append("sessiz toplama")

    # 4) Üst fitil risk cezası
    if not is_nan(uw):
        if uw >= 0.65:
            score -= 14
            notes.append("üst fitil↑")
        elif uw >= 0.50:
            score -= 8
            notes.append("fitil")

    # 5) Endeks ayrışması
    if not is_nan(xu_change) and not is_nan(ch):
        if xu_change <= -0.80 and ch >= 0.40:
            score += 12
            notes.append("endeks↓ hisse↑")

    score = clamp(score, 0, 100)
    return int(round(score)), " | ".join(notes)[:80]

def attach_scores(rows: List[Dict[str, Any]], xu_change: float) -> None:
    for r in rows:
        sc, note = taipo_score(r, xu_change)
        r["score"] = sc
        # not boşsa ekle; boşsa dokunma
        if note:
            r["note"] = note

# -----------------------------
# TAIPO Filters (EOD içinde) - eski modüller aynı
# -----------------------------
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

    def _vr(x): return 0 if is_nan(volume_ratio(x)) else volume_ratio(x)
    def _uw(x): return 0 if is_nan(upper_wick_ratio(x)) else upper_wick_ratio(x)

    out.sort(key=lambda x: (_vr(x), _uw(x)), reverse=True)
    return out[:10]

# -----------------------------
# Core sender (EOD)
# -----------------------------
async def send_eod(chat_id: int, bot, bist200_list: List[str]) -> None:
    close, xu_change = get_xu100_summary()
    close_s = "n/a" if is_nan(close) else f"{close:,.2f}"
    xu_change_s = "n/a" if is_nan(xu_change) else f"{xu_change:+.2f}%"

    rows = build_rows_from_is_list(bist200_list)

    # SCORE ekle
    attach_scores(rows, xu_change)

    first20 = rows[:20]

    rows_with_vol = [r for r in rows if not is_nan(r.get("volume"))]
    top10_vol = sorted(rows_with_vol, key=lambda x: safe_float(x.get("volume")), reverse=True)[:10]

    # Skor Top 20
    rows_with_score = [r for r in rows if not is_nan(r.get("score"))]
    top20_score = sorted(rows_with_score, key=lambda x: safe_float(x.get("score")), reverse=True)[:20]

    msg1 = (
        "📌 <b>BIST100 (XU100) Özet</b>\n"
        f"• Kapanış: <b>{close_s}</b>\n"
        f"• Günlük: <b>{xu_change_s}</b>\n\n"
        "📡 Radar için:\n"
        "• /radar 1 … /radar 10"
    )
    await bot.send_message(chat_id=chat_id, text=msg1, parse_mode=ParseMode.HTML)

    await bot.send_message(
        chat_id=chat_id,
        text=make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"),
        parse_mode=ParseMode.HTML
    )

    if top10_vol:
        await bot.send_message(
            chat_id=chat_id,
            text=make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"),
            parse_mode=ParseMode.HTML
        )

    if top20_score:
        await bot.send_message(
            chat_id=chat_id,
            text=make_table_reason(top20_score, "🏆 <b>TAIPO SKOR – TOP 20</b>\n<i>(0–100) momentum + hacim + sessiz toplama + risk</i>"),
            parse_mode=ParseMode.HTML
        )

    corr = select_correlation_trap(rows, xu_change)
    acc, fake = select_delta_thinking(rows)
    exit_warn = select_early_exit(rows, xu_change)

    if corr:
        await bot.send_message(
            chat_id=chat_id,
            text=make_table_reason(corr, "🧠 <b>KORELASYON (GİZLİ GÜÇ / AYRIŞMA)</b>\n<i>Endeks düşerken hisse + hacim artışı</i>"),
            parse_mode=ParseMode.HTML
        )
    else:
        await bot.send_message(chat_id=chat_id, text="🧠 <b>KORELASYON</b>: Bugün kriterlere uyan net aday yok.", parse_mode=ParseMode.HTML)

    if acc:
        await bot.send_message(
            chat_id=chat_id,
            text=make_table_reason(acc, "🧠 <b>DELTA THINKING — GÖRÜNMEYEN TOPLAMA</b>\n<i>Fiyat sabit/az oynuyor, hacim yükseliyor</i>"),
            parse_mode=ParseMode.HTML
        )
    else:
        await bot.send_message(chat_id=chat_id, text="🧠 <b>DELTA THINKING</b>: Sessiz toplama filtresi bugün boş.", parse_mode=ParseMode.HTML)

    if fake:
        await bot.send_message(
            chat_id=chat_id,
            text=make_table_reason(fake, "⚠️ <b>DELTA THINKING — SAHTE YÜKSELİŞ</b>\n<i>Fiyat ↑ ama hacim zayıf</i>"),
            parse_mode=ParseMode.HTML
        )

    if exit_warn:
        await bot.send_message(
            chat_id=chat_id,
            text=make_table_reason(exit_warn, "🚪 <b>ERKEN ÇIKIŞ (KÂR KORUMA / UZAK DUR)</b>\n<i>Hacim↑ fiyat→ / üst fitil / momentum yavaş</i>"),
            parse_mode=ParseMode.HTML
        )
    else:
        await bot.send_message(chat_id=chat_id, text="🚪 <b>ERKEN ÇIKIŞ</b>: Bugün acil uyarı üreten aday yok.", parse_mode=ParseMode.HTML)

# -----------------------------
# Telegram Handlers
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong! Bot ayakta.")

async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cid = update.effective_chat.id
    await update.message.reply_text(f"✅ Chat ID: <b>{cid}</b>\nBunu Render ENV: TAIPO_CHAT_ID olarak ekle.", parse_mode=ParseMode.HTML)

async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("TAIPO_PRO_INTEL | EOD request (manual)")

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    chat_id = update.effective_chat.id
    await send_eod(chat_id=chat_id, bot=context.bot, bist200_list=bist200_list)

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
    n = max(1, n)

    chunks = chunk_list(bist200_list, 20)
    total_parts = len(chunks)

    if n > total_parts:
        await update.message.reply_text(f"❌ /radar 1–{total_parts} arası. (Sen: {n})")
        return

    part_list = chunks[n - 1]
    rows = build_rows_from_is_list(part_list)

    # Skor da hesapla (radarda sadece not olarak kalsın istersen)
    _, xu_change = get_xu100_summary()
    attach_scores(rows, xu_change)

    title = f"📡 <b>BIST200 RADAR – Parça {n}/{total_parts}</b>\n(20 hisse)"
    await update.message.reply_text(make_table(rows, title), parse_mode=ParseMode.HTML)

# -----------------------------
# Scheduled Jobs (09:30 / 18:30)
# -----------------------------
async def job_auto_eod(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        bist200_list = env_csv("BIST200_TICKERS")
        if not bist200_list:
            logger.warning("AUTO_EOD skipped: BIST200_TICKERS empty")
            return

        chat_id_raw = os.getenv("TAIPO_CHAT_ID", "").strip()
        if not chat_id_raw:
            logger.warning("AUTO_EOD skipped: TAIPO_CHAT_ID missing")
            return

        chat_id = int(chat_id_raw)
        logger.info("AUTO_EOD sending to chat_id=%s", chat_id)
        await send_eod(chat_id=chat_id, bot=context.bot, bist200_list=bist200_list)
    except Exception as e:
        logger.exception("AUTO_EOD job failed: %s", e)

# -----------------------------
# Bot Commands (Telegram "/" menüsü)
# -----------------------------
async def post_init(app: Application) -> None:
    # menü komutları
    try:
        commands = [
            BotCommand("ping", "Bot ayakta mı kontrol"),
            BotCommand("eod", "BIST100 özet + radar + TAIPO tabloları"),
            BotCommand("radar", "BIST200 radar (ör: /radar 1)"),
            BotCommand("chatid", "Chat ID göster (AUTO_EOD için)"),
        ]
        await app.bot.set_my_commands(commands)
        logger.info("Bot commands registered.")
    except Exception as e:
        logger.warning("set_my_commands failed: %s", e)

    # otomatik cron
    auto = os.getenv("AUTO_EOD", "1").strip()
    if auto != "1":
        logger.info("AUTO_EOD disabled by env.")
        return

    # 09:30 ve 18:30 TR
    t1 = dt.time(hour=9, minute=30, tzinfo=TR_TZ)
    t2 = dt.time(hour=18, minute=30, tzinfo=TR_TZ)

    app.job_queue.run_daily(job_auto_eod, time=t1, name="auto_eod_0930")
    app.job_queue.run_daily(job_auto_eod, time=t2, name="auto_eod_1830")

    logger.info("AUTO_EOD scheduled at 09:30 and 18:30 (TR).")

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
    app.add_handler(CommandHandler("chatid", cmd_chatid))

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
