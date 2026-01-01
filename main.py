import os
import re
import math
import time
import logging
import asyncio
import sqlite3
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# -----------------------------
# Config
# -----------------------------
BOT_VERSION = os.getenv("BOT_VERSION", "v1.3.7-hybrid").strip() or "v1.3.7-hybrid"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TAIPO_PRO_INTEL")

TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12

TZ = ZoneInfo("Europe/Istanbul")

# Alarm ayarları
ALARM_COOLDOWN_SEC = 60 * 60  # 60 dk
ALARM_ALLOWED = {"TOPLAMA", "DİP TOPLAMA"}  # sadece bunlar

# Job frekansları
ALARM_SCAN_EVERY_SEC = int(os.getenv("ALARM_SCAN_EVERY_SEC", "300"))       # 5 dk
REPORT_SCAN_EVERY_SEC = int(os.getenv("REPORT_SCAN_EVERY_SEC", "3600"))    # 60 dk

# Kapanış raporu saati (TR)
CLOSE_HOUR = int(os.getenv("CLOSE_HOUR", "17"))
CLOSE_MINUTE = int(os.getenv("CLOSE_MINUTE", "50"))

# DB
SQLITE_PATH = os.getenv("SQLITE_PATH", "taipo_pro_intel.db")


# -----------------------------
# Helpers
# -----------------------------
def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def normalize_is_ticker(t: str) -> str:
    t = (t or "").strip().upper()
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


def clean_ticker(arg: str) -> str:
    a = (arg or "").strip().upper()
    a = re.sub(r"[^A-Z0-9\.\:]", "", a)
    if not a:
        return ""
    if a.startswith("BIST:"):
        a = a.replace("BIST:", "")
    if a.endswith(".IS"):
        a = a[:-3]
    return a


# ✅ HACİM KISA FORMAT (wrap engeller)
def format_volume(v: Any) -> str:
    try:
        n = float(v)
    except Exception:
        return "n/a"
    absn = abs(n)

    if absn >= 1_000_000_000:
        s = f"{n/1_000_000_000:.1f}B"
        return s.replace(".0B", "B")
    if absn >= 1_000_000:
        return f"{n/1_000_000:.0f}M"
    if absn >= 1_000:
        return f"{n/1_000:.0f}K"
    return f"{n:.0f}"


def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def now_tr() -> datetime:
    return datetime.now(TZ)


def is_market_hours_tr(dt: Optional[datetime] = None) -> bool:
    """Basit seans filtresi: 10:00 - 18:00 arası."""
    dt = dt or now_tr()
    t = dt.time()
    return dtime(10, 0) <= t <= dtime(18, 0)


# -----------------------------
# SQLite (persist)
# -----------------------------
def db_conn() -> sqlite3.Connection:
    con = sqlite3.connect(SQLITE_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def db_init() -> None:
    con = db_conn()
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS subs (
              chat_id INTEGER PRIMARY KEY,
              alarm_enabled INTEGER DEFAULT 0,
              report_enabled INTEGER DEFAULT 1
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist (
              chat_id INTEGER,
              ticker TEXT,
              PRIMARY KEY (chat_id, ticker)
            );
            """
        )
        con.commit()
    finally:
        con.close()


def db_ensure_sub(chat_id: int) -> None:
    con = db_conn()
    try:
        con.execute("INSERT OR IGNORE INTO subs(chat_id, alarm_enabled, report_enabled) VALUES(?,0,1);", (chat_id,))
        con.commit()
    finally:
        con.close()


def db_set_alarm(chat_id: int, enabled: bool) -> None:
    db_ensure_sub(chat_id)
    con = db_conn()
    try:
        con.execute("UPDATE subs SET alarm_enabled=? WHERE chat_id=?;", (1 if enabled else 0, chat_id))
        con.commit()
    finally:
        con.close()


def db_set_report(chat_id: int, enabled: bool) -> None:
    db_ensure_sub(chat_id)
    con = db_conn()
    try:
        con.execute("UPDATE subs SET report_enabled=? WHERE chat_id=?;", (1 if enabled else 0, chat_id))
        con.commit()
    finally:
        con.close()


def db_get_subs(flag: str) -> List[int]:
    if flag not in ("alarm_enabled", "report_enabled"):
        return []
    con = db_conn()
    try:
        cur = con.execute(f"SELECT chat_id FROM subs WHERE {flag}=1;")
        return [int(r[0]) for r in cur.fetchall()]
    finally:
        con.close()


def db_get_watchlist(chat_id: int) -> List[str]:
    con = db_conn()
    try:
        cur = con.execute("SELECT ticker FROM watchlist WHERE chat_id=? ORDER BY ticker ASC;", (chat_id,))
        return [str(r[0]) for r in cur.fetchall()]
    finally:
        con.close()


def db_watch_add(chat_id: int, ticker: str) -> bool:
    t = clean_ticker(ticker)
    if not t:
        return False
    db_ensure_sub(chat_id)
    con = db_conn()
    try:
        con.execute("INSERT OR IGNORE INTO watchlist(chat_id, ticker) VALUES(?,?);", (chat_id, t))
        con.commit()
        return True
    finally:
        con.close()


def db_watch_del(chat_id: int, ticker: str) -> bool:
    t = clean_ticker(ticker)
    if not t:
        return False
    con = db_conn()
    try:
        cur = con.execute("DELETE FROM watchlist WHERE chat_id=? AND ticker=?;", (chat_id, t))
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def db_watch_set(chat_id: int, tickers: List[str]) -> int:
    cleaned = []
    for x in tickers:
        t = clean_ticker(x)
        if t:
            cleaned.append(t)
    cleaned = sorted(set(cleaned))
    db_ensure_sub(chat_id)
    con = db_conn()
    try:
        con.execute("DELETE FROM watchlist WHERE chat_id=?;", (chat_id,))
        for t in cleaned:
            con.execute("INSERT OR IGNORE INTO watchlist(chat_id, ticker) VALUES(?,?);", (chat_id, t))
        con.commit()
        return len(cleaned)
    finally:
        con.close()


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
# Signal System (Hybrid)
# -----------------------------
def compute_signal_rows(rows: List[Dict[str, Any]], xu100_change: float) -> float:
    """
    Hybrid:
    - Top10 hacim eşiği (Top10'un 10. sırası)
    - TOPLAMA: Top10 + 0.00 .. +0.60 -> 🧠
    - DİP TOPLAMA: Top10 + -0.60 .. -0.01 -> 🧲
    - AYRIŞMA: XU100 <= -0.80 iken hisse >= +0.40 + Top10 -> 🧠
    - KÂR KORUMA: hisse >= +4.00 -> ⚠️
    Returns: top10_min_vol
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

        if in_top10 and (0.00 <= ch <= 0.60):
            r["signal"] = "🧠"
            r["signal_text"] = "TOPLAMA"
            continue

        if in_top10 and (-0.60 <= ch < 0.00):
            r["signal"] = "🧲"
            r["signal_text"] = "DİP TOPLAMA"
            continue

        r["signal"] = "-"
        r["signal_text"] = ""

    return float(top10_min_vol)


def _apply_signals_with_threshold(rows: List[Dict[str, Any]], xu100_change: float, top10_min_vol: float) -> None:
    """Watchlist için: BIST200 top10 eşiğini kullanarak sinyalleri uygula."""
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

        if in_top10 and (0.00 <= ch <= 0.60):
            r["signal"] = "🧠"
            r["signal_text"] = "TOPLAMA"
            continue

        if in_top10 and (-0.60 <= ch < 0.00):
            r["signal"] = "🧲"
            r["signal_text"] = "DİP TOPLAMA"
            continue

        r["signal"] = "-"
        r["signal_text"] = ""


# -----------------------------
# Table view (compact, wrap-safe)
# -----------------------------
def make_table(rows: List[Dict[str, Any]], title: str) -> str:
    header = f"{'HİSSE':<6} {'S':<2} {'GÜNLÜK%':>7} {'FİYAT':>8} {'HACİM':>7}"
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

        lines.append(f"{t:<6} {sig:<2} {ch_s:>7} {cl_s:>8} {vol_s:>7}")

    lines.append("</pre>")
    return "\n".join(lines)


def pick_candidates(rows: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
    cand = [r for r in rows if r.get("signal_text") == kind]
    return sorted(
        cand,
        key=lambda x: (x.get("volume") or 0) if (x.get("volume") == x.get("volume")) else 0,
        reverse=True
    )


def signal_summary_compact(rows: List[Dict[str, Any]]) -> str:
    def join(lst: List[str]) -> str:
        return ", ".join(lst) if lst else "—"

    toplama = [r["ticker"] for r in rows if r.get("signal_text") == "TOPLAMA"]
    dip = [r["ticker"] for r in rows if r.get("signal_text") == "DİP TOPLAMA"]
    ayrisma = [r["ticker"] for r in rows if r.get("signal_text") == "AYRIŞMA"]
    kar = [r["ticker"] for r in rows if r.get("signal_text") == "KÂR KORUMA"]

    return (
        f"🧠 <b>Sinyal Özeti ({BOT_VERSION})</b>\n"
        f"• 🧠 TOPLAMA: {join(toplama)}\n"
        f"• 🧲 DİP TOPLAMA: {join(dip)}\n"
        f"• 🧠 AYRIŞMA: {join(ayrisma)}\n"
        f"• ⚠️ KÂR KORUMA: {join(kar)}"
    )


def format_top10_threshold(min_vol: float) -> str:
    if not isinstance(min_vol, (int, float)) or math.isnan(min_vol) or min_vol == float("inf"):
        return "n/a"
    return format_volume(min_vol)


async def get_bist200_threshold(xu_change: float) -> Tuple[float, List[Dict[str, Any]]]:
    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        return float("nan"), []
    rows = await build_rows_from_is_list(bist200_list)
    top10_min_vol = compute_signal_rows(rows, xu_change)
    return top10_min_vol, rows


# -----------------------------
# Telegram Commands
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_ensure_sub(chat_id)
    await update.message.reply_text(f"🏓 Pong! ({BOT_VERSION})")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
        "🧭 <b>Komutlar</b>\n"
        "• /ping\n"
        "• /eod  → anlık rapor\n"
        "• /radar 1  → BIST200 parça radar\n"
        "• /watch → watchlist tablo\n"
        "• /watch_set AKBNK,CANTE,EREGL\n"
        "• /watch_add SASA\n"
        "• /watch_del SASA\n"
        "• /alarm_on  /alarm_off\n"
        "• /report_on /report_off\n"
        f"\n⚙️ <b>Sürüm</b>: {BOT_VERSION}"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_ensure_sub(chat_id)

    bist200_list = env_csv("BIST200_TICKERS")
    if not bist200_list:
        await update.message.reply_text("❌ BIST200_TICKERS env boş. Render → Environment’a ekle.")
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    xu_close, xu_change = await get_xu100_summary()
    rows = await build_rows_from_is_list(bist200_list)
    top10_min_vol = compute_signal_rows(rows, xu_change)

    first20 = rows[:20]
    rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
    top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]

    # 0) Kriter
    await update.message.reply_text(
        f"🧱 <b>Kriter</b>: Top10 hacim eşiği ≥ <b>{format_top10_threshold(top10_min_vol)}</b>",
        parse_mode=ParseMode.HTML
    )

    # 1) İlk 20
    await update.message.reply_text(
        make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"),
        parse_mode=ParseMode.HTML
    )

    # 2) Top10 hacim
    if top10_vol:
        await update.message.reply_text(
            make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"),
            parse_mode=ParseMode.HTML
        )

    # 3) Adaylar
    toplama_cand = pick_candidates(rows, "TOPLAMA")
    dip_cand = pick_candidates(rows, "DİP TOPLAMA")

    await update.message.reply_text(
        make_table(toplama_cand, "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>") if toplama_cand
        else "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>\n—",
        parse_mode=ParseMode.HTML
    )

    await update.message.reply_text(
        make_table(dip_cand, "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>") if dip_cand
        else "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>\n—",
        parse_mode=ParseMode.HTML
    )

    # 4) Özet
    await update.message.reply_text(signal_summary_compact(rows), parse_mode=ParseMode.HTML)

    # 5) XU100 line
    xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
    xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"
    await update.message.reply_text(
        f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}",
        parse_mode=ParseMode.HTML
    )


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_ensure_sub(chat_id)

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
    await update.message.reply_text(make_table(rows, title), parse_mode=ParseMode.HTML)


# ✅ /watch -> DB watchlist (chat bazlı)
async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_ensure_sub(chat_id)

    watch = db_get_watchlist(chat_id)
    if not watch:
        await update.message.reply_text(
            "❌ Watchlist boş.\nÖrnek: /watch_set AKBNK,CANTE,EREGL",
            parse_mode=ParseMode.HTML
        )
        return

    await update.message.reply_text("⏳ Veriler çekiliyor...")

    _, xu_change = await get_xu100_summary()

    rows = await build_rows_from_is_list(watch)

    # threshold: BIST200 varsa onu baz al
    top10_min_vol, _all_rows = await get_bist200_threshold(xu_change)
    if (top10_min_vol == top10_min_vol) and (not math.isnan(top10_min_vol)) and top10_min_vol != float("inf"):
        _apply_signals_with_threshold(rows, xu_change, top10_min_vol)
        thresh_s = format_top10_threshold(top10_min_vol)
    else:
        tmp = compute_signal_rows(rows, xu_change)
        thresh_s = format_top10_threshold(tmp)

    await update.message.reply_text(
        f"👀 <b>WATCHLIST</b> (Top10 hacim eşiği ≥ <b>{thresh_s}</b>)\n"
        f"• Liste: <b>{', '.join(watch)}</b>",
        parse_mode=ParseMode.HTML
    )
    await update.message.reply_text(make_table(rows, "📌 <b>Watchlist Radar</b>"), parse_mode=ParseMode.HTML)


async def cmd_watch_set(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_ensure_sub(chat_id)

    if not context.args:
        await update.message.reply_text("Kullanım: /watch_set AKBNK,CANTE,EREGL", parse_mode=ParseMode.HTML)
        return

    raw = " ".join(context.args).strip()
    parts = [p.strip() for p in re.split(r"[,\s]+", raw) if p.strip()]
    count = db_watch_set(chat_id, parts)
    await update.message.reply_text(f"✅ Watchlist güncellendi: <b>{count}</b> hisse.", parse_mode=ParseMode.HTML)


async def cmd_watch_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_ensure_sub(chat_id)

    if not context.args:
        await update.message.reply_text("Kullanım: /watch_add SASA", parse_mode=ParseMode.HTML)
        return

    t = clean_ticker(context.args[0])
    ok = db_watch_add(chat_id, t)
    if ok:
        await update.message.reply_text(f"✅ Eklendi: <b>{t}</b>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("❌ Geçersiz hisse.", parse_mode=ParseMode.HTML)


async def cmd_watch_del(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_ensure_sub(chat_id)

    if not context.args:
        await update.message.reply_text("Kullanım: /watch_del SASA", parse_mode=ParseMode.HTML)
        return

    t = clean_ticker(context.args[0])
    ok = db_watch_del(chat_id, t)
    if ok:
        await update.message.reply_text(f"🗑️ Silindi: <b>{t}</b>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("❌ Bulunamadı / silinemedi.", parse_mode=ParseMode.HTML)


async def cmd_alarm_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_set_alarm(chat_id, True)
    await update.message.reply_text(
        "🚨 <b>ALARM AÇIK</b>\n"
        "Sadece <b>TOPLAMA</b> ve <b>DİP TOPLAMA</b> sinyalleri gelir.\n"
        "Aynı hisse + aynı sinyal: <b>60 dk</b> içinde tekrar gönderilmez.",
        parse_mode=ParseMode.HTML
    )


async def cmd_alarm_off(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_set_alarm(chat_id, False)
    await update.message.reply_text("🔕 <b>ALARM KAPALI</b>", parse_mode=ParseMode.HTML)


async def cmd_report_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_set_report(chat_id, True)
    await update.message.reply_text("🧾 <b>OTOMATİK RAPOR AÇIK</b>", parse_mode=ParseMode.HTML)


async def cmd_report_off(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    chat_id = update.effective_chat.id
    db_set_report(chat_id, False)
    await update.message.reply_text("🧾 <b>OTOMATİK RAPOR KAPALI</b>", parse_mode=ParseMode.HTML)


# -----------------------------
# Jobs (Auto)
# -----------------------------
async def job_alarm_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Alarm: TOPLAMA + DİP TOPLAMA. Aynı sinyal 60 dk içinde tekrar gitmez."""
    chat_ids = db_get_subs("alarm_enabled")
    if not chat_ids:
        return

    # seans dışı spam olmasın
    if not is_market_hours_tr():
        return

    try:
        _, xu_change = await get_xu100_summary()
        top10_min_vol, _all_rows = await get_bist200_threshold(xu_change)
        thresh_ok = (top10_min_vol == top10_min_vol) and (not math.isnan(top10_min_vol)) and top10_min_vol != float("inf")
        thresh_s = format_top10_threshold(top10_min_vol) if thresh_ok else "n/a"

        # in-memory cooldown (restart olursa sıfırlanır - normal)
        last_sent = context.application.bot_data.setdefault("alarm_last_sent", {})
        now_ts = time.time()

        for chat_id in chat_ids:
            watch = db_get_watchlist(chat_id)
            if not watch:
                continue

            rows = await build_rows_from_is_list(watch)
            if thresh_ok:
                _apply_signals_with_threshold(rows, xu_change, top10_min_vol)
            else:
                compute_signal_rows(rows, xu_change)

            alerts = []
            chat_map = last_sent.setdefault(chat_id, {})  # ticker -> {signal: ts}

            for r in rows:
                t = r.get("ticker")
                st = r.get("signal_text", "")

                if st not in ALARM_ALLOWED:
                    continue

                sig_icon = r.get("signal", "-")
                ch = r.get("change", float("nan"))
                vol = r.get("volume", float("nan"))
                ch_s = "n/a" if (ch != ch) else f"{ch:+.2f}%"
                vol_s = format_volume(vol)

                ticker_map = chat_map.setdefault(t, {})  # signal -> ts
                last_ts = float(ticker_map.get(st, 0.0) or 0.0)

                if (now_ts - last_ts) < ALARM_COOLDOWN_SEC:
                    continue

                ticker_map[st] = now_ts
                alerts.append(f"{sig_icon} <b>{t}</b> • {st} • {ch_s} • {vol_s}")

            if alerts:
                msg = (
                    f"🚨 <b>ALARM</b> (Top10 eşik ≥ <b>{thresh_s}</b>)\n"
                    + "\n".join(alerts)
                )
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.exception("alarm job error: %s", e)


async def job_report_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Daha sık otomatik rapor: Watchlist Radar (abonelere)."""
    chat_ids = db_get_subs("report_enabled")
    if not chat_ids:
        return

    if not is_market_hours_tr():
        return

    try:
        _, xu_change = await get_xu100_summary()
        top10_min_vol, _all_rows = await get_bist200_threshold(xu_change)
        thresh_ok = (top10_min_vol == top10_min_vol) and (not math.isnan(top10_min_vol)) and top10_min_vol != float("inf")
        thresh_s = format_top10_threshold(top10_min_vol) if thresh_ok else "n/a"

        for chat_id in chat_ids:
            watch = db_get_watchlist(chat_id)
            if not watch:
                continue

            rows = await build_rows_from_is_list(watch)
            if thresh_ok:
                _apply_signals_with_threshold(rows, xu_change, top10_min_vol)
            else:
                compute_signal_rows(rows, xu_change)

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🧾 <b>OTOMATİK RAPOR</b> • Watchlist (Top10 eşik ≥ <b>{thresh_s}</b>)\n"
                     f"• Liste: <b>{', '.join(watch)}</b>",
                parse_mode=ParseMode.HTML
            )
            await context.bot.send_message(
                chat_id=chat_id,
                text=make_table(rows, "📌 <b>Watchlist Radar</b>"),
                parse_mode=ParseMode.HTML
            )

    except Exception as e:
        logger.exception("report job error: %s", e)


async def job_close_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    """17:50 kapanışa yakın tam EOD raporu (abonelere)."""
    chat_ids = db_get_subs("report_enabled")
    if not chat_ids:
        return

    try:
        bist200_list = env_csv("BIST200_TICKERS")
        if not bist200_list:
            return

        xu_close, xu_change = await get_xu100_summary()
        rows = await build_rows_from_is_list(bist200_list)
        top10_min_vol = compute_signal_rows(rows, xu_change)

        first20 = rows[:20]
        rows_with_vol = [r for r in rows if isinstance(r.get("volume"), (int, float)) and not math.isnan(r["volume"])]
        top10_vol = sorted(rows_with_vol, key=lambda x: x.get("volume", 0) or 0, reverse=True)[:10]
        toplama_cand = pick_candidates(rows, "TOPLAMA")
        dip_cand = pick_candidates(rows, "DİP TOPLAMA")

        xu_close_s = "n/a" if (xu_close != xu_close) else f"{xu_close:,.2f}"
        xu_change_s = "n/a" if (xu_change != xu_change) else f"{xu_change:+.2f}%"

        for chat_id in chat_ids:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🕔 <b>KAPANIŞ RAPORU (17:50)</b>\n"
                     f"📊 <b>XU100</b> • {xu_close_s} • {xu_change_s}\n"
                     f"🧱 <b>Kriter</b>: Top10 hacim eşiği ≥ <b>{format_top10_threshold(top10_min_vol)}</b>",
                parse_mode=ParseMode.HTML
            )
            await context.bot.send_message(chat_id=chat_id, text=make_table(first20, "📍 <b>Hisse Radar (ilk 20)</b>"), parse_mode=ParseMode.HTML)
            if top10_vol:
                await context.bot.send_message(chat_id=chat_id, text=make_table(top10_vol, "🔥 <b>EN YÜKSEK HACİM – TOP 10</b>"), parse_mode=ParseMode.HTML)
            await context.bot.send_message(
                chat_id=chat_id,
                text=make_table(toplama_cand, "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>") if toplama_cand else "🧠 <b>YÜKSELECEK ADAYLAR (TOPLAMA)</b>\n—",
                parse_mode=ParseMode.HTML
            )
            await context.bot.send_message(
                chat_id=chat_id,
                text=make_table(dip_cand, "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>") if dip_cand else "🧲 <b>DİP TOPLAMA ADAYLAR (EKSİ + HACİM)</b>\n—",
                parse_mode=ParseMode.HTML
            )
            await context.bot.send_message(chat_id=chat_id, text=signal_summary_compact(rows), parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.exception("close report job error: %s", e)


def schedule_jobs(app: Application) -> None:
    """JobQueue kurulum."""
    jq = app.job_queue

    # Alarm taraması
    jq.run_repeating(job_alarm_scan, interval=ALARM_SCAN_EVERY_SEC, first=15)

    # Daha sık rapor
    jq.run_repeating(job_report_scan, interval=REPORT_SCAN_EVERY_SEC, first=30)

    # Kapanış raporu: her gün 17:50 TR
    jq.run_daily(job_close_report, time=dtime(CLOSE_HOUR, CLOSE_MINUTE, tzinfo=TZ))


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    db_init()

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN env missing")

    app = Application.builder().token(token).build()

    # Commands
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))

    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("watch_set", cmd_watch_set))
    app.add_handler(CommandHandler("watch_add", cmd_watch_add))
    app.add_handler(CommandHandler("watch_del", cmd_watch_del))

    app.add_handler(CommandHandler("alarm_on", cmd_alarm_on))
    app.add_handler(CommandHandler("alarm_off", cmd_alarm_off))
    app.add_handler(CommandHandler("report_on", cmd_report_on))
    app.add_handler(CommandHandler("report_off", cmd_report_off))

    schedule_jobs(app)

    logger.info("Bot starting... version=%s", BOT_VERSION)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
