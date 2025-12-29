import os
import time
import json
import logging
from typing import List, Dict, Any, Tuple

import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("TAIPO_PRO_INTEL")

# -----------------------------
# ENV
# -----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

# BIST200_TICKERS: "THYAO.IS,ASELS.IS,..."  (Render env içinde tek satır)
BIST200_TICKERS_RAW = os.getenv("BIST200_TICKERS", "").strip()
WATCHLIST_BIST_RAW = os.getenv("WATCHLIST_BIST", "").strip()

# Optional
MODE = os.getenv("MODE", "prod").strip().lower()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing! Render Environment Variables içine BOT_TOKEN ekle.")

# -----------------------------
# TradingView Scanner Config
# -----------------------------
TV_SCAN_URL = "https://scanner.tradingview.com/turkey/scan"
TV_TIMEOUT = 12  # seconds

# Cache (rate-limit yememek için)
_CACHE: Dict[str, Any] = {
    "index": {"ts": 0, "data": None},
    "radar": {},  # page -> {"ts":..., "data":...}
}
CACHE_TTL_SEC = 60  # aynı dakikada 10 kez çağırmayalım

# -----------------------------
# Helpers
# -----------------------------
def _split_csv(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace("\n", ",").split(",")]
    return [p for p in parts if p]

def _to_tv_symbol(ticker: str) -> str:
    """
    Render env içine .IS ile girsen bile TradingView'e BIST: sembolü olarak gider.
    THYAO.IS -> BIST:THYAO
    THYAO -> BIST:THYAO
    """
    t = ticker.strip().upper()
    if not t:
        return t
    if t.endswith(".IS"):
        t = t[:-3]
    if ":" in t:
        return t  # kullanıcı zaten "BIST:THYAO" verdi diyelim
    return f"BIST:{t}"

def _chunks(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def tv_scan_symbols(tv_symbols: List[str]) -> Tuple[List[Dict[str, Any]], str]:
    """
    TradingView scanner'a tek POST ile çoklu sembol verisi çeker.
    Dönen liste her satır için: {"symbol": "...", "close": ..., "change": ..., "volume": ...}
    Hata olursa ([], "hata_mesaji") döner.
    """
    if not tv_symbols:
        return [], "EMPTY_SYMBOLS"

    payload = {
        "symbols": {"tickers": tv_symbols, "query": {"types": []}},
        "columns": ["name", "close", "change", "volume"]
    }

    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return [], f"TV_HTTP_{r.status_code}"
        data = r.json()
        rows = data.get("data", [])
        out = []
        for row in rows:
            s = row.get("s")  # "BIST:THYAO"
            d = row.get("d", [])  # [name, close, change, volume]
            # d bazen None dönebilir
            name = d[0] if len(d) > 0 else None
            close = d[1] if len(d) > 1 else None
            change = d[2] if len(d) > 2 else None
            vol = d[3] if len(d) > 3 else None
            out.append({"symbol": s, "name": name, "close": close, "change": change, "volume": vol})
        return out, ""
    except Exception as e:
        return [], f"TV_EXC_{type(e).__name__}:{e}"

def format_radar_table(rows: List[Dict[str, Any]], missing_note: str = "") -> str:
    """
    Telegram monospace tablo gibi gözüksün diye sade format.
    """
    header = "TICKER        Δ%      Close        Vol\n"
    header += "-------------------------------------------\n"
    lines = []
    missing = 0
    for it in rows:
        sym = (it.get("symbol") or "").replace("BIST:", "")
        chg = it.get("change")
        close = it.get("close")
        vol = it.get("volume")

        if chg is None or close is None:
            missing += 1
            chg_str = "n/a"
            close_str = "n/a"
        else:
            chg_str = f"{float(chg):+.2f}"
            close_str = f"{float(close):.2f}"

        if vol is None:
            vol_str = "n/a"
        else:
            # volume çok büyükse sadeleştir
            try:
                v = float(vol)
                if v >= 1e9:
                    vol_str = f"{v/1e9:.2f}B"
                elif v >= 1e6:
                    vol_str = f"{v/1e6:.2f}M"
                elif v >= 1e3:
                    vol_str = f"{v/1e3:.2f}K"
                else:
                    vol_str = f"{v:.0f}"
            except:
                vol_str = str(vol)

        lines.append(f"{sym:<10}  {chg_str:>6}  {close_str:>10}  {vol_str:>8}")

    footer = ""
    if missing > 0:
        footer += f"\n⚠️ Not: {missing} sembolde veri alınamadı."
    if missing_note:
        footer += f"\n⚠️ {missing_note}"
    return "```\n" + header + "\n".join(lines) + footer + "\n```"

def get_bist200_list() -> List[str]:
    tickers = _split_csv(BIST200_TICKERS_RAW)
    return tickers

def get_watchlist() -> List[str]:
    return _split_csv(WATCHLIST_BIST_RAW)

# -----------------------------
# Commands
# -----------------------------
async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("🏓 Pong! Bot ayakta.")

async def eod_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /eod: BIST:XU100 + kısa radar özeti + sayfa bilgisi
    """
    chat_id = update.effective_chat.id
    log.info("EOD requested by chat=%s", chat_id)

    # 1) Endeks (XU100) - TradingView: BIST:XU100
    now = time.time()
    if _CACHE["index"]["data"] is not None and (now - _CACHE["index"]["ts"] < CACHE_TTL_SEC):
        idx = _CACHE["index"]["data"]
        idx_err = ""
    else:
        idx_rows, idx_err = tv_scan_symbols(["BIST:XU100"])
        idx = idx_rows[0] if idx_rows else None
        _CACHE["index"]["data"] = idx
        _CACHE["index"]["ts"] = now

    if not idx or idx.get("close") is None:
        await update.effective_message.reply_text(f"⚠️ Endeks verisi alınamadı: {idx_err or 'BOS_VERI'}")
    else:
        close = float(idx["close"])
        chg = idx.get("change")
        chg_txt = f"{float(chg):+.2f}%" if chg is not None else "n/a"
        await update.effective_message.reply_text(
            "📌 *BIST100 (XU100) Özet*\n"
            f"• Sembol: XU100\n"
            f"• Kapanış: *{close:,.2f}*\n"
            f"• Günlük: *{chg_txt}*\n\n"
            "📡 Radar için:\n"
            "• `/radar 1` ... `/radar 10`\n",
            parse_mode="Markdown"
        )

    # 2) Mini Radar (ilk 20) — BIST200 listesinden
    tickers = get_bist200_list()
    if not tickers:
        await update.effective_message.reply_text("⚠️ BIST200_TICKERS boş. Render Environment’a eklemen lazım.")
        return

    first20 = tickers[:20]
    tv_syms = [_to_tv_symbol(t) for t in first20]
    rows, err = tv_scan_symbols(tv_syms)
    if not rows:
        await update.effective_message.reply_text(f"⚠️ Radar alınamadı: {err or 'BOS_VERI'}")
        return

    # değişime göre sırala (None en alta)
    def _sort_key(x):
        c = x.get("change")
        return (-9999 if c is None else float(c))
    rows_sorted = sorted(rows, key=_sort_key)  # en kötüden iyiye
    await update.effective_message.reply_text("📍 *Hisse Radar (ilk 20)*", parse_mode="Markdown")
    await update.effective_message.reply_text(format_radar_table(rows_sorted), parse_mode="Markdown")

async def radar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /radar N  -> N=1..10
    Her sayfa 20 hisse.
    """
    chat_id = update.effective_chat.id

    # args parse
    page = 1
    if context.args:
        try:
            page = int(context.args[0])
        except:
            page = 1

    if page < 1:
        page = 1

    tickers = get_bist200_list()
    if not tickers:
        await update.effective_message.reply_text("⚠️ BIST200_TICKERS boş. Render Environment’a eklemen lazım.")
        return

    pages = _chunks(tickers, 20)
    max_page = len(pages)

    if page > max_page:
        await update.effective_message.reply_text(f"⚠️ Sayfa yok. En fazla: {max_page}. Örn: /radar {max_page}")
        return

    log.info("RADAR page=%s requested by chat=%s", page, chat_id)

    # cache
    now = time.time()
    cache_hit = _CACHE["radar"].get(page)
    if cache_hit and (now - cache_hit["ts"] < CACHE_TTL_SEC):
        rows = cache_hit["data"]
        err = ""
    else:
        batch = pages[page - 1]
        tv_syms = [_to_tv_symbol(t) for t in batch]
        rows, err = tv_scan_symbols(tv_syms)
        _CACHE["radar"][page] = {"ts": now, "data": rows}

    if not rows:
        await update.effective_message.reply_text(f"⚠️ Radar alınamadı: {err or 'BOS_VERI'}")
        return

    # değişime göre sırala (None en alta)
    def _sort_key(x):
        c = x.get("change")
        # negatiften pozitife daha net görünsün diye:
        return 9999 if c is None else float(c)

    rows_sorted = sorted(rows, key=_sort_key)  # en düşük -> en yüksek
    title = f"📡 *BIST200 RADAR — Parça {page}/{max_page}*\n(20 hisse)"
    await update.effective_message.reply_text(title, parse_mode="Markdown")
    await update.effective_message.reply_text(format_radar_table(rows_sorted), parse_mode="Markdown")

# -----------------------------
# MAIN
# -----------------------------
def main():
    log.info("Bot starting... MODE=%s", MODE)

    app = Application.builder().token(BOT_TOKEN).build()

    # handlers (RADAR kesin yakalansın diye net CommandHandler)
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("eod", eod_cmd))
    app.add_handler(CommandHandler("radar", radar_cmd))

    # polling
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
