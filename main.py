import os
import time
import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf
import pandas as pd

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("taipo-pro-intel")

# -------------------------------------------------
# CACHE (TTL)
# -------------------------------------------------
@dataclass
class CacheItem:
    value: Any
    expires_at: float


_CACHE: Dict[str, CacheItem] = {}
_CACHE_LOCK = asyncio.Lock()


async def cache_get(key: str) -> Optional[Any]:
    async with _CACHE_LOCK:
        item = _CACHE.get(key)
        if not item:
            return None
        if time.time() >= item.expires_at:
            _CACHE.pop(key, None)
            return None
        return item.value


async def cache_set(key: str, value: Any, ttl: int):
    async with _CACHE_LOCK:
        _CACHE[key] = CacheItem(value=value, expires_at=time.time() + ttl)


# -------------------------------------------------
# HELPERS
# -------------------------------------------------
class YahooDataError(Exception):
    pass


def parse_csv_env(name: str) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]


def chunks(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def fmt_try(v: float) -> str:
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# -------------------------------------------------
# YAHOO SAFE FETCH
# -------------------------------------------------
def df_ok(df: pd.DataFrame) -> bool:
    return df is not None and not df.empty and "Close" in df.columns


async def yahoo_download_once(
    tickers: List[str],
    period: str = "15d",
    interval: str = "1d",
) -> pd.DataFrame:
    try:
        df = yf.download(
            tickers=" ".join(tickers),
            period=period,
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            threads=False,
            progress=False,
        )
        return df
    except Exception as e:
        raise YahooDataError(str(e)) from e


# -------------------------------------------------
# INDEX (BIST100)
# -------------------------------------------------
async def fetch_index() -> Tuple[str, pd.DataFrame]:
    for sym in ["XU100.IS", "^XU100"]:
        try:
            df = await yahoo_download_once([sym], period="10d", interval="1d")
            if isinstance(df, pd.DataFrame) and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df = df[sym]
                if df_ok(df):
                    return sym, df
        except Exception:
            continue
    raise YahooDataError("Endeks verisi yok / Yahoo boş dönüyor")


def build_index_msg(sym: str, df: pd.DataFrame) -> str:
    closes = df["Close"].dropna()
    last = closes.iloc[-1]
    prev = closes.iloc[-2]
    chg = last - prev
    pct = (chg / prev) * 100 if prev else 0
    arrow = "🟢" if chg > 0 else ("🔴" if chg < 0 else "🟡")
    date = closes.index[-1].strftime("%Y-%m-%d")

    return (
        f"📌 <b>BIST100</b>\n"
        f"• Sembol: <code>{sym}</code>\n"
        f"• Tarih: <b>{date}</b>\n"
        f"• Kapanış: <b>{fmt_try(last)}</b>\n"
        f"• Değişim: {arrow} <b>{chg:+.2f}</b> (<b>{pct:+.2f}%</b>)"
    )


# -------------------------------------------------
# RADAR
# -------------------------------------------------
def build_radar(df: pd.DataFrame, tickers: List[str]) -> str:
    rows = []
    for t in tickers:
        try:
            dft = df[t]
            closes = dft["Close"].dropna()
            if closes.shape[0] < 2:
                continue
            last, prev = closes.iloc[-1], closes.iloc[-2]
            pct = ((last - prev) / prev) * 100 if prev else 0

            spike = ""
            if "Volume" in dft.columns and len(dft["Volume"].dropna()) >= 6:
                v = dft["Volume"].dropna()
                ratio = v.iloc[-1] / v.iloc[-6:-1].mean()
                if ratio >= 1.5:
                    spike = f" | Hacim x{ratio:.2f}"

            arrow = "🟢" if pct > 0 else ("🔴" if pct < 0 else "🟡")
            rows.append((abs(pct), f"<code>{t}</code> → {arrow} <b>{pct:+.2f}%</b> | {fmt_try(last)}{spike}"))
        except Exception:
            continue

    if not rows:
        return "⚠️ Radar: veri yok"

    rows.sort(key=lambda x: x[0], reverse=True)
    out = ["📡 <b>Hisse Radar</b>"]
    for i, (_, line) in enumerate(rows[:20], start=1):
        out.append(f"{i}. {line}")
    return "\n".join(out)


# -------------------------------------------------
# COMMANDS
# -------------------------------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏓 Pong! Bot ayakta.")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cached = await cache_get("eod")
    if cached:
        await update.message.reply_text(cached, parse_mode=ParseMode.HTML)
        return

    try:
        sym, df = await fetch_index()
        index_msg = build_index_msg(sym, df)
    except YahooDataError as e:
        index_msg = f"⚠️ <b>Endeks alınamadı</b>\n<code>{e}</code>"

    tickers = parse_csv_env("BIST200_TICKERS")
    pages = chunks(tickers, 20)
    page1 = pages[0] if pages else []

    radar_msg = "ℹ️ Radar listesi yok."
    if page1:
        try:
            rdf = await yahoo_download_once(page1)
            radar_msg = build_radar(rdf, page1)
        except Exception:
            radar_msg = "⚠️ Radar alınamadı (rate limit)."

    final = index_msg + "\n\n" + radar_msg + "\n\n<i>Sayfalar: /radar 1–10</i>"
    await cache_set("eod", final, 90)
    await update.message.reply_text(final, parse_mode=ParseMode.HTML)


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    page = int(args[0]) if args and args[0].isdigit() else 1
    page = max(1, min(page, 10))

    tickers = parse_csv_env("BIST200_TICKERS")
    pages = chunks(tickers, 20)

    if page > len(pages):
        await update.message.reply_text("⚠️ Bu sayfa yok.")
        return

    key = f"radar_{page}"
    cached = await cache_get(key)
    if cached:
        await update.message.reply_text(cached, parse_mode=ParseMode.HTML)
        return

    try:
        rdf = await yahoo_download_once(pages[page - 1])
        msg = f"📄 <b>Radar Sayfa {page}</b>\n" + build_radar(rdf, pages[page - 1])
    except Exception:
        msg = "⚠️ Radar alınamadı (rate limit)."

    await cache_set(key, msg, 120)
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


# -------------------------------------------------
# BOOT
# -------------------------------------------------
def main():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN yok")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))

    log.info("Bot başlıyor...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
