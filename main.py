# main.py
# TAIPO PRO INTEL - Stabil Yahoo Finance + BIST Radar
# python-telegram-bot v22.x (async)

import os
import time
import math
import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("taipo-pro-intel")


# ----------------------------
# ENV
# ----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

MODE = os.getenv("MODE", "PROD").strip().upper()

WATCHLIST_BIST = os.getenv("WATCHLIST_BIST", "").strip()
BIST200_TICKERS = os.getenv("BIST200_TICKERS", "").strip()

# Optional: if you want currency in EOD (e.g., "USDTRY=X")
BIST_CURRENCY = os.getenv("BIST_CURRENCY", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing. Please set it in Render Environment Variables.")


# ----------------------------
# Cache / Rate Limit Controls
# ----------------------------
@dataclass
class CacheItem:
    ts: float
    value: object


CACHE: Dict[str, CacheItem] = {}

# Yahoo’ya çok vurmayı engelle (özellikle Render testlerinde)
CACHE_TTL_SECONDS = 60 * 10     # 10 dk cache
MIN_SECONDS_BETWEEN_SAME_KEY = 20  # aynı şeyi 20 sn içinde tekrar çekme

_last_fetch_ts: Dict[str, float] = {}


def _now() -> float:
    return time.time()


def _cache_get(key: str) -> Optional[object]:
    item = CACHE.get(key)
    if not item:
        return None
    if (_now() - item.ts) > CACHE_TTL_SECONDS:
        return None
    return item.value


def _cache_set(key: str, value: object) -> None:
    CACHE[key] = CacheItem(ts=_now(), value=value)


def _throttle_key(key: str) -> bool:
    """Return True if should wait (too frequent)."""
    last = _last_fetch_ts.get(key, 0.0)
    if (_now() - last) < MIN_SECONDS_BETWEEN_SAME_KEY:
        return True
    _last_fetch_ts[key] = _now()
    return False


# ----------------------------
# Helpers
# ----------------------------
def parse_tickers(raw: str) -> List[str]:
    if not raw:
        return []
    # supports "A.IS,B.IS, C.IS" etc
    out = []
    for x in raw.split(","):
        t = x.strip()
        if t:
            out.append(t)
    return out


def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def fmt_pct(x: float) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "n/a"
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.2f}%"


def safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


# ----------------------------
# Yahoo Finance fetch (safe)
# ----------------------------
async def fetch_history_safe(
    ticker: str,
    period: str = "5d",
    interval: str = "1d",
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Returns (df, err_msg). df None => error or empty.
    Uses caching + throttling to avoid rate limit.
    """
    cache_key = f"HIST:{ticker}:{period}:{interval}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached, None

    if _throttle_key(cache_key):
        # Too soon; use cache if exists; else wait a bit
        await asyncio.sleep(1.0)

    try:
        # yfinance can rate-limit; keep requests conservative
        t = yf.Ticker(ticker)
        df = t.history(period=period, interval=interval, auto_adjust=False)
        if df is None or df.empty:
            return None, f"Not enough data / empty for {ticker}"
        _cache_set(cache_key, df)
        return df, None

    except Exception as e:
        # yfinance may throw YFRateLimitError or other network exceptions
        msg = str(e)
        if "Rate limited" in msg or "Too Many Requests" in msg:
            return None, "Yahoo rate limit (Too Many Requests)."
        return None, msg


def calc_last_change(df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (last_close, daily_pct_change) using last two rows.
    """
    if df is None or df.empty:
        return None, None
    close = df.get("Close")
    if close is None or len(close) < 2:
        return None, None
    last = safe_float(close.iloc[-1])
    prev = safe_float(close.iloc[-2])
    if last is None or prev is None or prev == 0:
        return last, None
    pct = (last - prev) / prev * 100.0
    return last, pct


def calc_volume(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty:
        return None
    vol = df.get("Volume")
    if vol is None or len(vol) == 0:
        return None
    return safe_float(vol.iloc[-1])


# ----------------------------
# Commands
# ----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong! Bot çalışıyor.")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /eod
    - BIST index snapshot (XU100) best-effort
    - Optional currency snapshot
    - Small radar preview (first 20 of BIST200 if present)
    """
    await update.message.reply_text("⏳ EOD hazırlanıyor...")

    lines = []
    lines.append("📌 <b>TAIPO PRO INTEL – EOD</b>")

    # 1) Index
    idx_symbol = "^XU100"
    df_idx, err = await fetch_history_safe(idx_symbol, period="5d", interval="1d")
    if df_idx is None:
        lines.append(f"⚠️ <b>Endeks</b> alınamadı: <code>{err or 'BOS_VERI'}</code>")
    else:
        last, pct = calc_last_change(df_idx)
        lines.append(f"📈 <b>{idx_symbol}</b> Close: <code>{last:.2f}</code> | Günlük: <b>{fmt_pct(pct)}</b>")

    # 2) Currency (optional)
    if BIST_CURRENCY:
        df_fx, err_fx = await fetch_history_safe(BIST_CURRENCY, period="5d", interval="1d")
        if df_fx is None:
            lines.append(f"💱 <b>{BIST_CURRENCY}</b>: <code>{err_fx or 'BOS_VERI'}</code>")
        else:
            last_fx, pct_fx = calc_last_change(df_fx)
            if last_fx is not None:
                lines.append(f"💱 <b>{BIST_CURRENCY}</b>: <code>{last_fx:.4f}</code> | Günlük: <b>{fmt_pct(pct_fx)}</b>")

    # 3) Radar preview (first chunk)
    bist200 = parse_tickers(BIST200_TICKERS)
    if bist200:
        first_chunk = bist200[:20]
        radar_text = await build_radar_block(first_chunk, title="🔎 Radar Önizleme (İlk 20)")
        lines.append(radar_text)
        lines.append("➡️ Devamı için: <code>/radar 1</code>, <code>/radar 2</code> ...")
    else:
        lines.append("ℹ️ BIST200 listesi yok. Render env’ye <code>BIST200_TICKERS</code> ekleyebilirsin.")

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_radar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /radar N
    - N: 1..10 (200 hisseyi 20'şer parça)
    """
    bist200 = parse_tickers(BIST200_TICKERS)
    if not bist200:
        await update.message.reply_text("⚠️ BIST200_TICKERS boş. Render Environment’a eklemen gerekiyor.")
        return

    chunks = chunk_list(bist200, 20)
    n = 1
    if context.args:
        try:
            n = int(context.args[0])
        except Exception:
            n = 1

    if n < 1 or n > len(chunks):
        await update.message.reply_text(f"⚠️ Geçersiz parça. 1 ile {len(chunks)} arasında yaz.\nÖrn: /radar 1")
        return

    await update.message.reply_text(f"⏳ Radar {n}/{len(chunks)} hazırlanıyor...")

    block = await build_radar_block(chunks[n - 1], title=f"📡 BIST200 RADAR – Parça {n}/{len(chunks)}")

    await update.message.reply_text(block, parse_mode=ParseMode.HTML)


# ----------------------------
# Radar builder (safe + light)
# ----------------------------
async def build_radar_block(tickers: List[str], title: str) -> str:
    """
    For each ticker: last close, daily % change, volume (last)
    Keeps Yahoo calls limited with cache & small delays.
    """
    rows = []
    failed = 0

    # Gentle pacing: small delay between calls to reduce rate limit chance
    for i, t in enumerate(tickers, start=1):
        df, err = await fetch_history_safe(t, period="5d", interval="1d")
        if df is None:
            failed += 1
            rows.append((t, None, None, None, err))
        else:
            last, pct = calc_last_change(df)
            vol = calc_volume(df)
            rows.append((t, last, pct, vol, None))

        # tiny delay every few tickers
        if i % 5 == 0:
            await asyncio.sleep(0.6)

    # Rank by daily change (desc), ignoring None
    scored = []
    for (t, last, pct, vol, err) in rows:
        scored.append((t, pct if pct is not None else -9999, last, vol, err))
    scored.sort(key=lambda x: x[1], reverse=True)

    lines = [f"<b>{title}</b>"]
    lines.append("<code>TICKER     Δ%     Close        Vol</code>")
    lines.append("<code>------------------------------------</code>")

    for t, pct, last, vol, err in scored:
        if err:
            lines.append(f"<code>{t:<10}  n/a   n/a          n/a</code>  ⚠️")
            continue

        close_s = "n/a" if last is None else f"{last:,.2f}".replace(",", "")
        pct_s = "n/a" if pct == -9999 else fmt_pct(pct)
        vol_s = "n/a" if vol is None else f"{int(vol):,}".replace(",", ".")
        # keep fixed-ish width
        lines.append(f"<code>{t:<10} {pct_s:>6} {close_s:>10} {vol_s:>12}</code>")

    if failed > 0:
        lines.append(f"\n⚠️ <b>Not:</b> {failed} sembolde veri alınamadı (BOS_VERI / rate limit / delisted).")

    return "\n".join(lines)


# ----------------------------
# App bootstrap
# ----------------------------
def main() -> None:
    logger.info("Starting TAIPO PRO INTEL bot | MODE=%s", MODE)

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eod", cmd_eod))
    app.add_handler(CommandHandler("radar", cmd_radar))

    # If you want: show help when unknown command
    # (optional)

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
