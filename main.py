import os
import time
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, Any, List

import yfinance as yf
import pandas as pd

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("taipo-pro-intel")


# -----------------------------
# SIMPLE TTL CACHE (in-memory)
# -----------------------------
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


async def cache_set(key: str, value: Any, ttl_seconds: int) -> None:
    async with _CACHE_LOCK:
        _CACHE[key] = CacheItem(value=value, expires_at=time.time() + ttl_seconds)


# -----------------------------
# YAHOO FETCH (SAFE)
# -----------------------------
class YahooDataError(Exception):
    pass


def _is_dataframe_ok(df: pd.DataFrame) -> bool:
    # yfinance sometimes returns empty or missing columns
    if df is None or df.empty:
        return False
    # Close must exist for our EOD summary
    return "Close" in df.columns and df["Close"].dropna().shape[0] >= 2


async def fetch_history_once(
    ticker: str,
    period: str = "10d",
    interval: str = "1d",
    timeout_seconds: int = 15,
) -> pd.DataFrame:
    """
    One Yahoo hit (single yfinance call). If it fails, raise.
    """
    # yfinance uses requests under the hood; timeout isn't always respected,
    # but we still keep our retries + cache to avoid hammering.
    try:
        # One call: yf.download
        df = yf.download(
            tickers=ticker,
            period=period,
            interval=interval,
            group_by="column",
            auto_adjust=False,
            threads=False,
            progress=False,
        )
        # yf.download may return multi-index columns for multiple tickers;
        # here we always pass single ticker so it should be normal.
        return df
    except Exception as e:
        raise YahooDataError(str(e)) from e


async def fetch_with_retries(
    tickers_to_try: List[str],
    period: str = "10d",
    interval: str = "1d",
    max_attempts_per_ticker: int = 2,
    base_backoff_sec: float = 1.2,
) -> Tuple[str, pd.DataFrame]:
    """
    Tries tickers in order. For each ticker, retries a little on rate limit/network.
    Returns (used_ticker, df) or raises YahooDataError.
    """
    last_err = None

    for ticker in tickers_to_try:
        for attempt in range(1, max_attempts_per_ticker + 1):
            try:
                df = await fetch_history_once(ticker, period=period, interval=interval)
                if _is_dataframe_ok(df):
                    return ticker, df

                # Data came but empty/insufficient
                last_err = YahooDataError(f"Not enough data for {ticker}")
                break  # no point retrying same ticker if it is truly empty (often delisted/symbol mismatch)

            except Exception as e:
                msg = str(e).lower()
                last_err = e

                # Rate limit patterns (yfinance may throw YFRateLimitError or generic)
                is_rate_limited = ("rate limit" in msg) or ("too many requests" in msg) or ("429" in msg)
                is_temp = is_rate_limited or ("timeout" in msg) or ("temporarily" in msg) or ("connection" in msg)

                if is_temp and attempt < max_attempts_per_ticker:
                    sleep_s = base_backoff_sec * (2 ** (attempt - 1))
                    log.warning("Yahoo temp error for %s (attempt %s/%s): %s | backoff %.1fs",
                                ticker, attempt, max_attempts_per_ticker, e, sleep_s)
                    await asyncio.sleep(sleep_s)
                    continue

                # Non-temporary or out of attempts
                log.warning("Yahoo error for %s (attempt %s/%s): %s",
                            ticker, attempt, max_attempts_per_ticker, e)
                break

    raise YahooDataError(str(last_err) if last_err else "Yahoo fetch failed")


def build_eod_summary(df: pd.DataFrame, used_ticker: str) -> str:
    """
    Builds a clean EOD summary from df.
    Requires at least 2 closes.
    """
    closes = df["Close"].dropna()
    if closes.shape[0] < 2:
        raise YahooDataError(f"Not enough data for {used_ticker}")

    last_close = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2])
    chg = last_close - prev_close
    chg_pct = (chg / prev_close) * 100 if prev_close != 0 else 0.0

    last_date = closes.index[-1]
    if hasattr(last_date, "to_pydatetime"):
        last_date_str = last_date.to_pydatetime().strftime("%Y-%m-%d")
    else:
        last_date_str = str(last_date)[:10]

    arrow = "🟢" if chg > 0 else ("🔴" if chg < 0 else "🟡")

    # TR formatting
    summary = (
        f"📌 <b>BIST100 (Yahoo)</b>\n"
        f"• Sembol: <code>{used_ticker}</code>\n"
        f"• Tarih: <b>{last_date_str}</b>\n"
        f"• Kapanış: <b>{last_close:,.2f}</b>\n"
        f"• Değişim: {arrow} <b>{chg:+.2f}</b> (<b>{chg_pct:+.2f}%</b>)\n"
    )
    return summary


# -----------------------------
# TELEGRAM COMMANDS
# -----------------------------
async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong! Bot çalışıyor.")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    End-of-day / quick index snapshot.
    Safe: cache + retries + fallback symbols.
    """
    # Cache key prevents spamming Yahoo if user sends /eod repeatedly
    cache_key = "eod_bist100"
    cached = await cache_get(cache_key)
    if cached:
        await update.message.reply_text(cached, parse_mode=ParseMode.HTML)
        return

    # Try a more reliable symbol first; fallback to ^XU100
    tickers = ["XU100.IS", "^XU100"]

    try:
        used_ticker, df = await fetch_with_retries(
            tickers_to_try=tickers,
            period="10d",
            interval="1d",
            max_attempts_per_ticker=2,
            base_backoff_sec=1.2,
        )
        msg = build_eod_summary(df, used_ticker)

        # Cache for 90 seconds (tweakable)
        await cache_set(cache_key, msg, ttl_seconds=90)
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

    except YahooDataError as e:
        # This is the clean "BOS_VERI" management
        err_text = str(e)

        # Rate limit special hint (still won't crash)
        low = err_text.lower()
        if "rate" in low or "too many requests" in low or "429" in low:
            out = (
                "⚠️ <b>Yahoo rate-limit</b> yakalandı.\n"
                "• Bot çökmüyor (stabil).\n"
                "• 2–3 dk sonra tekrar dene.\n"
            )
        else:
            out = (
                f"⚠️ <b>Endeks verisi alınamadı</b>\n"
                f"• Sebep: <code>{err_text}</code>\n"
                f"• Not: Sembol uyumsuz / piyasa kapalı / veri gecikmeli olabilir.\n"
            )

        # Cache error briefly to avoid repeated hammering
        await cache_set(cache_key, out, ttl_seconds=30)
        await update.message.reply_text(out, parse_mode=ParseMode.HTML)

    except Exception as e:
        log.exception("Unexpected error in /eod: %s", e)
        out = "⚠️ Beklenmeyen hata oluştu ama bot ayakta. Biraz sonra tekrar dene."
        await update.message.reply_text(out)


# -----------------------------
# APP BOOTSTRAP
# -----------------------------
def require_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v


async def on_startup(app: Application) -> None:
    log.info("Bot started. Commands: /ping /eod")


def main() -> None:
    token = require_env("BOT_TOKEN")

    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("eod", cmd_eod))

    application.post_init = on_startup

    log.info("Starting polling...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
