import os
import time
import logging
from typing import List, Tuple, Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# TradingView official screener wrapper
from tradingview_screener import Query, col

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("taipo_pro_intel")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

# Cache to avoid rate-limit / bans (simple in-memory TTL)
_CACHE = {
    "ts": 0,
    "eod_text": "",
}
CACHE_TTL_SEC = 120  # 2 minutes

# How many radar rows per Telegram message
CHUNK_SIZE = 20


def _chunk_lines(lines: List[str], chunk_size: int = CHUNK_SIZE) -> List[str]:
    return ["\n".join(lines[i:i + chunk_size]) for i in range(0, len(lines), chunk_size)]


def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        # pandas may give numpy types
        return float(x)
    except Exception:
        return default


def _fmt_num(n: Optional[float]) -> str:
    if n is None:
        return "?"
    # volume-like big numbers
    if abs(n) >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:.2f}K"
    return f"{n:.2f}"


def _fmt_pct(p: Optional[float]) -> str:
    if p is None:
        return "?"
    sign = "+" if p > 0 else ""
    return f"{sign}{p:.2f}%"


def tv_get_xu100_snapshot() -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (close, change_percent) for BIST:XU100.
    change field naming in TradingView is usually 'change' (percent).
    """
    try:
        # IMPORTANT: use set_tickers so we target the exact symbol
        q = (
            Query()
            .set_markets("turkey")
            .select("name", "close", "change")
            .set_tickers("BIST:XU100")
            .limit(1)
        )
        _, df = q.get_scanner_data()
        if df is None or df.empty:
            return None, None
        close = _safe_float(df.iloc[0].get("close"))
        chg = _safe_float(df.iloc[0].get("change"))
        return close, chg
    except Exception as e:
        logger.exception("XU100 snapshot failed: %s", e)
        return None, None


def tv_get_bist_radar(limit: int = 200) -> List[str]:
    """
    “BIST200 gibi” radar:
    TradingView Turkey market içinde BIST hisselerini tarar,
    Relative Volume (10d) + Volume + Daily Change ile sıralayıp üstten 200 verir.
    """
    lines: List[str] = []

    try:
        q = (
            Query()
            .set_markets("turkey")
            .select(
                "name",
                "close",
                "change",
                "volume",
                "relative_volume_10d_calc",
                "market_cap_basic",
                "exchange",
            )
            .where(
                col("exchange") == "BIST",
                col("volume") > 0,
            )
            .order_by("relative_volume_10d_calc", ascending=False)
            .limit(limit)
        )

        _, df = q.get_scanner_data()
        if df is None or df.empty:
            return ["⚠️ Radar verisi boş döndü (TradingView)."]

        # df index generally contains the "ticker" string like "BIST:THYAO"
        # sometimes there's also "ticker" column; handle both
        for i in range(min(limit, len(df))):
            row = df.iloc[i]
            ticker = None
            try:
                ticker = df.index[i]
            except Exception:
                ticker = row.get("ticker")

            name = row.get("name") or ""
            close = _safe_float(row.get("close"))
            chg = _safe_float(row.get("change"))
            vol = _safe_float(row.get("volume"))
            rv = _safe_float(row.get("relative_volume_10d_calc"))
            mcap = _safe_float(row.get("market_cap_basic"))

            # shorten symbol
            sym = str(ticker).split(":")[-1] if ticker else "?"
            rv_txt = f"{rv:.2f}" if rv is not None else "?"
            mcap_txt = _fmt_num(mcap) if mcap is not None else "?"

            lines.append(
                f"{i+1:>3}. {sym:<6} {close if close is not None else '?':>8}  "
                f"({ _fmt_pct(chg) })  Vol:{_fmt_num(vol):>7}  RV:{rv_txt:>4}  MCap:{mcap_txt}"
            )

        return lines

    except Exception as e:
        logger.exception("Radar failed: %s", e)
        return [f"⚠️ Radar alınamadı: {e}"]


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("✅ ping -> pong (TAIPO PRO INTEL çalışıyor)")


async def cmd_eod(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Cache (avoid hitting TradingView too frequently)
    now = int(time.time())
    if _CACHE["eod_text"] and (now - _CACHE["ts"] < CACHE_TTL_SEC):
        await update.message.reply_text(_CACHE["eod_text"])
        return

    close, chg = tv_get_xu100_snapshot()

    header = "📌 TAIPO PRO INTEL – EOD (TradingView)\n"
    if close is None and chg is None:
        header += "⚠️ XU100 (BIST100) anlık veri alınamadı (delayed/boş döndü olabilir).\n"
    else:
        header += f"🇹🇷 XU100: {close}  ({_fmt_pct(chg)})\n"

    header += "\n📡 RADAR (BIST – yüksek RV/volume odaklı) — 200 satır, 20’şer mesaj:\n"

    radar_lines = tv_get_bist_radar(limit=200)

    # First message: header
    await update.message.reply_text(header)

    # Send radar in chunks
    chunks = _chunk_lines(radar_lines, CHUNK_SIZE)
    for idx, chunk in enumerate(chunks, start=1):
        await update.message.reply_text(f"— Radar Paket {idx}/{len(chunks)} —\n{chunk}")

    # Store cache as a short marker message (header only, to avoid storing all 200 lines)
    _CACHE["ts"] = now
    _CACHE["eod_text"] = header + "\n(⚡ Cache aktif: 2 dk içinde tekrar çağrılırsa aynı başlık döner.)"


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env missing")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("eod", cmd_eod))

    logger.info("Bot starting…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
