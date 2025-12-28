import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import requests
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("TAIPO_BIST")

# -----------------------
# Config
# -----------------------
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
DEFAULT_INDEX_TICKER = "^XU100"  # BIST100
HTTP_TIMEOUT = 12


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def parse_watchlist(raw: str) -> List[str]:
    """
    WATCHLIST_BIST expected like:
    "THYAO.IS,AKBNK.IS,ASELS.IS"
    """
    if not raw:
        return []
    items = [x.strip() for x in raw.split(",") if x.strip()]
    # remove duplicates, keep order
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def yahoo_chart(ticker: str, range_: str = "1mo", interval: str = "1d") -> Dict[str, Any]:
    """
    Fetch OHLCV from Yahoo chart endpoint.
    """
    url = YAHOO_CHART_URL.format(ticker=ticker)
    params = {"range": range_, "interval": interval, "includePrePost": "false"}
    headers = {"User-Agent": "Mozilla/5.0"}

    r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    if not data.get("chart") or not data["chart"].get("result"):
        raise ValueError(f"No chart result for {ticker}")

    result = data["chart"]["result"][0]
    indicators = result.get("indicators", {}).get("quote", [])
    if not indicators:
        raise ValueError(f"No quote indicators for {ticker}")

    quote = indicators[0]
    # arrays can contain None
    opens = quote.get("open", [])
    highs = quote.get("high", [])
    lows = quote.get("low", [])
    closes = quote.get("close", [])
    vols = quote.get("volume", [])
    timestamps = result.get("timestamp", [])

    return {
        "ticker": ticker,
        "timestamp": timestamps,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": vols,
    }


def _last_two_valid(values: List[Optional[float]]) -> Optional[Tuple[float, float]]:
    """
    Returns (prev, last) non-None values from the tail.
    """
    cleaned = [v for v in values if v is not None]
    if len(cleaned) < 2:
        return None
    return cleaned[-2], cleaned[-1]


def pct_change(prev: float, last: float) -> float:
    if prev == 0:
        return 0.0
    return (last - prev) / prev * 100.0


def calc_daily_metrics(chart: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute:
    - price_pct: last close vs previous close (%)
    - vol_pct: last volume vs previous volume (%)
    - last candle wick metrics from last available OHLC
    """
    close_pair = _last_two_valid(chart["close"])
    vol_pair = _last_two_valid(chart["volume"])

    if not close_pair or not vol_pair:
        raise ValueError(f"Not enough data for {chart['ticker']}")

    prev_close, last_close = close_pair
    prev_vol, last_vol = vol_pair

    price_pct = pct_change(prev_close, last_close)
    vol_pct = pct_change(prev_vol, last_vol)

    # last candle OHLC (use last non-None aligned values)
    # We'll pick the last index where close is not None and OHLC not None
    idx = None
    for i in range(len(chart["close"]) - 1, -1, -1):
        if (
            chart["close"][i] is not None
            and chart["open"][i] is not None
            and chart["high"][i] is not None
            and chart["low"][i] is not None
        ):
            idx = i
            break
    if idx is None:
        raise ValueError(f"No valid OHLC candle for {chart['ticker']}")

    o = float(chart["open"][idx])
    h = float(chart["high"][idx])
    l = float(chart["low"][idx])
    c = float(chart["close"][idx])

    body = abs(c - o)
    full = max(h - l, 1e-9)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l

    upper_wick_ratio = upper_wick / full
    lower_wick_ratio = lower_wick / full
    body_ratio = body / full

    return {
        "ticker": chart["ticker"],
        "price_pct": price_pct,
        "vol_pct": vol_pct,
        "prev_close": prev_close,
        "last_close": last_close,
        "prev_vol": prev_vol,
        "last_vol": last_vol,
        "ohlc": {"o": o, "h": h, "l": l, "c": c},
        "wick": {
            "upper_ratio": upper_wick_ratio,
            "lower_ratio": lower_wick_ratio,
            "body_ratio": body_ratio,
        },
    }


# -----------------------
# 3'lü Sistem v1
# -----------------------
def delta_thinking(m: Dict[str, Any]) -> Tuple[str, str]:
    """
    Delta = price movement vs volume movement.
    Mod-1 temkinli eşikler:
      - "GÖRÜNMEYEN TOPLAMA": price ~ flat (|price|<=0.25) & volume up >= +15
      - "SAHTE": price up >= +0.4 & volume down <= -10
      - "SAĞLIKLI": price up >= +0.4 & volume up >= +10
    """
    p = m["price_pct"]
    v = m["vol_pct"]

    if abs(p) <= 0.25 and v >= 15:
        return "⚠️ GÖRÜNMEYEN TOPLAMA", f"Fiyat ~{p:+.2f}%, Hacim {v:+.1f}% (sessiz birikim)"
    if p >= 0.40 and v <= -10:
        return "⚠️ SAHTE YÜKSELİŞ", f"Fiyat {p:+.2f}%, Hacim {v:+.1f}% (desteksiz)"
    if p >= 0.40 and v >= 10:
        return "✅ SAĞLIKLI", f"Fiyat {p:+.2f}%, Hacim {v:+.1f}% (eşlik ediyor)"

    return "ℹ️ NÖTR", f"Fiyat {p:+.2f}%, Hacim {v:+.1f}%"


def index_correlation_trap(index_pct: float, m: Dict[str, Any]) -> Tuple[str, str]:
    """
    Endeks düşerken hisse güçlü + hacim artıyorsa: GÜÇLÜ AYRIŞMA.
    Mod-1:
      - index <= -0.80
      - stock >= +0.40
      - volume >= +10
    """
    p = m["price_pct"]
    v = m["vol_pct"]

    if index_pct <= -0.80 and p >= 0.40 and v >= 10:
        return "🧠 GÜÇLÜ AYRIŞMA", f"Endeks {index_pct:+.2f}%, Hisse {p:+.2f}%, Hacim {v:+.1f}% (gizli güç)"
    if index_pct <= -0.80 and p < 0:
        return "⚠️ ENDİKSLE DÜŞÜYOR", f"Endeks {index_pct:+.2f}%, Hisse {p:+.2f}%"
    return "ℹ️ KORELASYON", f"Endeks {index_pct:+.2f}%, Hisse {p:+.2f}%"


def early_exit_intelligence(index_pct: float, m: Dict[str, Any]) -> Tuple[str, str]:
    """
    Erken Çıkış Zekâsı (Mod-1 temkinli):
      - volume up >= +15
      - price small (<= +0.25) OR negative
      - upper wick ratio high (>= 0.45)  -> satış baskısı
      - AND index weakening (<= -0.30) gives extra weight
    """
    p = m["price_pct"]
    v = m["vol_pct"]
    uw = m["wick"]["upper_ratio"]
    body = m["wick"]["body_ratio"]

    # ana sinyal: hacim artıyor ama fiyat gitmiyor + üst fitil baskın
    if v >= 15 and (p <= 0.25) and uw >= 0.45 and body <= 0.35:
        note = f"Hacim {v:+.1f}%, Fiyat {p:+.2f}%, ÜstFitil {uw*100:.0f}%"
        if index_pct <= -0.30:
            return "⚠️ KÂR KORUMA", note + f" | Endeks {index_pct:+.2f}% zayıf"
        return "⚠️ KÂR KORUMA", note

    return "✅ OK", f"ÜstFitil {uw*100:.0f}%, Gövde {body*100:.0f}%"


def score_compose(index_pct: float, m: Dict[str, Any]) -> Tuple[int, List[str]]:
    """
    Ordinaryüs birleştirme skoru (v1):
      - Delta: 0..40
      - Ayrışma: 0..30
      - Erken çıkış riski: -0..-40
    """
    score = 50  # base
    notes = []

    d_tag, d_note = delta_thinking(m)
    a_tag, a_note = index_correlation_trap(index_pct, m)
    e_tag, e_note = early_exit_intelligence(index_pct, m)

    # Delta
    if "GÖRÜNMEYEN TOPLAMA" in d_tag:
        score += 25
    elif "SAĞLIKLI" in d_tag:
        score += 15
    elif "SAHTE" in d_tag:
        score -= 15

    # Ayrışma
    if "GÜÇLÜ AYRIŞMA" in a_tag:
        score += 20
    elif "ENDİKSLE DÜŞÜYOR" in a_tag:
        score -= 10

    # Erken çıkış
    if "KÂR KORUMA" in e_tag:
        score -= 25

    score = max(0, min(100, score))

    notes.append(f"{d_tag} — {d_note}")
    notes.append(f"{a_tag} — {a_note}")
    notes.append(f"{e_tag} — {e_note}")
    return score, notes


# -----------------------
# Telegram Handlers
# -----------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "✅ TAIPO PRO ANIL aktif.\n\n"
        "Komutlar:\n"
        "/ping - test\n"
        "/radar - WATCHLIST_BIST tarama (3'lü sistem)\n"
        "/eod - gün sonu rapor + radar özeti\n"
    )


async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong! Bot çalışıyor.")


def _fmt_line(ticker: str, m: Dict[str, Any], score: int) -> str:
    return f"{ticker:<10} | {m['price_pct']:+.2f}% | V {m['vol_pct']:+.1f}% | Skor {score:>3}"


async def radar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    mode = _env("MODE", "MOD1").upper()  # we keep MOD1 now
    watchlist = parse_watchlist(_env("WATCHLIST_BIST", ""))

    if not watchlist:
        await update.message.reply_text("⚠️ WATCHLIST_BIST boş. Render -> Environment'dan ekle.")
        return

    # Index first
    try:
        idx_chart = yahoo_chart(DEFAULT_INDEX_TICKER, range_="7d", interval="1d")
        idx_m = calc_daily_metrics(idx_chart)
        index_pct = idx_m["price_pct"]
    except Exception as e:
        logger.exception("Index fetch failed")
        await update.message.reply_text(f"⚠️ Endeks verisi alınamadı: {e}")
        return

    results = []
    for t in watchlist:
        try:
            ch = yahoo_chart(t, range_="1mo", interval="1d")
            m = calc_daily_metrics(ch)
            score, notes = score_compose(index_pct, m)
            results.append((t, score, m, notes))
        except Exception as e:
            results.append((t, 0, {"price_pct": 0.0, "vol_pct": 0.0, "wick": {"upper_ratio": 0, "body_ratio": 0}}, [f"⚠️ Veri alınamadı: {e}"]))

    # sort by score desc
    results.sort(key=lambda x: x[1], reverse=True)

    top = results[:5]
    bottom = results[-5:] if len(results) > 5 else []

    header = (
        f"🧠 TAIPO-BİST RADAR (3'lü Sistem v1 | {mode})\n"
        f"📌 Endeks (^XU100): {index_pct:+.2f}%\n\n"
        f"{'HİSSE':<10} | {'Fiyat':>7} | {'Hacim':>8} | Skor\n"
        f"{'-'*42}\n"
    )

    lines = [header]
    for t, score, m, _ in top:
        lines.append(_fmt_line(t, m, score))

    if bottom:
        lines.append("\n⚠️ En Riskli / Zayıf (skor düşük):")
        for t, score, m, _ in bottom:
            lines.append(_fmt_line(t, m, score))

    # Detailed notes for the best one (ordinaryüs kısa ama anlamlı)
    best_t, best_score, best_m, best_notes = top[0]
    lines.append("\n📌 En İyi Aday Detay:")
    lines.append(f"🎯 {best_t} | Skor {best_score}/100 | Fiyat {best_m['price_pct']:+.2f}% | Hacim {best_m['vol_pct']:+.1f}%")
    for n in best_notes:
        lines.append(f"• {n}")

    # If the top is warning for exit, mention
    await update.message.reply_text("\n".join(lines))


async def eod_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    watchlist = parse_watchlist(_env("WATCHLIST_BIST", ""))
    mode = _env("MODE", "MOD1").upper()

    # index
    try:
        idx_chart = yahoo_chart(DEFAULT_INDEX_TICKER, range_="7d", interval="1d")
        idx_m = calc_daily_metrics(idx_chart)
        index_pct = idx_m["price_pct"]
        index_close = idx_m["last_close"]
    except Exception as e:
        logger.exception("Index fetch failed")
        await update.message.reply_text(f"⚠️ Endeks verisi alınamadı: {e}")
        return

    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # radar summary
    radar_lines = []
    if watchlist:
        scored = []
        for t in watchlist:
            try:
                ch = yahoo_chart(t, range_="1mo", interval="1d")
                m = calc_daily_metrics(ch)
                score, notes = score_compose(index_pct, m)
                scored.append((t, score, m, notes))
            except Exception as e:
                scored.append((t, 0, None, [f"⚠️ Veri alınamadı: {e}"]))

        scored.sort(key=lambda x: x[1], reverse=True)

        radar_lines.append("🎯 RADAR (WATCHLIST_BIST)")
        radar_lines.append(f"{'HİSSE':<10} | {'Fiyat':>7} | {'Hacim':>8} | Skor")
        radar_lines.append("-" * 42)
        for t, score, m, _ in scored:
            if m is None:
                radar_lines.append(f"{t:<10} |   n/a  |   n/a  | {score:>3}")
            else:
                radar_lines.append(_fmt_line(t, m, score))

        # add 1-line alerts for any "KÂR KORUMA" or "GÖRÜNMEYEN TOPLAMA"
        alerts = []
        for t, score, m, notes in scored:
            if m is None:
                continue
            joined = " ".join(notes)
            if "KÂR KORUMA" in joined:
                alerts.append(f"⚠️ {t}: KÂR KORUMA (dağıtım/fitil baskısı olabilir)")
            if "GÖRÜNMEYEN TOPLAMA" in joined:
                alerts.append(f"🧠 {t}: GÖRÜNMEYEN TOPLAMA (sessiz hacim artışı)")

        if alerts:
            radar_lines.append("\n🔔 Sinyaller:")
            radar_lines.extend(alerts)

    msg = [
        f"📌 TAIPO EOD RAPOR (3'lü Sistem v1 | {mode})",
        f"🕒 {now}",
        "",
        f"📊 BIST100 (^XU100): {index_close:.2f}  ({index_pct:+.2f}%)",
        "",
    ]
    if radar_lines:
        msg.extend(radar_lines)
        msg.append("")
    msg.append("✅ Altyapı stabil. Sonraki adım: Saatli otomatik EOD + geniş radar.")

    await update.message.reply_text("\n".join(msg))


def main() -> None:
    token = _env("BOT_TOKEN", "")
    if not token:
        logger.error("BOT_TOKEN env missing! Render -> Environment -> BOT_TOKEN kontrol et.")
        sys.exit(1)

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("radar", radar_cmd))
    app.add_handler(CommandHandler("eod", eod_cmd))

    logger.info("✅ TAIPO-BIST starting (3'lü Sistem v1) -> run_polling")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )


if __name__ == "__main__":
    main()
