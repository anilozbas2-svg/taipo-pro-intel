import os
import logging
import requests
from datetime import datetime, timezone, timedelta

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# CONFIG
# ----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

# Default lists (can be overridden via ENV)
DEFAULT_WATCHLIST_BIST = os.getenv("WATCHLIST_BIST", "THYAO.IS,AKBNK.IS,ASELS.IS").strip()
DEFAULT_WATCHLIST_CRYPTO = os.getenv("WATCHLIST_CRYPTO", "bitcoin,ethereum,solana").strip()

# Optional formatting
CURRENCY_BIST = os.getenv("BIST_CURRENCY", "TRY").strip()  # Yahoo returns TRY for .IS usually
CRYPTO_VS = os.getenv("CRYPTO_VS", "usd").strip()

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("taipo-pro-intel")

TR_TZ = timezone(timedelta(hours=3))

# ----------------------------
# HELPERS
# ----------------------------
def now_tr_str() -> str:
    return datetime.now(TR_TZ).strftime("%d.%m.%Y %H:%M")

def parse_csv_symbols(s: str) -> list[str]:
    items = [x.strip() for x in (s or "").split(",")]
    return [x for x in items if x]

def fmt_num(x, decimals=2):
    try:
        return f"{float(x):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(x)

def pct(x, decimals=2):
    try:
        return f"{float(x):.{decimals}f}%"
    except Exception:
        return str(x)

def sign_emoji(pct_val: float) -> str:
    if pct_val > 0:
        return "🟢"
    if pct_val < 0:
        return "🔴"
    return "⚪️"

# ----------------------------
# DATA PROVIDERS
# ----------------------------
def fetch_bist_quotes_yahoo(symbols: list[str]) -> list[dict]:
    """
    Uses an unofficial Yahoo Finance quote endpoint.
    If Yahoo blocks/rate-limits, this may fail sometimes.
    """
    if not symbols:
        return []

    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols)}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    results = data.get("quoteResponse", {}).get("result", []) or []
    out = []
    for q in results:
        sym = q.get("symbol", "")
        name = q.get("shortName") or q.get("longName") or sym

        last = q.get("regularMarketPrice")
        chg = q.get("regularMarketChange")
        chg_pct = q.get("regularMarketChangePercent")
        prev = q.get("regularMarketPreviousClose")
        day_low = q.get("regularMarketDayLow")
        day_high = q.get("regularMarketDayHigh")
        vol = q.get("regularMarketVolume")
        cur = q.get("currency") or CURRENCY_BIST

        out.append({
            "symbol": sym,
            "name": name,
            "price": last,
            "change": chg,
            "change_pct": chg_pct,
            "prev_close": prev,
            "day_low": day_low,
            "day_high": day_high,
            "volume": vol,
            "currency": cur,
        })
    return out

def fetch_crypto_coingecko(ids: list[str], vs: str = "usd") -> list[dict]:
    """
    CoinGecko public endpoint (no API key).
    """
    if not ids:
        return []

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs,
        "ids": ",".join(ids),
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    arr = r.json()

    out = []
    for c in arr:
        out.append({
            "id": c.get("id"),
            "symbol": (c.get("symbol") or "").upper(),
            "name": c.get("name") or c.get("id"),
            "price": c.get("current_price"),
            "change_pct_24h": c.get("price_change_percentage_24h"),
            "high_24h": c.get("high_24h"),
            "low_24h": c.get("low_24h"),
            "volume": c.get("total_volume"),
            "mcap": c.get("market_cap"),
        })
    return out

# ----------------------------
# BOT COMMANDS
# ----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("mode", "bist")
    await update.message.reply_text(
        "✅ TAIPO PRO INTEL aktif!\n\n"
        "Komutlar:\n"
        "/start - Başlat\n"
        "/ping - Test\n"
        "/help - Yardım\n"
        "/mode bist - BIST modu\n"
        "/mode crypto - Crypto modu\n"
        "/eod - Gün sonu raporu (seçili moda göre)\n"
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = context.user_data.get("mode", "bist")
    await update.message.reply_text(
        f"🧠 Komutlar:\n"
        f"/mode bist\n"
        f"/mode crypto\n"
        f"/eod\n\n"
        f"Şu anki mod: **{mode}**\n"
        f"Tarih/Saat (TR): {now_tr_str()}",
        parse_mode="Markdown"
    )

async def mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Kullanım: /mode bist  veya  /mode crypto")
        return

    m = context.args[0].strip().lower()
    if m not in ("bist", "crypto"):
        await update.message.reply_text("Geçersiz mod. Kullanım: /mode bist  veya  /mode crypto")
        return

    context.user_data["mode"] = m
    await update.message.reply_text(f"✅ Mod ayarlandı: {m}")

async def eod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = context.user_data.get("mode", "bist")

    # Optional: allow /eod bist or /eod crypto as override
    if context.args:
        arg_mode = context.args[0].strip().lower()
        if arg_mode in ("bist", "crypto"):
            mode = arg_mode

    try:
        if mode == "crypto":
            ids = parse_csv_symbols(DEFAULT_WATCHLIST_CRYPTO)
            rows = fetch_crypto_coingecko(ids, vs=CRYPTO_VS)
            if not rows:
                await update.message.reply_text("Crypto listesi boş. WATCHLIST_CRYPTO env ayarla.")
                return

            lines = []
            lines.append(f"📌 **TAIPO EOD – CRYPTO**  ({now_tr_str()} TR)")
            lines.append(f"Liste: {', '.join(ids)}")
            lines.append("")

            for c in rows:
                p = c.get("price")
                ch = c.get("change_pct_24h")
                ch_val = float(ch) if ch is not None else 0.0
                em = sign_emoji(ch_val)

                lines.append(
                    f"{em} **{c.get('symbol')}** ({c.get('name')})\n"
                    f"• Fiyat: {fmt_num(p, 4)} {CRYPTO_VS.upper()}\n"
                    f"• 24s Değişim: {pct(ch_val, 2)}\n"
                    f"• 24s Aralık: {fmt_num(c.get('low_24h'), 4)} – {fmt_num(c.get('high_24h'), 4)}\n"
                    f"• Hacim: {fmt_num(c.get('volume'), 0)}\n"
                )

            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

        else:
            symbols = parse_csv_symbols(DEFAULT_WATCHLIST_BIST)
            rows = fetch_bist_quotes_yahoo(symbols)
            if not rows:
                await update.message.reply_text("BIST listesi boş. WATCHLIST_BIST env ayarla.")
                return

            lines = []
            lines.append(f"📌 **TAIPO EOD – BIST**  ({now_tr_str()} TR)")
            lines.append(f"Liste: {', '.join(symbols)}")
            lines.append("")

            for q in rows:
                chp = q.get("change_pct")
                chp_val = float(chp) if chp is not None else 0.0
                em = sign_emoji(chp_val)
                cur = q.get("currency") or CURRENCY_BIST

                lines.append(
                    f"{em} **{q.get('symbol')}**\n"
                    f"• Fiyat: {fmt_num(q.get('price'), 2)} {cur}\n"
                    f"• Günlük: {pct(chp_val, 2)}  (Δ {fmt_num(q.get('change'), 2)})\n"
                    f"• Gün Aralığı: {fmt_num(q.get('day_low'), 2)} – {fmt_num(q.get('day_high'), 2)}\n"
                    f"• Hacim: {fmt_num(q.get('volume'), 0)}\n"
                )

            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    except requests.exceptions.HTTPError as e:
        log.exception("HTTPError")
        await update.message.reply_text(
            "⚠️ Veri kaynağından cevap alınamadı (HTTP).\n"
            "Bu genelde geçici olur. 1-2 dk sonra tekrar /eod dene.\n"
            f"Hata: {str(e)}"
        )
    except Exception as e:
        log.exception("EOD error")
        await update.message.reply_text(
            "⚠️ EOD çalışırken hata oldu.\n"
            f"Hata: {str(e)}"
        )

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env yok. Render > Environment'tan ekle.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("mode", mode_cmd))
    app.add_handler(CommandHandler("eod", eod))

    log.info("Bot starting (polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
