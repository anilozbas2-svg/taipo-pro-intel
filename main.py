import os
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# LOGGING
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("taipo-bist-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN")


def format_radar_message() -> str:
    """
    Şimdilik TEST RADAR (dummy).
    Sonraki adımda burayı gerçek BIST verisiyle dolduracağız.
    """
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    lines = [
        "🎯 *TAIPO RADAR (TEST MODU)*",
        f"🕒 {now}",
        "",
        "✅ İzleme Listesi (örnek):",
        "1) THYAO — Momentum +",
        "2) ASELS — Kırılım izleme",
        "3) SISE — Dipten toparlanma",
        "4) KCHOL — Trend takibi",
        "5) SASA — Volatil takip",
        "",
        "⚠️ Not: Bu liste şu an TEST amaçlıdır.",
        "Sonraki adım: gerçek veri + filtreler + skor.",
    ]
    return "\n".join(lines)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ TAIPO PRO (BIST) aktif!\n\n"
        "Komutlar:\n"
        "/start - Başlat\n"
        "/ping - Test\n"
        "/help - Yardım\n"
        "/radar - Radar (test)\n"
    )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📌 Yardım\n\n"
        "Komutlar:\n"
        "/start\n"
        "/ping\n"
        "/help\n"
        "/radar\n\n"
        "Şu an test modundayız. Radar çalışması doğruysa\n"
        "sonraki adımda gerçek BIST verisini bağlayacağız."
    )


async def radar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = format_radar_message()
    # Markdown kullanıyoruz (yıldızlar vs.)
    await update.message.reply_text(msg, parse_mode="Markdown")


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN tanımlı değil (Render Environment -> BOT_TOKEN).")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("radar", radar))

    logger.info("✅ Bot polling başlıyor...")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
