import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# LOGGING
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ TAIPO PRO (BIST) aktif!\n\n"
        "Komutlar:\n"
        "/start - Başlat\n"
        "/ping - Test\n"
        "/help - Yardım\n"
    )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📌 Yardım\n\n"
        "Şu an sadece temel test modu açık.\n"
        "Komutlar:\n"
        "/start\n"
        "/ping\n"
        "/help\n\n"
        "Sonraki adım: /eod (BIST kapanış raporu) ekleyeceğiz."
    )


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN tanımlı değil (Render Environment Variables içine ekle)")

    # TEK UYGULAMA
    application = Application.builder().token(BOT_TOKEN).build()

    # HANDLERS
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ping", ping))
    application.add_handler(CommandHandler("help", help_cmd))

    logger.info("✅ Bot polling başlıyor... (tek instance / tek run_polling)")

    # TEK POLLING
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
