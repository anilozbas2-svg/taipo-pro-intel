import os
import logging

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "✅ TAIPO PRO INTEL aktif!\n\n"
        "Komutlar:\n"
        "/start - Başlat\n"
        "/ping - Test\n"
        "/help - Yardım"
    )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("pong ✅")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📌 Komutlar:\n"
        "/start\n"
        "/ping\n"
        "/help\n\n"
        "Bot Render üzerinde çalışıyor."
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling an update:", exc_info=context.error)


def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN environment variable is missing")

    app = Application.builder().token(token).build()

    # Komutlar
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_cmd))

    # Hata yakalama
    app.add_error_handler(error_handler)

    logger.info("✅ TAIPO PRO INTEL başladı (Polling)")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
