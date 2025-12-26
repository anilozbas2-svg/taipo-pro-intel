import os
import logging
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("taipo-pro-intel")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "TAIPO PRO INTEL ✅\n"
        "Elite radar for silent accumulation, divergence & profit protection.\n"
        "Komutlar: /start /ping"
    )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("pong ✅ Bot ayakta.")


def main() -> None:
    if not TOKEN:
        raise ValueError("BOT_TOKEN env variable is missing. Render -> Settings -> Environment'a ekle.")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))

    logger.info("Bot is starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
