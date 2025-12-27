import os
import logging

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("taipo-pro-intel")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ TAIPO PRO INTEL aktif!\n\nKomutlar:\n/start - Başlat\n/ping - Test\n/help - Yardım"
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📌 Komutlar:\n/start - Başlat\n/ping - Test\n/help - Yardım\n\n"
        "Yakında: /radar /eod /mode bist|crypto"
    )

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env yok. Render > Environment'dan ekle.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_cmd))

    log.info("Bot starting (polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
