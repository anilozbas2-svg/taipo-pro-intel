import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ TAIPO PRO INTEL çalışıyor gardaşım! /taipo yaz.")

def main():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN environment variable is missing!")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))

    logging.info("Bot starting with polling...")
    app.run_polling()

if __name__ == "__main__":
    main()
