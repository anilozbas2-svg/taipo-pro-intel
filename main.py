import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")  # Render Environment'da aynen BOT_TOKEN olacak

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "TAIPO PRO INTEL ✅\n\nKomutlar:\n/start\n/ping"
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅ Bot çalışıyor.")

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env değişkeni yok. Render > Environment'a ekle.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))

    # Webhook değil, polling kullanıyoruz
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
