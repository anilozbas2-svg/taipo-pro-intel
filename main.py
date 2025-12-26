import os
import logging

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# =========================
# 🔥 İMZA TESTİ (ÇOK ÖNEMLİ)
# =========================
print("✅ TAIPO PRO INTEL - NEW MAIN.PY - 2025 - SIGNATURE OK ✅")

# =========================
# LOG AYARLARI
# =========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================
# KOMUTLAR
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "✅ TAIPO PRO INTEL aktif!\n\n"
        "Komutlar:\n"
        "/start - Başlat\n"
        "/ping - Test\n"
        "/help - Yardım"
    )

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("pong 🟢")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📌 Komutlar:\n"
        "/start\n"
        "/ping\n"
        "/help\n\n"
        "Bot Render üzerinde çalışıyor 🚀"
    )

# =========================
# HATA YAKALAMA
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling an update:", exc_info=context.error)

# =========================
# MAIN
# =========================
def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("❌ BOT_TOKEN environment variable is missing!")

    app = Application.builder().token(token).build()

    # Handler'lar
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("help", help_cmd))

    # Error handler
    app.add_error_handler(error_handler)

    # Polling
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )

if __name__ == "__main__":
    main()
