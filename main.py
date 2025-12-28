import os
import sys
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("TAIPO_PRO_ANIL")

# -----------------------
# Handlers (PTB v22.1 async)
# -----------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "✅ TAIPO PRO ANIL aktif.\n\n"
        "Komutlar:\n"
        "/ping - test\n"
        "/eod - gün sonu rapor (taslak)"
    )

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏓 Pong! Bot çalışıyor.")

async def eod_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    msg = (
        f"📌 TAIPO EOD RAPOR (Taslak)\n"
        f"🕒 {now}\n\n"
        f"1) Günün genel yönü: (yakında)\n"
        f"2) Radar hisseler: (yakında)\n"
        f"3) Haber etkisi: (yakında)\n"
        f"4) Yarın için not: (yakında)\n\n"
        f"✅ Altyapı stabil. Sonraki adım: veri kaynaklarını bağlamak."
    )
    await update.message.reply_text(msg)

def main() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        logger.error("BOT_TOKEN env missing! Render -> Environment -> BOT_TOKEN kontrol et.")
        sys.exit(1)

    app = ApplicationBuilder().token(token).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("ping", ping_cmd))
    app.add_handler(CommandHandler("eod", eod_cmd))

    logger.info("✅ Bot starting (python-telegram-bot==22.1) -> run_polling")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )

if __name__ == "__main__":
    main()
