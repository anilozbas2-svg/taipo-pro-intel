import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


WELCOME = (
    "🛡️ TAIPO PRO INTEL aktif.\n\n"
    "Bu bir *giriş sinyali* botu değildir.\n"
    "Odak: *Sessiz toplama* • *Güçlü ayrışma* • *Kâr koruma*.\n\n"
    "Komutlar:\n"
    "/radar — Elite radar listesi\n"
    "/status — Rejim & tarama durumu\n"
    "/help — Sistem nasıl çalışır\n"
)

HELP_TEXT = (
    "TAIPO PRO INTEL çekirdek 3 kural:\n"
    "1) Delta Thinking\n"
    "2) Endeks Korelasyon Tuzağı (Güçlü Ayrışma)\n"
    "3) Erken Çıkış Zekâsı (Kâr Koruma)\n\n"
    "Not: Mesajlar *giriş önerisi* değildir. Radar amaçlıdır."
)

STATUS_TEXT = (
    "📡 Durum: DEV MODE\n"
    "Tarama: Kapalı (v0)\n"
    "Rejim: Tanımsız\n\n"
    "Sonraki adım: Delta/Korelasyon/Erken Çıkış modülleri bağlanacak."
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_markdown(WELCOME)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(STATUS_TEXT)


async def radar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Şimdilik demo mesajı. Sonra gerçek radar buraya gelecek.
    msg = (
        "🔎 *ELİT RADAR (Demo)*\n\n"
        "Şu an tarama motoru hazırlık aşamasında.\n"
        "Yakında: Sessiz toplama + güçlü ayrışma adayları burada listelenecek."
    )
    await update.message.reply_markdown(msg)


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN bulunamadı. .env dosyanı kontrol et.")

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("radar", radar))

    print("TAIPO PRO INTEL bot running...")
    app.run_polling()


if __name__ == "__main__":
    main()
