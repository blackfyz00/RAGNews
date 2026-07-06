import os
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler

load_dotenv() 

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

async def start(update: Update, context):
    keyboard = [[InlineKeyboardButton("Запросить новости", callback_data='news')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text('Привет! Нажми кнопку:', reply_markup=reply_markup)

async def button_handler(update: Update, context):
    query = update.callback_query
    if query.data == 'news':
        await query.answer()
        await query.edit_message_text(text="Ок")
        print("Пользователь запросил новости!")

def main():
    application = ApplicationBuilder().token(TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))

    application.run_polling()

if __name__ == '__main__':
    main()