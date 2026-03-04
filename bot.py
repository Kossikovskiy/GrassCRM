
import os
import sys
import asyncio
from telegram import Bot
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# --- ВАЖНО: Импортируем модели из main.py ---
# Это гарантирует, что мы работаем с той же структурой базы данных
from main import Deal, Stage, DATABASE_URL

# --- Настройки ---
# Эти значения должны быть установлены как переменные окружения на вашем хостинге
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Проверяем, что DATABASE_URL установлена (она импортируется из main.py)
if not DATABASE_URL:
    print("Ошибка: Переменная окружения DATABASE_URL не установлена.")
    sys.exit(1)

async def send_daily_summary():
    """Основная функция: получает данные и отправляет сообщение в Telegram."""

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Ошибка: Переменные окружения TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID должны быть установлены.")
        return

    print("Подключаюсь к базе данных PostgreSQL...")
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)

    message_lines = ["🔥 **Ежедневная сводка по сделкам в работе:**\n"]

    try:
        with Session() as session:
            # Выбираем сделки, которые НЕ находятся в конечных стадиях
            stmt = (
                select(Deal)
                .join(Stage)
                .where(Stage.is_final == False)
                .order_by(Stage.order)
            )
            deals_in_progress = session.execute(stmt).scalars().all()

            if not deals_in_progress:
                print("Сделок в работе не найдено. Сообщение не отправлено.")
                # Можно раскомментировать, если нужно получать сообщение, даже когда сделок нет
                # await Bot(token=TELEGRAM_BOT_TOKEN).send_message(chat_id=TELEGRAM_CHAT_ID, text="Сделок в работе на сегодня нет.")
                return

            print(f"Найдено сделок в работе: {len(deals_in_progress)}")

            current_stage = ""
            for deal in deals_in_progress:
                if deal.stage.name != current_stage:
                    current_stage = deal.stage.name
                    # Используем MarkdownV2 для форматирования
                    message_lines.append(f"\n*{current_stage}*" )
                
                # Форматируем каждую сделку, экранируя спецсимволы
                title = deal.title.replace("-", "\\-")
                client = deal.client.replace("-", "\\-")
                message_lines.append(f"  - {title} \\(Клиент: {client}\\)")

        # Инициализируем бота
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        # Собираем и отправляем сообщение
        final_message = "\n".join(message_lines)
        print(f"Отправляю сообщение в чат {TELEGRAM_CHAT_ID}...")
        
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, 
            text=final_message, 
            parse_mode='MarkdownV2'
        )
        
        print("Сообщение успешно отправлено!")

    except Exception as e:
        print(f"Произошла ошибка при работе с ботом: {e}")
        # Можно добавить отправку ошибки в Telegram для отладки
        try:
            error_bot = Bot(token=TELEGRAM_BOT_TOKEN)
            await error_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"Ошибка в работе Telegram-бота: {e}")
        except Exception as e_send:
            print(f"Не удалось даже отправить сообщение об ошибке: {e_send}")

if __name__ == "__main__":
    print("Запускаю отправку сводки вручную...")
    asyncio.run(send_daily_summary())
