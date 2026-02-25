import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import CommandStart

from config import BOT_TOKEN, USE_WHITELIST, WHITELIST_USER_IDS
# ВАЖНО: твой готовый db.py должен содержать эти функции:
# - ensure_db_fresh()  -> проверяет обновление inn.csv и обновляет БД
# - get_items_by_inn() -> возвращает None (если ИНН не найден) или список строк "ФИО: телефон"
from db import ensure_db_fresh, get_items_by_inn

# Логи пишем в файл logs.txt (в папке проекта)
logging.basicConfig(
    level=logging.INFO,
    filename="logs.txt",
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def is_valid_inn(text: str) -> bool:
    t = text.strip()
    return t.isdigit() and len(t) in (10, 12)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("Отправьте ИНН компании, и я покажу телефоны, если они есть в базе.")

@dp.message()
async def handle_inn(message: Message):
    # Ограничение доступа (если включили whitelist)
    if USE_WHITELIST and message.from_user and message.from_user.id not in WHITELIST_USER_IDS:
        return

    text = (message.text or "").strip()

    # Валидация ИНН
    if not is_valid_inn(text):
        logging.info(f"user={message.from_user.id if message.from_user else None} | invalid_inn | text={text}")
        await message.answer("Введите корректный ИНН (10 или 12 цифр).")
        return

    inn = text

    try:
        # ВАЖНО: это учитывает ежедневное обновление inn.csv
        # Если файл обновился — БД автоматически пересоберётся/обновится.
        await ensure_db_fresh()

        items = await get_items_by_inn(inn)
    except Exception as e:
        logging.exception(f"error | inn={inn} | err={e}")
        await message.answer("Техническая ошибка. Попробуйте позже.")
        return

    # 3 сценария ответа по ТЗ
    if items is None:
        logging.info(f"inn={inn} | result=A_not_found")
        await message.answer("ИНН не найден.")
        return

    if not items:
        logging.info(f"inn={inn} | result=B_no_phones")
        await message.answer("По данному ИНН номеров нет.")
        return

    logging.info(f"inn={inn} | result=C_found | count={len(items)}")
    # items уже содержит строки вида "ФИО: телефон"
    await message.answer("Телефоны:\n" + "\n".join(items))

async def main():
    # При запуске тоже проверим актуальность базы (на случай, если inn.csv обновили ночью)
    await ensure_db_fresh()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())