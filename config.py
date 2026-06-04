import os

# ====== Telegram ======
BOT_TOKEN = os.getenv("TG_BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError(
        "Не задан токен Telegram-бота. "
        "Добавьте переменную окружения TELEGRAM_BOT_TOKEN на сервере Bothost."
    )

# ====== Data files ======
CSV_PATH = os.getenv("CSV_PATH", "inn.csv")
DB_PATH = os.getenv("DB_PATH", "phones.db")

# ====== Access control ======
USE_WHITELIST = os.getenv("USE_WHITELIST", "false").lower() == "true"

WHITELIST_USER_IDS = {
    int(x)
    for x in os.getenv("WHITELIST_USER_IDS", "").split(",")
    if x.strip()
}
