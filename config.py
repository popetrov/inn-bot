# ====== Telegram ======
BOT_TOKEN = "8623192862:AAFL0INyUNEDiprhwQb3zjSzvle63VG2bj8"

# ====== Data files ======
CSV_PATH = "inn.csv"       # файл обновляется ежедневно (кладёшь новый поверх старого)
DB_PATH = "phones.db"      # SQLite база (создаётся автоматически)

# ====== Access control (опционально) ======
# Если нужно ограничить доступ к боту по Telegram user_id:
USE_WHITELIST = False
WHITELIST_USER_IDS = {123456789}  # сюда можно добавить свой user_id