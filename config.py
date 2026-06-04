import os

BOT_TOKEN = "8623192862:AAFyABLsnXuOhVO3IsdjkZjDPUkvc8k6OxA"

CSV_PATH = os.getenv("CSV_PATH", "inn.csv")
DB_PATH = os.getenv("DB_PATH", "phones.db")

USE_WHITELIST = os.getenv("USE_WHITELIST", "false").lower() == "true"

WHITELIST_USER_IDS = {
    int(x)
    for x in os.getenv("WHITELIST_USER_IDS", "").split(",")
    if x.strip()
}
