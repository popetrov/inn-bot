import asyncio
import json
import logging
import os
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

from db import phone_key, split_phones


SPREADSHEET_ID = "1I_6u61p4fTMNqfZnbXS3kX3NKTJ7fJlL9JgXK4zDiPA"
SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
USERNAME = re.compile(r"[A-Za-z0-9_]{5,32}")
DEFAULT_INTERVAL = 900


def parse_contacts(rows):
    headers = [str(value).strip().casefold() for value in rows[0]] if rows else []
    required = ("инн", "фио", "сотовые", "username")
    if any(headers.count(name) != 1 for name in required):
        raise ValueError("Unexpected DVK sheet headers")
    indices = [headers.index(name) for name in required]

    contacts = {}
    for row in rows[1:]:
        inn, name, phones, usernames = (
            str(row[index]).strip() if index < len(row) and row[index] is not None else ""
            for index in indices
        )
        if not re.fullmatch(r"(?:[0-9]{10}|[0-9]{12})", inn):
            continue

        entries = contacts.setdefault(inn, {"phones": {}, "usernames": {}})
        for phone in split_phones(phones):
            key = phone_key(phone)
            if key:
                entries["phones"].setdefault(key, f"{name or 'Контакт ДВК'}: {phone}")
        for raw in re.split(r"[\s,;|]+", usernames):
            username = re.sub(r"^(?:https?://)?(?:t\.me|telegram\.me)/", "", raw, flags=re.I)
            username = username.removeprefix("@")
            if USERNAME.fullmatch(username):
                entries["usernames"].setdefault(username.lower(), f"@{username}")

    return {
        inn: (list(entries["phones"].values()), list(entries["usernames"].values()))
        for inn, entries in contacts.items()
        if entries["phones"] or entries["usernames"]
    }


class DvkContacts:
    def __init__(self, credentials_json, cache_path):
        self.credentials_json = credentials_json
        self.cache_path = Path(cache_path)

    @classmethod
    def from_environment(cls):
        credentials_json = os.environ.get("DVK_GOOGLE_SERVICE_ACCOUNT_JSON")
        if not credentials_json:
            return None
        default_dir = Path("/app/data") if Path("/app/data").is_dir() else Path(".")
        return cls(credentials_json, os.environ.get("DVK_CACHE_PATH") or default_dir / "dvk_contacts.db")

    def sync(self):
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2.service_account import Credentials

        credentials = Credentials.from_service_account_info(
            json.loads(self.credentials_json), scopes=[SCOPE],
        )
        cells = quote("'База'!A:J", safe="")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{cells}"
        with AuthorizedSession(credentials, refresh_timeout=15) as session:
            response = session.get(url, params={"valueRenderOption": "FORMATTED_VALUE"}, timeout=30)
            response.raise_for_status()
            contacts = parse_contacts(response.json()["values"])
        if not contacts:
            raise ValueError("DVK sheet contains no contacts")

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.cache_path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS contacts (inn TEXT PRIMARY KEY, data TEXT NOT NULL)")
            db.execute("DELETE FROM contacts")
            db.executemany(
                "INSERT INTO contacts (inn, data) VALUES (?, ?)",
                ((inn, json.dumps(data, ensure_ascii=False)) for inn, data in contacts.items()),
            )
        logging.info("DVK sync succeeded | inns=%d", len(contacts))

    async def run(self):
        while True:
            try:
                await asyncio.to_thread(self.sync)
            except Exception as error:
                logging.warning("DVK sync failed | error_type=%s", type(error).__name__)
            await asyncio.sleep(DEFAULT_INTERVAL)

    def lookup(self, inn):
        if not self.cache_path.is_file():
            return [], []
        with closing(sqlite3.connect(self.cache_path)) as db:
            row = db.execute("SELECT data FROM contacts WHERE inn = ?", (inn,)).fetchone()
        return json.loads(row[0]) if row else ([], [])


def merge_phones(existing, additional):
    merged = list(existing or ())
    seen = {phone_key(item.rsplit(": ", 1)[-1]) for item in merged}
    for item in additional:
        key = phone_key(item.rsplit(": ", 1)[-1])
        if key not in seen:
            seen.add(key)
            merged.append(item)
    return merged


def contact_messages(phones, usernames):
    lines = []
    if phones:
        lines.extend(["Телефоны:", *phones])
    if usernames:
        if lines:
            lines.append("")
        lines.extend(["Telegram:", *usernames])

    # Telegram limits message length. Keep contact lines together when possible.
    limit = 4000
    message = ""
    for line in lines:
        separator = "\n" if message else ""
        if len(message) + len(separator) + len(line) > limit:
            if message:
                yield message
            message = ""
            while len(line) > limit:
                yield line[:limit]
                line = line[limit:]
        message = f"{message}\n{line}" if message else line
    if message:
        yield message
