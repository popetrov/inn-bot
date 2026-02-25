import csv
import json
import os
import re
import time
import logging
import aiosqlite
from typing import Dict, List, Optional, Tuple

from config import DB_PATH, CSV_PATH

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    filename="logs.txt",
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ----------------------------
# DB schema
# ----------------------------
CREATE_COMPANIES_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    inn TEXT PRIMARY KEY,
    items_json TEXT
);
"""

CREATE_META_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

# ----------------------------
# Column patterns
# ----------------------------
DIRECTOR_PHONES_RE = re.compile(r"^director_(\d+)_phones$")
DIRECTOR_FIO_RE = re.compile(r"^director_(\d+)_fio$")

FOUNDER_PHONES_RE = re.compile(r"^founder_(\d+)_phones$")
FOUNDER_FIO_RE = re.compile(r"^founder_(\d+)_fio$")


# ----------------------------
# Helpers
# ----------------------------
def split_phones(raw: str) -> List[str]:
    """Split phones by common separators."""
    if not raw:
        return []
    raw = raw.replace("\n", ",")
    raw = raw.replace(";", ",").replace("|", ",")
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def phone_key(phone: str) -> str:
    """Key for dedupe: digits only."""
    return "".join(ch for ch in phone if ch.isdigit())


def safe_int(s: str) -> int:
    try:
        return int(s)
    except Exception:
        return 10**9


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_COMPANIES_SQL)
        await db.execute(CREATE_META_SQL)
        await db.commit()


async def set_meta(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        await db.commit()


async def get_meta(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = await cur.fetchone()
    return row[0] if row else None


def _discover_pairs(fieldnames: List[str]) -> Tuple[List[Tuple[str, Optional[str], str]], List[Tuple[str, Optional[str], str]]]:
    """
    Returns:
      director_pairs: list of (idx, fio_col_or_None, phones_col)
      founder_pairs:  list of (idx, fio_col_or_None, phones_col)
    """
    director_map: Dict[str, List[Optional[str]]] = {}  # idx -> [fio_col, phones_col]
    founder_map: Dict[str, List[Optional[str]]] = {}

    for name in fieldnames:
        m = DIRECTOR_PHONES_RE.match(name)
        if m:
            idx = m.group(1)
            director_map.setdefault(idx, [None, None])[1] = name

        m = DIRECTOR_FIO_RE.match(name)
        if m:
            idx = m.group(1)
            director_map.setdefault(idx, [None, None])[0] = name

        m = FOUNDER_PHONES_RE.match(name)
        if m:
            idx = m.group(1)
            founder_map.setdefault(idx, [None, None])[1] = name

        m = FOUNDER_FIO_RE.match(name)
        if m:
            idx = m.group(1)
            founder_map.setdefault(idx, [None, None])[0] = name

    director_pairs = [(idx, cols[0], cols[1]) for idx, cols in director_map.items() if cols[1]]
    founder_pairs = [(idx, cols[0], cols[1]) for idx, cols in founder_map.items() if cols[1]]

    director_pairs.sort(key=lambda x: safe_int(x[0]))
    founder_pairs.sort(key=lambda x: safe_int(x[0]))

    return director_pairs, founder_pairs


def _write_duplicates_report(inn_counts: Dict[str, int], duplicates: List[str]) -> None:
    """Write duplicates report to file duplicates_inn.txt."""
    with open("duplicates_inn.txt", "w", encoding="utf-8") as rep:
        rep.write("Дубли ИНН в inn.csv (company_inn):\n")
        for inn in duplicates:
            rep.write(f"{inn}; count={inn_counts.get(inn, 0)}\n")


async def rebuild_db_from_csv():
    """
    Full rebuild DB from CSV.
    - Checks duplicates by company_inn and writes duplicates_inn.txt
    - Merges multiple rows with same INN (unions phones)
    - Stores list of strings 'ФИО: телефон' as JSON
    """
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Не найден файл {CSV_PATH}")

    start_ts = time.time()
    logging.info("CSV rebuild started")

    # Read CSV fully into memory (needed for duplicate count + stable merge)
    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        fieldnames = reader.fieldnames or []

        if "company_inn" not in fieldnames:
            raise ValueError("В CSV нет колонки company_inn")

        director_pairs, founder_pairs = _discover_pairs(fieldnames)
        if not director_pairs and not founder_pairs:
            raise ValueError("Не найдены колонки director_*_phones и founder_*_phones")

        rows = list(reader)

    # Duplicate INN check
    inn_counts: Dict[str, int] = {}
    for row in rows:
        inn = (row.get("company_inn") or "").strip()
        if not inn:
            continue
        inn_counts[inn] = inn_counts.get(inn, 0) + 1

    duplicates = sorted([inn for inn, c in inn_counts.items() if c > 1])
    if duplicates:
        _write_duplicates_report(inn_counts, duplicates)
        logging.warning(f"Найдены дубли ИНН: {len(duplicates)} шт. (см. duplicates_inn.txt)")
    else:
        # если дублей нет, можно оставить старый файл или перезаписать пустым — перезапишем “чистым”
        with open("duplicates_inn.txt", "w", encoding="utf-8") as rep:
            rep.write("Дубли ИНН не найдены.\n")

    # Merge rows by INN
    inn_to_items: Dict[str, List[str]] = {}
    inn_to_seenphones: Dict[str, set] = {}  # inn -> set(phone_key)

    for row in rows:
        inn = (row.get("company_inn") or "").strip()
        if not inn:
            continue

        items = inn_to_items.setdefault(inn, [])
        seen = inn_to_seenphones.setdefault(inn, set())

        # directors
        for _, fio_col, phones_col in director_pairs:
            fio = (row.get(fio_col) or "").strip() if fio_col else ""
            phones_raw = row.get(phones_col) or ""
            for ph in split_phones(phones_raw):
                k = phone_key(ph)
                if not k or k in seen:
                    continue
                seen.add(k)
                label = fio if fio else "Директор"
                items.append(f"{label}: {ph.strip()}")

        # founders
        for _, fio_col, phones_col in founder_pairs:
            fio = (row.get(fio_col) or "").strip() if fio_col else ""
            phones_raw = row.get(phones_col) or ""
            for ph in split_phones(phones_raw):
                k = phone_key(ph)
                if not k or k in seen:
                    continue
                seen.add(k)
                label = fio if fio else "Учредитель"
                items.append(f"{label}: {ph.strip()}")

    # Recreate companies table (safe rebuild)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DROP TABLE IF EXISTS companies")
        await db.execute(CREATE_COMPANIES_SQL)

        for inn, items in inn_to_items.items():
            items_json = json.dumps(items, ensure_ascii=False)
            await db.execute(
                "INSERT INTO companies (inn, items_json) VALUES (?, ?)",
                (inn, items_json),
            )

        await db.commit()

    # Save metadata (mtime)
    mtime = str(int(os.path.getmtime(CSV_PATH)))
    await init_db()
    await set_meta("csv_mtime", mtime)
    await set_meta("last_rebuild_ts", str(int(time.time())))

    elapsed = time.time() - start_ts
    logging.info(f"CSV rebuild finished | inns={len(inn_to_items)} | duplicates={len(duplicates)} | seconds={elapsed:.2f}")


async def ensure_db_fresh():
    """
    If inn.csv changed -> rebuild DB.
    Called on startup and before each request in bot.py.
    """
    await init_db()

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Не найден файл {CSV_PATH}")

    current_mtime = str(int(os.path.getmtime(CSV_PATH)))
    saved_mtime = await get_meta("csv_mtime")

    if saved_mtime != current_mtime:
        await rebuild_db_from_csv()


async def get_items_by_inn(inn: str) -> Optional[List[str]]:
    """
    None -> ИНН не найден
    []   -> ИНН найден, но элементов нет
    [..] -> список строк "ФИО: телефон"
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT items_json FROM companies WHERE inn = ?", (inn,))
        row = await cur.fetchone()

    if row is None:
        return None

    items_json = row[0]
    if not items_json:
        return []

    try:
        items = json.loads(items_json)
        if isinstance(items, list):
            return [str(x) for x in items if str(x).strip()]
        return []
    except Exception:
        return []