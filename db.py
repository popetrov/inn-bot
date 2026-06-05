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
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Не найден файл {CSV_PATH}")

    start_ts = time.time()
    logging.info("CSV rebuild started")
    print("CSV rebuild started", flush=True)

    temp_db_path = DB_PATH + ".tmp"

    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)

    inn_to_items: Dict[str, List[str]] = {}
    inn_to_seenphones: Dict[str, set] = {}
    inn_counts: Dict[str, int] = {}

    total_rows = 0
    valid_inn_rows = 0
    bad_inn_rows = 0

    with open_csv_strict_utf8(CSV_PATH) as f:
        reader = csv.DictReader(f, delimiter=";")

        raw_fieldnames = reader.fieldnames or []
        fieldnames = normalize_fieldnames(raw_fieldnames)
        reader.fieldnames = fieldnames

        if not fieldnames:
            raise ValueError("CSV пустой или не удалось прочитать заголовки")

        if "company_inn" not in fieldnames:
            raise ValueError(
                f"В CSV нет колонки company_inn. Найдены колонки: {fieldnames[:20]}"
            )

        director_pairs, founder_pairs = _discover_pairs(fieldnames)

        has_company_phones = "company_phones" in fieldnames

        if not director_pairs and not founder_pairs and not has_company_phones:
            raise ValueError(
                "Не найдены телефонные колонки: company_phones, "
                "director_*_phones или founder_*_phones"
            )

        for row in reader:
            total_rows += 1

            if total_rows % 5000 == 0:
                msg = f"CSV rebuild progress | rows={total_rows} | inns={len(inn_to_items)}"
                logging.info(msg)
                print(msg, flush=True)

            inn = (row.get("company_inn") or "").strip()

            # Не валим сборку из-за плохих ИНН.
            # Просто пропускаем строки, которые бот всё равно не сможет нормально искать.
            if not inn or not inn.isdigit() or len(inn) not in (10, 12):
                bad_inn_rows += 1
                continue

            valid_inn_rows += 1
            inn_counts[inn] = inn_counts.get(inn, 0) + 1

            items = inn_to_items.setdefault(inn, [])
            seen = inn_to_seenphones.setdefault(inn, set())

            # 1. Телефоны компании
            if has_company_phones:
                for ph in split_phones(row.get("company_phones") or ""):
                    key = phone_key(ph)
                    if not key or key in seen:
                        continue

                    seen.add(key)
                    items.append(f"Компания: {ph.strip()}")

            # 2. Телефоны директоров
            for _, fio_col, phones_col in director_pairs:
                fio = (row.get(fio_col) or "").strip() if fio_col else ""
                phones_raw = row.get(phones_col) or ""

                for ph in split_phones(phones_raw):
                    key = phone_key(ph)
                    if not key or key in seen:
                        continue

                    seen.add(key)
                    label = fio if fio else "Директор"
                    items.append(f"{label}: {ph.strip()}")

            # 3. Телефоны учредителей
            for _, fio_col, phones_col in founder_pairs:
                fio = (row.get(fio_col) or "").strip() if fio_col else ""
                phones_raw = row.get(phones_col) or ""

                for ph in split_phones(phones_raw):
                    key = phone_key(ph)
                    if not key or key in seen:
                        continue

                    seen.add(key)
                    label = fio if fio else "Учредитель"
                    items.append(f"{label}: {ph.strip()}")

    duplicates = sorted([inn for inn, count in inn_counts.items() if count > 1])

    if duplicates:
        _write_duplicates_report(inn_counts, duplicates)
        logging.warning(f"Найдены дубли ИНН: {len(duplicates)} шт. (см. duplicates_inn.txt)")
    else:
        with open("duplicates_inn.txt", "w", encoding="utf-8") as rep:
            rep.write("Дубли ИНН не найдены.\n")

    logging.info(
        f"CSV parsed | rows={total_rows} | valid_inn_rows={valid_inn_rows} | "
        f"bad_inn_rows={bad_inn_rows} | unique_inns={len(inn_to_items)}"
    )
    print(
        f"CSV parsed | rows={total_rows} | valid_inn_rows={valid_inn_rows} | "
        f"bad_inn_rows={bad_inn_rows} | unique_inns={len(inn_to_items)}",
        flush=True,
    )

    # Собираем новую базу во временный файл.
    async with aiosqlite.connect(temp_db_path) as db:
        await db.execute(CREATE_COMPANIES_SQL)
        await db.execute(CREATE_META_SQL)

        batch = []
        for inn, items in inn_to_items.items():
            items_json = json.dumps(items, ensure_ascii=False)
            batch.append((inn, items_json))

            if len(batch) >= 1000:
                await db.executemany(
                    "INSERT INTO companies (inn, items_json) VALUES (?, ?)",
                    batch,
                )
                await db.commit()
                batch.clear()

        if batch:
            await db.executemany(
                "INSERT INTO companies (inn, items_json) VALUES (?, ?)",
                batch,
            )
            await db.commit()

        mtime = str(int(os.path.getmtime(CSV_PATH)))

        await db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("csv_mtime", mtime),
        )
        await db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("last_rebuild_ts", str(int(time.time()))),
        )

        await db.commit()

    # Только теперь заменяем старую базу новой.
    os.replace(temp_db_path, DB_PATH)

    elapsed = time.time() - start_ts
    msg = (
        f"CSV rebuild finished | inns={len(inn_to_items)} | "
        f"duplicates={len(duplicates)} | bad_inn_rows={bad_inn_rows} | "
        f"seconds={elapsed:.2f}"
    )

    logging.info(msg)
    print(msg, flush=True)


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
