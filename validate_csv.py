from pathlib import Path
import csv
import io

CSV_PATH = Path("inn.csv")


def main():
    if not CSV_PATH.exists():
        raise SystemExit("ERROR: inn.csv not found")

    raw = CSV_PATH.read_bytes()

    # 1. Разрешаем только UTF-8 / UTF-8 BOM
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        # Даём понятную диагностику
        try:
            raw.decode("cp1251")
            raise SystemExit(
                "ERROR: inn.csv is encoded in cp1251, but the project requires UTF-8. "
                "Please re-save inn.csv as UTF-8 before deploy."
            )
        except UnicodeDecodeError:
            raise SystemExit(
                "ERROR: inn.csv is not valid UTF-8 and not valid cp1251. "
                "The file is corrupted or saved in an unsupported encoding."
            )

    # 2. Проверяем, что CSV вообще читается как таблица
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    fieldnames = reader.fieldnames or []

    if "company_inn" not in fieldnames:
        raise SystemExit(
            f"ERROR: inn.csv does not contain required column 'company_inn'. Found: {fieldnames[:20]}"
        )

    print("OK: inn.csv is valid UTF-8 and contains company_inn")


if __name__ == "__main__":
    main()