# utils.py
import pdfplumber
import re
from datetime import datetime
from db_manager import insert_transaction



def parse_amount(amount_str: str) -> float:
    match = re.search(r"([-+]?\s?[\d\s,\.]+)\s?₸", amount_str)
    if not match:
        raise ValueError(f"Не удалось найти сумму в '{amount_str}'")

    clean = match.group(1).replace(" ", "").replace(",", ".").strip()
    if "-" in clean:
        clean = "-" + clean.replace("-", "")
    return float(clean)


def import_kaspi_pdf(pdf_path: str):
    imported_count = 0
    errors = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for i, row in enumerate(table):
                if i == 0 and "Дата" in row[0]:
                    continue  # пропускаем заголовок

                try:
                    date_str = row[0].strip()
                    raw_amount = row[1]
                    operation_type = row[2]
                    details = row[3]

                    amount = parse_amount(raw_amount)
                    insert_transaction(date_str, amount, operation_type, details)
                    imported_count += 1

                except Exception as e:
                    errors.append(str(e))

    return {"imported": imported_count, "errors": errors}
