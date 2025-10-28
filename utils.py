import os
import pdfplumber
from datetime import datetime
import sqlite3

def parse_amount(amount_str: str) -> float:
    """Парсит сумму вида '- 2 170,00 ₸' → -2170.00"""
    try:
        clean = amount_str.replace("₸", "").replace(" ", "").replace(",", ".")
        return float(clean)
    except Exception as e:
        raise ValueError(f"Ошибка преобразования суммы: {amount_str} ({e})")

def add_transaction_to_monthly_db(date_str: str, amount: float, operation_type: str, details: str):
    """Добавляет одну транзакцию в соответствующую БД"""
    dt = datetime.strptime(date_str, "%d.%m.%y")
    year, month = dt.year, dt.month
    db_name = f"richpapa_{year}_{month:02d}.db"

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS transactions_{year}_{month:02d} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        amount REAL NOT NULL,
        operation_type TEXT NOT NULL,
        details TEXT DEFAULT NULL
    )
    """)

    cursor.execute(f"""
    INSERT INTO transactions_{year}_{month:02d} (date, amount, operation_type, details)
    VALUES (?, ?, ?, ?)
    """, (date_str, amount, operation_type, details))

    conn.commit()
    conn.close()

def import_kaspi_pdf(pdf_path: str):
    """Импортирует PDF-выписку и добавляет все транзакции в месячные БД"""
    if not os.path.exists(pdf_path):
        print(f"Файл {pdf_path} не найден!")
        return {"imported": 0, "error": "Файл не найден"}

    imported_count = 0
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for i, row in enumerate(table):
                if i == 0 and row[0].startswith("Дата"):
                    continue

                date = row[0]
                raw_amount = row[1]
                operation_type = row[2]
                details = row[3]

                try:
                    amount = parse_amount(raw_amount)
                    add_transaction_to_monthly_db(date, amount, operation_type, details)
                    imported_count += 1
                except Exception as e:
                    print(f"Ошибка при обработке строки {row}: {e}")

    return {"imported": imported_count}
