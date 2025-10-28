import sqlite3
import json
from datetime import datetime
import os


def get_db_name(date_str: str) -> str:
    """Определяем БД по дате транзакции"""
    try:
        # поддержка 24.09.25 / 24.09.2025
        try:
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError:
            date_obj = datetime.strptime(date_str, "%d.%m.%y")
        year, month = date_obj.year, date_obj.month
        return f"richpapa_{year}_{month:02d}.db", year, month
    except Exception as e:
        raise ValueError(f"Неверный формат даты: {date_str} ({e})")


def ensure_monthly_db(year: int, month: int):
    """Создаёт таблицы, если нет"""
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
    CREATE TABLE IF NOT EXISTS selected_transactions_{year}_{month:02d} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        total_amount REAL NOT NULL,
        category TEXT DEFAULT NULL,
        extra_details TEXT DEFAULT NULL,
        raw_transaction_ids TEXT DEFAULT NULL
    )
    """)

    conn.commit()
    conn.close()


def add_selected_transaction(selected_data: dict):
    """
    selected_data = {
        "date": "25.10.25",
        "total_amount": 12000,
        "category": "Еда",
        "extra_details": "Совместный обед",
        "raw_transaction_ids": [1, 3, 5]
    }
    """
    date_str = selected_data["date"]
    db_name, year, month = get_db_name(date_str)
    ensure_monthly_db(year, month)

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    raw_json = json.dumps(selected_data.get("raw_transaction_ids", []))

    cursor.execute(f"""
        INSERT INTO selected_transactions_{year}_{month:02d}
        (date, total_amount, category, extra_details, raw_transaction_ids)
        VALUES (?, ?, ?, ?, ?)
    """, (
        date_str,
        selected_data["total_amount"],
        selected_data.get("category"),
        selected_data.get("extra_details"),
        raw_json
    ))

    conn.commit()
    conn.close()
    return {"message": "✅ Добавлено", "db": db_name}


def fetch_selected_transactions(year: int, month: int):
    """Возвращает все selected_transactions как JSON"""
    db_name = f"richpapa_{year}_{month:02d}.db"
    if not os.path.exists(db_name):
        return []

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM selected_transactions_{year}_{month:02d}")
    rows = cursor.fetchall()

    cols = [desc[0] for desc in cursor.description]
    conn.close()

    return [dict(zip(cols, row)) for row in rows]
