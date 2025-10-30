import sqlite3
import os
from datetime import datetime

DB_PATH = "richpapa.db"

def create_database():
    """Создает файл richpapa.db, если он не существует"""
    if not os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        conn.close()


def _connect():

    return sqlite3.connect(DB_PATH)


def create_monthly_tables(year: int, month: int):

    conn = _connect()
    cursor = conn.cursor()

    transactions_table = f"transactions_{year}_{month:02d}"
    selected_table = f"selected_transactions_{year}_{month:02d}"

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {transactions_table} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        amount REAL NOT NULL,
        operation_type TEXT,
        details TEXT
    )
    """)

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {selected_table} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        total_amount REAL NOT NULL,
        category TEXT,
        extra_details TEXT,
        raw_transaction_ids TEXT
    )
    """)

    conn.commit()
    conn.close()
    print(f"[DB] Таблицы {transactions_table} и {selected_table} готовы.")


def insert_transaction(date: str, amount: float, operation_type: str, details: str):
    dt = datetime.strptime(date, "%d.%m.%y")
    year, month = dt.year, dt.month

    create_monthly_tables(year, month)
    conn = _connect()
    cursor = conn.cursor()

    table_name = f"transactions_{year}_{month:02d}"
    cursor.execute(f"""
        INSERT INTO {table_name} (date, amount, operation_type, details)
        VALUES (?, ?, ?, ?)
    """, (date, amount, operation_type, details))

    conn.commit()
    conn.close()


def fetch_transactions(year: int, month: int):
    table_name = f"transactions_{year}_{month:02d}"
    conn = _connect()
    cursor = conn.cursor()

    # Проверяем, существует ли таблица
    cursor.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND name=?
    """, (table_name,))
    if not cursor.fetchone():
        conn.close()
        return []

    # Извлекаем все записи
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    conn.close()

    # Преобразуем в JSON-список
    return [
        {
            "id": row[0],
            "date": row[1],
            "amount": row[2],
            "operation_type": row[3],
            "details": row[4],
        }
        for row in rows
    ]