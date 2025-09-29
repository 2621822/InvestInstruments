import sqlite3
import csv

# Путь к файлу базы данных и CSV
db_path = 'share.db'
csv_path = 'share.csv'

# Создание подключения к базе данных
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Создание таблицы share (если не существует)
cursor.execute('''
CREATE TABLE IF NOT EXISTS share (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    name TEXT,
    sector TEXT
)
''')

# Загрузка данных из CSV
with open(csv_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    shares = [(row['ticker'], row.get('name', ''), row.get('sector', '')) for row in reader]

# Вставка данных в таблицу
cursor.executemany('INSERT INTO share (ticker, name, sector) VALUES (?, ?, ?)', shares)

# Сохранение изменений и закрытие соединения
conn.commit()
conn.close()