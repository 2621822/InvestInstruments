# --- Импорт необходимых библиотек ---
import requests
import json
import pandas as pd
import datetime
import sqlite3
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo

# --- Константы ---
DB_PATH = 'moex_data.db'
START_DATE = datetime.date(2022, 1, 1)
# --- Получение последних дат по каждому инструменту из таблицы moex ---
def get_last_dates_from_db(conn, instruments):
    """
    Возвращает словарь {secid: last_date} для каждого инструмента из таблицы moex.
    Если данных нет, возвращает None.
    """
    last_dates = {}
    for secid in instruments:
        # Получаем BOARDID для данного инструмента
        query_boardid = "SELECT DISTINCT BOARDID FROM moex WHERE SECID = ?"
        boardids = [row[0] for row in conn.execute(query_boardid, (secid,)).fetchall()]
        for boardid in boardids:
            query = "SELECT MAX(TRADEDATE) FROM moex WHERE SECID = ? AND BOARDID = ?"
            result = conn.execute(query, (secid, boardid)).fetchone()
            key = (boardid, secid)
            last_dates[key] = result[0] if result and result[0] else None
    return last_dates

# --- Импорт необходимых библиотек ---
import requests
import json
import pandas as pd
import datetime
import sqlite3
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo

def get_date_ranges(start, end, step=100):
    """
    Возвращает список кортежей (начало, конец) для диапазонов дат с заданным шагом.
    """
    ranges = []
    current = start
    while current <= end:
        till = min(current + datetime.timedelta(days=step-1), end)
        ranges.append((current, till))
        current = till + datetime.timedelta(days=1)
    return ranges

def load_instruments(conn, needed):
    """
    Загружает справочник инструментов из таблицы share и фильтрует по списку needed.
    """
    share_df = pd.read_sql('SELECT * FROM share', conn)
    return share_df[share_df['share'].isin(needed)]['share'].dropna().unique()

def fetch_moex_data(instruments, date_ranges):
    """
    Загружает исторические данные по каждому инструменту за указанные диапазоны дат.
    Возвращает список строк и список столбцов.
    """
    all_rows = []
    columns = None
    for secid in instruments:
        print(f"Обрабатывается инструмент: {secid}")
        has_data = False
        instrument_rows = 0
        for dr_start, dr_end in date_ranges:
            url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/{secid}.json?from={dr_start}&till={dr_end}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if columns is None:
                    columns = data["history"]["columns"]
                rows = data["history"]["data"]
                row_count = len(rows) if rows else 0
                print(f"  Диапазон дат: {dr_start} — {dr_end}: получено строк: {row_count}")
                if rows:
                    has_data = True
                    all_rows.extend(rows)
                    instrument_rows += row_count
            else:
                print(f"  Диапазон дат: {dr_start} — {dr_end}: ошибка {response.status_code} для {secid}: {response.text}")
        print(f"  Получено строк: {instrument_rows}")
        if not has_data:
            print(f"Нет данных по инструменту: {secid}")
    return all_rows, columns

def save_json(data, path):
    """
    Сохраняет данные в JSON-файл.
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Данные успешно записаны в файл '{path}'")

def format_dates(df, columns):
    """
    Форматирует столбцы дат в российский формат (дд.мм.гггг).
    """
    for date_col in columns:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%d.%m.%Y')
    return df

def save_excel(df, path):
    """
    Сохраняет DataFrame в Excel и преобразует лист в умную таблицу.
    """
    df.to_excel(path, index=False)
    wb = load_workbook(path)
    ws = wb.active
    tab = Table(displayName="MOEXTable", ref=ws.dimensions)
    style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False,
                           showLastColumn=False, showRowStripes=True, showColumnStripes=True)
    tab.tableStyleInfo = style
    ws.add_table(tab)
    wb.save(path)
    print(f"Данные сохранены в умной таблице Excel: '{path}'")

def save_sqlite(df, conn, table_name):
    """
    Сохраняет DataFrame в таблицу SQLite.
    """
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Данные сохранены в базе данных moex_data.db, таблица '{table_name}'")

# --- Основной алгоритм ---
def main():
    end_date = datetime.date.today()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            share_df = pd.read_sql('SELECT * FROM share', conn)
            instruments = share_df['share'].dropna().unique()
            boardid_map = {}
            last_dates = get_last_dates_from_db(conn, instruments)
            summary = []
            all_data = []
            columns = None
            for secid in instruments:
                query_boardid = "SELECT DISTINCT BOARDID FROM moex WHERE SECID = ?"
                boardids = [row[0] for row in conn.execute(query_boardid, (secid,)).fetchall()]
                boardids = boardids if boardids else ['TQBR']
                total_rows = 0
                for boardid in boardids:
                    last_date = last_dates.get((boardid, secid))
                    if last_date:
                        try:
                            start_date = datetime.datetime.strptime(last_date, '%Y-%m-%d').date() + datetime.timedelta(days=1)
                        except ValueError:
                            start_date = datetime.datetime.strptime(last_date, '%d.%m.%Y').date() + datetime.timedelta(days=1)
                    else:
                        start_date = START_DATE
                    date_ranges = get_date_ranges(start_date, end_date)
                    sec_rows, sec_columns = fetch_moex_data([secid], date_ranges)
                    sec_rows = [row for row in sec_rows if (len(row) > 0 and (row[0] == boardid))]
                    if sec_columns and columns is None:
                        columns = sec_columns
                    if sec_rows:
                        df = pd.DataFrame(sec_rows, columns=columns)
                        # Удаляем дубли по ключу
                        if set(['BOARDID', 'TRADEDATE', 'SECID']).issubset(df.columns):
                            df = df.drop_duplicates(subset=['BOARDID', 'TRADEDATE', 'SECID'])
                        # Сохраняем в базу
                        df.to_sql('moex', conn, if_exists='append', index=False)
                        # Форматируем даты
                        df = format_dates(df, ['TRADEDATE', 'TRADE_SESSION_DATE'])
                        # Добавляем к общим данным
                        all_data.extend(df.to_dict(orient='records'))
                        added_rows = len(df)
                        total_rows += added_rows
                summary.append({'SECID': secid, 'rows': total_rows})
            # Сохраняем общий DataFrame и JSON только один раз
            if all_data and columns:
                all_data_df = pd.DataFrame(all_data, columns=columns)
                save_excel(all_data_df, 'moex_data.xlsx')
                save_json(all_data, 'moex_data.json')
            print("\nИтоговая сводка:")
            for item in summary:
                print(f"Инструмент: {item['SECID']}, добавлено строк: {item['rows']}")
            print(f"Всего обработано инструментов: {len(summary)}")
            print(f"Общее количество добавленных строк: {sum(item['rows'] for item in summary)}")
    except Exception as e:
        print(f"Ошибка выполнения: {e}")

if __name__ == "__main__":
    main()

