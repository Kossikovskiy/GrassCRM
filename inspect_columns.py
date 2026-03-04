
import pandas as pd

DEALS_FILE_PATH = 'docs/DEAL_20260225_80749126_699ec1e196167.xls'

try:
    # Используем read_html, который возвращает список всех таблиц в файле.
    # Нам нужна первая (и, скорее всего, единственная) таблица.
    tables = pd.read_html(DEALS_FILE_PATH, header=0) # header=0 говорит, что первая строка - это заголовки
    df = tables[0]
    
    print("Названия колонок в файле (прочитано из HTML):")
    print(list(df.columns))
    
except FileNotFoundError:
    print(f"ОШИБКА: Файл {DEALS_FILE_PATH} не найден.")
except Exception as e:
    print(f"Произошла ошибка: {e}")
