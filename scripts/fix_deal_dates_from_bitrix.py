"""Исправляет created_at у сделок по колонке "Дата начала" из выгрузки Битрикс.

Запуск:
  python scripts/fix_deal_dates_from_bitrix.py DEAL_....xls
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scripts.import_bitrix import TableParser, parse_date
from models.database import get_engine, get_session_factory, Deal


def main(filepath: str):
    parser = TableParser()
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        parser.feed(f.read())

    if not parser.rows:
        print('❌ Пустой файл')
        return

    headers = parser.rows[0]
    rows = parser.rows[1:]

    def col(name):
        try:
            return headers.index(name)
        except ValueError:
            return None

    idx_title = col('Название сделки')
    idx_client_first = col('Контакт: Имя')
    idx_client_last = col('Контакт: Фамилия')
    idx_start = col('Дата начала')

    if idx_title is None or idx_start is None:
        print('❌ В файле нет обязательных колонок')
        return

    mapping = {}
    for row in rows:
        title = row[idx_title].strip() if idx_title < len(row) else ''
        first = row[idx_client_first].strip() if idx_client_first is not None and idx_client_first < len(row) else ''
        last = row[idx_client_last].strip() if idx_client_last is not None and idx_client_last < len(row) else ''
        client = f"{first} {last}".strip() if (first or last) else title
        start_raw = row[idx_start].strip() if idx_start < len(row) else ''
        dt = parse_date(start_raw)
        if title and client and dt:
            mapping[(title, client)] = dt

    engine = get_engine()
    Session = get_session_factory(engine)

    updated = 0
    with Session() as session:
        deals = session.query(Deal).all()
        for d in deals:
            dt = mapping.get((d.title, d.client))
            if not dt:
                continue
            if d.created_at.date() != dt.date():
                d.created_at = dt
                d.updated_at = datetime.utcnow()
                if d.closed_at and d.closed_at < dt:
                    d.closed_at = dt
                updated += 1
        session.commit()

    print(f'✅ Обновлено дат сделок: {updated}')


if __name__ == '__main__':
    filepath = sys.argv[1] if len(sys.argv) > 1 else 'DEAL_20260225_80749126_699ec1e196167.xls'
    if not os.path.exists(filepath):
        print(f'❌ Файл не найден: {filepath}')
        sys.exit(1)
    main(filepath)
