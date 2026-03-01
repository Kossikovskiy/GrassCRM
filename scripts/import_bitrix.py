"""
Импорт сделок из выгрузки Битрикс24
Запуск: python scripts/import_bitrix.py путь/к/файлу.xls

Файл .xls от Битрикс — это на самом деле HTML-таблица.
Скрипт парсит её, группирует строки по ID сделки,
суммирует товары и создаёт сделки в нашей БД.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from html.parser import HTMLParser
from datetime import datetime
from models.database import (
    get_engine, get_session_factory, init_db,
    Deal, DealService, Stage, Service
)


# ── Парсер HTML-таблицы ──────────────────────────────────────

class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.current_row = []
        self.current_cell = ''
        self.in_cell = False

    def handle_starttag(self, tag, attrs):
        if tag in ('td', 'th'):
            self.in_cell = True
            self.current_cell = ''
        elif tag == 'tr':
            self.current_row = []

    def handle_endtag(self, tag):
        if tag in ('td', 'th'):
            self.current_row.append(self.current_cell.strip())
            self.in_cell = False
        elif tag == 'tr':
            if self.current_row:
                self.rows.append(self.current_row)

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data


# ── Маппинг стадий Битрикс → наши этапы ────────────────────

STAGE_MAP = {
    'Сделка успешна':  'Успешно',
    'Сделка провалена':'Провалена',
    'Новая':           'Начальная',
    'В работе':        'В работе',
    'Подготовка':      'Запланировано',
    'Согласование':    'Согласовать',
    'Ожидание':        'Ожидание',
}


def parse_date(s: str):
    """Парсим дату из формата Битрикс: 30.10.2025 17:19:43"""
    if not s:
        return None
    for fmt in ('%d.%m.%Y %H:%M:%S', '%d.%m.%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


def parse_amount(s: str) -> float:
    """Парсим сумму: '7 000,50' → 7000.5"""
    if not s:
        return 0.0
    s = s.replace(' ', '').replace('\xa0', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def import_bitrix(filepath: str, skip_existing: bool = True):
    print(f"\n🌿 Импорт из Битрикс: {filepath}")

    # Читаем файл
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        content = f.read()

    parser = TableParser()
    parser.feed(content)

    if not parser.rows:
        print("❌ Файл пустой или не распознан")
        return

    headers = parser.rows[0]
    rows    = parser.rows[1:]

    # Индексы нужных колонок
    def col(name):
        try:
            return headers.index(name)
        except ValueError:
            return None

    IDX = {
        'id':        col('ID'),
        'title':     col('Название сделки'),
        'stage':     col('Стадия сделки'),
        'sum':       col('Сумма'),
        'date':      col('Дата создания'),
        'start_date':col('Дата начала'),
        'manager':   col('Ответственный'),
        'notes':     col('Комментарий'),
        'first':     col('Контакт: Имя'),
        'last':      col('Контакт: Фамилия'),
        'phone':     col('Контакт: Мобильный телефон'),
        'product':   col('Товар'),
        'price':     col('Цена'),
        'qty':       col('Количество'),
        'closed':    col('Сделка закрыта'),
    }

    # Группируем строки по ID сделки
    deals_raw: dict[str, dict] = {}
    for row in rows:
        def get(key):
            idx = IDX.get(key)
            return row[idx].strip() if idx is not None and idx < len(row) else ''

        bid = get('id')
        if not bid:
            continue

        if bid not in deals_raw:
            # Собираем имя клиента
            first = get('first')
            last  = get('last')
            phone = get('phone')
            if first or last:
                client = f"{first} {last}".strip()
            else:
                client = get('title')  # если контакта нет — берём название

            deals_raw[bid] = {
                'bitrix_id': bid,
                'title':     get('title') or f'Сделка #{bid}',
                'client':    client,
                'phone':     phone,
                'stage':     get('stage'),
                'date':      get('date'),
                'start_date':get('start_date'),
                'manager':   get('manager'),
                'notes':     get('notes'),
                'closed':    get('closed'),
                'products':  [],
                'total':     0.0,
            }

        # Добавляем товары этой сделки
        product = get('product')
        price   = parse_amount(get('price'))
        qty_str = get('qty')
        qty     = float(qty_str) if qty_str else 1.0
        amount  = parse_amount(get('sum'))

        if product and price:
            deals_raw[bid]['products'].append({
                'name': product,
                'price': price,
                'qty': qty,
                'subtotal': price * qty
            })
        elif amount:
            # Если нет детализации товара — берём общую сумму
            deals_raw[bid]['total'] = max(deals_raw[bid]['total'], amount)

    print(f"📦 Найдено уникальных сделок: {len(deals_raw)}")

    # Подключаемся к БД
    engine = get_engine()
    init_db(engine)
    Session = get_session_factory(engine)

    with Session() as session:
        # Загружаем этапы
        stage_objs = {s.name: s for s in session.query(Stage).all()}

        imported = 0
        skipped  = 0

        for bid, d in deals_raw.items():
            # Пропускаем дубли
            if skip_existing:
                exists = session.query(Deal).filter(
                    Deal.title == d['title'],
                    Deal.client == d['client']
                ).first()
                if exists:
                    skipped += 1
                    continue

            # Маппим этап
            our_stage_name = STAGE_MAP.get(d['stage'], 'Начальная')
            stage = stage_objs.get(our_stage_name) or stage_objs.get('Начальная')

            # Дата
            # В выгрузке Битрикса "Дата создания" может отражать массовую миграцию (например 2025),
            # а реальная рабочая дата сделки хранится в поле "Дата начала".
            created_at = parse_date(d.get('start_date')) or parse_date(d.get('date')) or datetime.utcnow()
            closed_at  = parse_date(d['closed']) if d['closed'] else None
            if not closed_at and stage and stage.is_final:
                closed_at = created_at

            # Считаем итоговую сумму
            if d['products']:
                total = sum(p['subtotal'] for p in d['products'])
            else:
                total = d['total']

            # Примечание: добавляем телефон если есть
            notes = d['notes']
            if d['phone']:
                notes = f"Тел: {d['phone']}" + (f"\n{notes}" if notes else '')

            # Создаём сделку
            deal = Deal(
                title=d['title'],
                client=d['client'],
                manager=d['manager'],
                notes=notes,
                stage_id=stage.id if stage else None,
                created_at=created_at,
                updated_at=created_at,
                closed_at=closed_at,
            )
            session.add(deal)
            session.flush()

            # Добавляем товары как DealService (без привязки к прайсу)
            for p in d['products']:
                # Ищем услугу по названию в нашем прайсе
                svc = session.query(Service).filter(
                    Service.name.ilike(f"%{p['name'][:20]}%")
                ).first()

                ds = DealService(
                    deal_id=deal.id,
                    service_id=svc.id if svc else None,
                    quantity=p['qty'],
                    price_at_moment=p['price'],
                    notes=p['name']  # сохраняем оригинальное название товара
                )
                session.add(ds)

            # Если товаров не было — создаём одну строку с общей суммой
            if not d['products'] and total > 0:
                ds = DealService(
                    deal_id=deal.id,
                    service_id=None,
                    quantity=1,
                    price_at_moment=total,
                    notes=d['title']
                )
                session.add(ds)

            imported += 1

        session.commit()

    print(f"\n✅ Импортировано:  {imported} сделок")
    print(f"⏭️  Пропущено (дубли): {skipped} сделок")
    print(f"\n🎉 Готово! Откройте CRM и обновите страницу.")


if __name__ == '__main__':
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    force = '--force' in sys.argv  # --force = добавить всё, даже дубли

    if not args:
        default = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'DEAL_20260225_80749126_699ec1e196167.xls'
        )
        filepath = default
    else:
        filepath = args[0]

    if not os.path.exists(filepath):
        print(f"❌ Файл не найден: {filepath}")
        print("Использование: python scripts/import_bitrix.py файл.xls [--force]")
        sys.exit(1)

    import_bitrix(filepath, skip_existing=not force)
