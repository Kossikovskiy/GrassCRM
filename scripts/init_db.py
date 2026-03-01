"""
Инициализация БД и заполнение начальными данными
Запуск: python scripts/init_db.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models.database import (
    init_db, get_engine, get_session_factory,
    Stage, ServiceCategory, Service, ExpenseCategory, Equipment, Expense
)
from datetime import datetime
from data.seed_data import STAGES, EXPENSE_CATEGORIES, SERVICE_CATEGORIES, SERVICES, EQUIPMENT, EXPENSES


def seed_database(session):
    # Этапы
    if session.query(Stage).count() == 0:
        for s in STAGES:
            session.add(Stage(**s))
        print(f"✅ Добавлено {len(STAGES)} этапов")

    # Категории расходов
    if session.query(ExpenseCategory).count() == 0:
        for name in EXPENSE_CATEGORIES:
            session.add(ExpenseCategory(name=name))
        print(f"✅ Добавлено {len(EXPENSE_CATEGORIES)} категорий расходов")

    # Категории услуг
    if session.query(ServiceCategory).count() == 0:
        for cat in SERVICE_CATEGORIES:
            session.add(ServiceCategory(**cat))
        print(f"✅ Добавлено {len(SERVICE_CATEGORIES)} категорий услуг")

    session.commit()

    # Услуги
    if session.query(Service).count() == 0:
        for (name, cat_name, unit, price, min_vol) in SERVICES:
            cat = session.query(ServiceCategory).filter_by(name=cat_name).first()
            if cat:
                session.add(Service(
                    name=name,
                    category_id=cat.id,
                    unit=unit,
                    price=price,
                    min_volume=min_vol
                ))
        print(f"✅ Добавлено {len(SERVICES)} услуг")

    # Техника
    if session.query(Equipment).count() == 0:
        for eq in EQUIPMENT:
            payload = dict(eq)
            if payload.get("purchase_date"):
                payload["purchase_date"] = datetime.strptime(payload["purchase_date"], "%Y-%m-%d").date()
            session.add(Equipment(**payload))
        print(f"✅ Добавлено {len(EQUIPMENT)} единиц техники")

    # Расходы
    if session.query(Expense).count() == 0:
        cats = {c.name.lower().replace(' ', ''): c for c in session.query(ExpenseCategory).all()}
        for row in EXPENSES:
            key = row["category"].lower().replace(' ', '')
            cat = cats.get(key)
            if not cat:
                cat = session.query(ExpenseCategory).filter(ExpenseCategory.name.ilike(f"%{row['category']}%")).first()
            dt = datetime.strptime(row["date"], "%Y-%m-%d").date()
            session.add(Expense(
                date=dt,
                name=row["name"],
                category_id=cat.id if cat else None,
                amount=float(row["amount"]),
                year=dt.year,
            ))
        print(f"✅ Добавлено {len(EXPENSES)} расходов")

    session.commit()
    print("\n🎉 База данных успешно инициализирована!")


if __name__ == "__main__":
    engine = get_engine()
    init_db(engine)
    Session = get_session_factory(engine)
    with Session() as session:
        seed_database(session)
