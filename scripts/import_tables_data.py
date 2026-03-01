"""
Импорт техники и расходов из таблиц (seed_data).
Запуск: python scripts/import_tables_data.py
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models.database import get_engine, get_session_factory, init_db, Equipment, Expense, ExpenseCategory
from data.seed_data import EQUIPMENT, EXPENSES


def main():
    engine = get_engine()
    init_db(engine)
    Session = get_session_factory(engine)

    with Session() as session:
        session.query(Expense).delete()
        session.query(Equipment).delete()
        session.commit()

        for eq in EQUIPMENT:
            payload = dict(eq)
            if payload.get("purchase_date"):
                payload["purchase_date"] = datetime.strptime(payload["purchase_date"], "%Y-%m-%d").date()
            session.add(Equipment(**payload))

        session.commit()

        cat_map = {c.name.lower().replace(' ', ''): c for c in session.query(ExpenseCategory).all()}
        for row in EXPENSES:
            key = row["category"].lower().replace(' ', '')
            cat = cat_map.get(key)
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

        session.commit()
        print(f"✅ Импортировано техники: {session.query(Equipment).count()}")
        print(f"✅ Импортировано расходов: {session.query(Expense).count()}")


if __name__ == '__main__':
    main()
