"""
Шаблон для импорта исторических данных из Excel/CSV
Адаптируйте под структуру ваших файлов.

Установите: pip install pandas openpyxl
Запуск: python scripts/import_history.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime
from models.database import (
    get_engine, get_session_factory,
    Deal, DealService, Stage, Service, ServiceCategory,
    Expense, ExpenseCategory, Equipment
)

engine = get_engine()
Session = get_session_factory(engine)


def import_deals_from_list(deals_data: list[dict]):
    """
    Импорт сделок из списка словарей.
    
    Формат deals_data:
    [
        {
            "title": "Покос Иванова",
            "client": "Иванов А.П.",
            "stage": "Успешно",           # название этапа
            "manager": "",
            "created_at": "15.05.2024",   # DD.MM.YYYY
            "services": [
                {"name": "Покос травы (до 20 см)", "quantity": 10, "price": 350}
            ]
        },
        ...
    ]
    """
    with Session() as session:
        imported = 0
        for d in deals_data:
            # Найти этап
            stage = None
            if d.get("stage"):
                stage = session.query(Stage).filter_by(name=d["stage"]).first()
            if not stage:
                stage = session.query(Stage).filter_by(name="Согласовать").first()

            # Парсить дату
            created_at = datetime.utcnow()
            if d.get("created_at"):
                for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
                    try:
                        created_at = datetime.strptime(d["created_at"], fmt)
                        break
                    except ValueError:
                        continue

            deal = Deal(
                title=d.get("title", "Сделка"),
                client=d.get("client", "Клиент"),
                manager=d.get("manager", ""),
                address=d.get("address", ""),
                notes=d.get("notes", ""),
                stage_id=stage.id if stage else None,
                created_at=created_at,
                updated_at=created_at,
            )
            if stage and stage.type == "success":
                deal.closed_at = created_at

            session.add(deal)
            session.flush()

            # Добавить услуги
            for svc_data in d.get("services", []):
                # Искать услугу по названию
                service = None
                if svc_data.get("name"):
                    service = session.query(Service).filter(
                        Service.name.ilike(f"%{svc_data['name']}%")
                    ).first()
                
                price = svc_data.get("price", service.price if service else 0)
                qty = svc_data.get("quantity", 1)
                
                ds = DealService(
                    deal_id=deal.id,
                    service_id=service.id if service else None,
                    quantity=qty,
                    price_at_moment=price,
                    notes=svc_data.get("notes", "")
                )
                session.add(ds)

            imported += 1
        
        session.commit()
        print(f"✅ Импортировано {imported} сделок")


def import_expenses_from_list(expenses_data: list[dict]):
    """
    Импорт расходов из списка словарей.
    
    Формат:
    [
        {
            "date": "15.05.2024",
            "name": "Бензин АИ-92",
            "category": "Топливо",
            "amount": 5000,
            "equipment": "Газонокосилка самоходная"  # опционально
        },
        ...
    ]
    """
    with Session() as session:
        imported = 0
        for e in expenses_data:
            # Парсить дату
            parsed_date = None
            for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
                try:
                    parsed_date = datetime.strptime(e["date"], fmt).date()
                    break
                except (ValueError, KeyError):
                    continue
            if not parsed_date:
                print(f"⚠️ Пропущена запись (неверная дата): {e}")
                continue

            # Найти категорию
            cat = session.query(ExpenseCategory).filter(
                ExpenseCategory.name.ilike(f"%{e.get('category', '')}%")
            ).first()

            # Найти технику (если указана)
            equipment = None
            if e.get("equipment"):
                equipment = session.query(Equipment).filter(
                    Equipment.name.ilike(f"%{e['equipment']}%")
                ).first()

            expense = Expense(
                date=parsed_date,
                name=e.get("name", "Расход"),
                category_id=cat.id if cat else None,
                amount=float(e.get("amount", 0)),
                year=parsed_date.year,
                equipment_id=equipment.id if equipment else None,
                notes=e.get("notes", "")
            )
            session.add(expense)
            imported += 1

        session.commit()
        print(f"✅ Импортировано {imported} расходов")


# ─────────────────────────────────────────────
# Пример данных для тестирования
# ─────────────────────────────────────────────

SAMPLE_DEALS_2024 = [
    {
        "title": "Покос участка Антон Пески",
        "client": "Антон Пески",
        "stage": "Успешно",
        "created_at": "15.05.2024",
        "services": [
            {"name": "Покос травы (до 20 см)", "quantity": 8, "price": 350},
            {"name": "Сгребание и сборка травы", "quantity": 8, "price": 200},
        ]
    },
    {
        "title": "Вертикуляция газона Петров",
        "client": "Петров В.В.",
        "stage": "Успешно",
        "created_at": "20.06.2024",
        "services": [
            {"name": "Вертикуляция (скарификация)", "quantity": 5, "price": 600},
        ]
    },
    {
        "title": "Спил деревьев Сидорова",
        "client": "Сидорова Н.К.",
        "stage": "Успешно",
        "created_at": "10.07.2024",
        "services": [
            {"name": "Спил деревьев (до 5м)", "quantity": 2, "price": 3000},
            {"name": "Корчевание пней", "quantity": 2, "price": 2000},
        ]
    },
]

SAMPLE_EXPENSES_2024 = [
    {"date": "01.04.2024", "name": "Бензин АИ-92", "category": "Топливо", "amount": 3500},
    {"date": "15.04.2024", "name": "Масло моторное Husqvarna", "category": "Тех. жидкости/запчасти", "amount": 1200},
    {"date": "01.05.2024", "name": "Бензин АИ-92", "category": "Топливо", "amount": 4200},
    {"date": "10.05.2024", "name": "Леска триммерная 3мм", "category": "Расходники", "amount": 800},
    {"date": "20.05.2024", "name": "Реклама Авито", "category": "Реклама", "amount": 2000},
    {"date": "01.06.2024", "name": "Бензин АИ-92", "category": "Топливо", "amount": 5500},
    {"date": "15.06.2024", "name": "Фильтр воздушный Husqvarna 128R", "category": "Тех. жидкости/запчасти", "amount": 450, "equipment": "Триммер бензиновый"},
]


if __name__ == "__main__":
    print("🌿 Импорт тестовых данных...")
    import_deals_from_list(SAMPLE_DEALS_2024)
    import_expenses_from_list(SAMPLE_EXPENSES_2024)
    print("\n✅ Готово! Теперь можно спрашивать Claude о данных.")
    print("\nПример: 'Покажи все сделки за 2024 год и прибыль'")
