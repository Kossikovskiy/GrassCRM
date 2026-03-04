"""
MCP-сервер для CRM бизнеса по покосу травы
Запуск: python mcp_server/server.py
"""

import json
import sys
import os
from datetime import datetime, date, timedelta
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server
from sqlalchemy import extract, func, and_

from models.database import (
    get_engine, get_session_factory, init_db,
    Stage, Service, ServiceCategory, Deal, DealService,
    Equipment, Maintenance, ExpenseCategory, Expense
)
from data.seed_data import STAGES, EXPENSE_CATEGORIES, SERVICE_CATEGORIES, SERVICES, EQUIPMENT

# Инициализация
engine = get_engine()
init_db(engine)
Session = get_session_factory(engine)

# Автозаполнение при первом запуске
def _auto_seed():
    from scripts.init_db import seed_database
    with Session() as s:
        if s.query(Stage).count() == 0:
            seed_database(s)

try:
    _auto_seed()
except Exception:
    pass

app = Server("grass-crm")


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _deal_total(deal, session) -> float:
    total = 0.0
    for ds in deal.deal_services:
        total += ds.quantity * ds.price_at_moment
    return total


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _ok(data: Any) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps(data, ensure_ascii=False, default=str))]


def _err(msg: str) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps({"error": msg}, ensure_ascii=False))]


# ─────────────────────────────────────────────
#  TOOL DEFINITIONS
# ─────────────────────────────────────────────

@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        # ── DEALS ──────────────────────────────
        types.Tool(
            name="create_deal",
            description="Создать новую сделку с набором услуг",
            inputSchema={
                "type": "object",
                "properties": {
                    "title":    {"type": "string", "description": "Название сделки"},
                    "client":   {"type": "string", "description": "Имя клиента"},
                    "manager":  {"type": "string", "description": "Менеджер/исполнитель", "default": ""},
                    "address":  {"type": "string", "description": "Адрес объекта", "default": ""},
                    "notes":    {"type": "string", "description": "Примечания", "default": ""},
                    "services": {
                        "type": "array",
                        "description": "Список услуг [{service_id, quantity}]",
                        "items": {
                            "type": "object",
                            "properties": {
                                "service_id": {"type": "integer"},
                                "quantity":   {"type": "number"}
                            },
                            "required": ["service_id", "quantity"]
                        }
                    }
                },
                "required": ["title", "client"]
            }
        ),
        types.Tool(
            name="get_deals",
            description="Получить список сделок с фильтрами",
            inputSchema={
                "type": "object",
                "properties": {
                    "status":    {"type": "string", "description": "Фильтр по этапу (название этапа)"},
                    "client":    {"type": "string", "description": "Фильтр по имени клиента"},
                    "date_from": {"type": "string", "description": "Начало периода (YYYY-MM-DD)"},
                    "date_to":   {"type": "string", "description": "Конец периода (YYYY-MM-DD)"},
                    "year":      {"type": "integer", "description": "Год создания сделок"},
                    "month":     {"type": "integer", "description": "Месяц (1-12)"},
                }
            }
        ),
        types.Tool(
            name="update_deal_stage",
            description="Переместить сделку на другой этап канбана",
            inputSchema={
                "type": "object",
                "properties": {
                    "deal_id":    {"type": "integer"},
                    "stage_name": {"type": "string", "description": "Название нового этапа"}
                },
                "required": ["deal_id", "stage_name"]
            }
        ),
        types.Tool(
            name="get_deal_stages",
            description="Получить все этапы канбана",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="get_deal_statistics",
            description="Статистика по сделкам за год",
            inputSchema={
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Год (например 2025)"}
                },
                "required": ["year"]
            }
        ),
        # ── EQUIPMENT ──────────────────────────
        types.Tool(
            name="add_equipment",
            description="Добавить технику/инвентарь",
            inputSchema={
                "type": "object",
                "properties": {
                    "name":          {"type": "string"},
                    "model":         {"type": "string", "default": ""},
                    "serial":        {"type": "string", "default": ""},
                    "purchase_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "purchase_cost": {"type": "number", "default": 0},
                    "engine_hours":  {"type": "number", "default": 0, "description": "Наработка в моточасах"},
                    "status":        {"type": "string", "enum": ["active", "repair", "retired"], "default": "active"},
                    "notes":         {"type": "string", "default": ""}
                },
                "required": ["name"]
            }
        ),
        types.Tool(
            name="get_equipment",
            description="Получить список техники",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {"type": "string", "description": "Фильтр: active | repair | retired"}
                }
            }
        ),
        types.Tool(
            name="update_equipment_status",
            description="Обновить статус техники",
            inputSchema={
                "type": "object",
                "properties": {
                    "equipment_id": {"type": "integer"},
                    "status": {"type": "string", "enum": ["active", "repair", "retired"]}
                },
                "required": ["equipment_id", "status"]
            }
        ),
        types.Tool(
            name="schedule_maintenance",
            description="Запланировать или добавить ТО для техники",
            inputSchema={
                "type": "object",
                "properties": {
                    "equipment_id": {"type": "integer"},
                    "date":         {"type": "string", "description": "YYYY-MM-DD"},
                    "description":  {"type": "string"},
                    "cost":         {"type": "number", "default": 0},
                    "performed_by": {"type": "string", "default": ""}
                },
                "required": ["equipment_id", "date", "description"]
            }
        ),
        types.Tool(
            name="get_maintenance_schedule",
            description="Получить предстоящие ТО и историю обслуживания",
            inputSchema={
                "type": "object",
                "properties": {
                    "upcoming_only": {"type": "boolean", "description": "Только предстоящие ТО", "default": True},
                    "days_ahead":    {"type": "integer", "description": "Дней вперёд для поиска", "default": 30}
                }
            }
        ),
        # ── EXPENSES ───────────────────────────
        types.Tool(
            name="add_expense",
            description="Добавить расход",
            inputSchema={
                "type": "object",
                "properties": {
                    "date":         {"type": "string", "description": "YYYY-MM-DD или DD.MM.YYYY"},
                    "name":         {"type": "string", "description": "Наименование расхода"},
                    "category":     {"type": "string", "description": "Категория расхода"},
                    "amount":       {"type": "number"},
                    "equipment_id": {"type": "integer", "description": "ID техники (если применимо)"},
                    "notes":        {"type": "string", "default": ""}
                },
                "required": ["date", "name", "category", "amount"]
            }
        ),
        types.Tool(
            name="get_expenses",
            description="Получить расходы с фильтрами",
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "year":     {"type": "integer"},
                    "month":    {"type": "integer"},
                    "equipment_id": {"type": "integer"}
                }
            }
        ),
        types.Tool(
            name="get_expense_categories",
            description="Получить все категории расходов",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="get_expense_summary",
            description="Сводка расходов по категориям за год",
            inputSchema={
                "type": "object",
                "properties": {"year": {"type": "integer"}},
                "required": ["year"]
            }
        ),
        # ── SERVICES ───────────────────────────
        types.Tool(
            name="get_services",
            description="Получить прайс-лист услуг",
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {"type": "string", "description": "Фильтр по категории"}
                }
            }
        ),
        types.Tool(
            name="get_service_categories",
            description="Получить все категории услуг",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="calculate_service_cost",
            description="Рассчитать стоимость услуги по ID и объёму",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_id": {"type": "integer"},
                    "quantity":   {"type": "number"}
                },
                "required": ["service_id", "quantity"]
            }
        ),
        # ── REPORTS ────────────────────────────
        types.Tool(
            name="generate_profit_loss_report",
            description="Отчёт о прибылях и убытках за год",
            inputSchema={
                "type": "object",
                "properties": {"year": {"type": "integer"}},
                "required": ["year"]
            }
        ),
        types.Tool(
            name="generate_equipment_report",
            description="Полный отчёт по состоянию техники",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="generate_client_history_report",
            description="История всех сделок клиента",
            inputSchema={
                "type": "object",
                "properties": {
                    "client_name": {"type": "string"}
                },
                "required": ["client_name"]
            }
        ),
    ]


# ─────────────────────────────────────────────
#  TOOL HANDLERS
# ─────────────────────────────────────────────

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    with Session() as session:
        try:
            return await _dispatch(name, arguments, session)
        except Exception as e:
            return _err(f"Ошибка при выполнении {name}: {e}")


async def _dispatch(name: str, args: dict, session) -> list[types.TextContent]:

    # ── DEALS ──────────────────────────────────
    if name == "create_deal":
        stage = session.query(Stage).filter_by(name="Согласовать").first()
        deal = Deal(
            title=args["title"],
            client=args["client"],
            manager=args.get("manager", ""),
            address=args.get("address", ""),
            notes=args.get("notes", ""),
            stage_id=stage.id if stage else None
        )
        session.add(deal)
        session.flush()

        total = 0.0
        for svc in args.get("services", []):
            service = session.query(Service).get(svc["service_id"])
            if not service:
                continue
            qty = max(float(svc["quantity"]), service.min_volume)
            ds = DealService(
                deal_id=deal.id,
                service_id=service.id,
                quantity=qty,
                price_at_moment=service.price
            )
            session.add(ds)
            total += qty * service.price

        session.commit()
        return _ok({
            "id": deal.id,
            "title": deal.title,
            "client": deal.client,
            "stage": stage.name if stage else None,
            "total": total,
            "created_at": str(deal.created_at)
        })

    if name == "get_deals":
        q = session.query(Deal)
        if args.get("status"):
            stage = session.query(Stage).filter(
                Stage.name.ilike(f"%{args['status']}%")
            ).first()
            if stage:
                q = q.filter(Deal.stage_id == stage.id)
        if args.get("client"):
            q = q.filter(Deal.client.ilike(f"%{args['client']}%"))
        if args.get("year"):
            q = q.filter(extract("year", Deal.created_at) == args["year"])
        if args.get("month"):
            q = q.filter(extract("month", Deal.created_at) == args["month"])
        if args.get("date_from"):
            d = _parse_date(args["date_from"])
            if d:
                q = q.filter(Deal.created_at >= d)
        if args.get("date_to"):
            d = _parse_date(args["date_to"])
            if d:
                q = q.filter(Deal.created_at <= d)

        deals = q.order_by(Deal.created_at.desc()).all()
        result = []
        for deal in deals:
            total = _deal_total(deal, session)
            result.append({
                "id": deal.id,
                "title": deal.title,
                "client": deal.client,
                "stage": deal.stage.name if deal.stage else None,
                "manager": deal.manager,
                "total": total,
                "services_count": len(deal.deal_services),
                "created_at": str(deal.created_at),
            })
        return _ok({"deals": result, "count": len(result), "total_sum": sum(d["total"] for d in result)})

    if name == "update_deal_stage":
        deal = session.query(Deal).get(args["deal_id"])
        if not deal:
            return _err(f"Сделка {args['deal_id']} не найдена")
        stage = session.query(Stage).filter(
            Stage.name.ilike(f"%{args['stage_name']}%")
        ).first()
        if not stage:
            return _err(f"Этап '{args['stage_name']}' не найден")
        deal.stage_id = stage.id
        deal.updated_at = datetime.utcnow()
        if stage.is_final:
            deal.closed_at = datetime.utcnow()
        session.commit()
        return _ok({"deal_id": deal.id, "new_stage": stage.name})

    if name == "get_deal_stages":
        stages = session.query(Stage).order_by(Stage.order).all()
        return _ok([{
            "id": s.id, "name": s.name, "order": s.order,
            "type": s.type, "is_final": s.is_final, "color": s.color,
            "deals_count": len(s.deals)
        } for s in stages])

    if name == "get_deal_statistics":
        year = args["year"]
        deals = session.query(Deal).filter(
            extract("year", Deal.created_at) == year
        ).all()
        
        by_stage: dict[str, dict] = {}
        by_month: dict[int, dict] = {}
        total_revenue = 0.0
        
        for deal in deals:
            total = _deal_total(deal, session)
            stage_name = deal.stage.name if deal.stage else "Неизвестно"
            
            by_stage.setdefault(stage_name, {"count": 0, "sum": 0.0})
            by_stage[stage_name]["count"] += 1
            by_stage[stage_name]["sum"] += total
            
            m = deal.created_at.month
            by_month.setdefault(m, {"count": 0, "sum": 0.0})
            by_month[m]["count"] += 1
            by_month[m]["sum"] += total
            
            if deal.stage and deal.stage.type == "success":
                total_revenue += total
        
        return _ok({
            "year": year,
            "total_deals": len(deals),
            "total_revenue": total_revenue,
            "by_stage": by_stage,
            "by_month": {k: v for k, v in sorted(by_month.items())}
        })

    # ── EQUIPMENT ──────────────────────────────
    if name == "add_equipment":
        eq = Equipment(
            name=args["name"],
            model=args.get("model", ""),
            serial=args.get("serial", ""),
            purchase_date=_parse_date(args.get("purchase_date")),
            purchase_cost=args.get("purchase_cost", 0),
            engine_hours=args.get("engine_hours", 0),
            status=args.get("status", "active"),
            notes=args.get("notes", "")
        )
        session.add(eq)
        session.commit()
        return _ok({"id": eq.id, "name": eq.name, "status": eq.status, "engine_hours": eq.engine_hours})

    if name == "get_equipment":
        q = session.query(Equipment)
        if args.get("status"):
            q = q.filter(Equipment.status == args["status"])
        eqs = q.order_by(Equipment.name).all()
        return _ok([{
            "id": e.id, "name": e.name, "model": e.model,
            "status": e.status, "purchase_cost": e.purchase_cost, "engine_hours": e.engine_hours,
            "last_maintenance": str(e.last_maintenance) if e.last_maintenance else None,
            "next_maintenance": str(e.next_maintenance) if e.next_maintenance else None,
            "notes": e.notes
        } for e in eqs])

    if name == "update_equipment_status":
        eq = session.query(Equipment).get(args["equipment_id"])
        if not eq:
            return _err(f"Техника {args['equipment_id']} не найдена")
        eq.status = args["status"]
        session.commit()
        return _ok({"id": eq.id, "name": eq.name, "status": eq.status})

    if name == "schedule_maintenance":
        eq = session.query(Equipment).get(args["equipment_id"])
        if not eq:
            return _err(f"Техника {args['equipment_id']} не найдена")
        maint_date = _parse_date(args["date"])
        m = Maintenance(
            equipment_id=eq.id,
            date=maint_date,
            description=args["description"],
            cost=args.get("cost", 0),
            performed_by=args.get("performed_by", "")
        )
        session.add(m)
        # Обновляем даты на технике
        today = date.today()
        if maint_date and maint_date <= today:
            eq.last_maintenance = maint_date
        elif maint_date and maint_date > today:
            eq.next_maintenance = maint_date
        session.commit()
        return _ok({"maintenance_id": m.id, "equipment": eq.name, "date": str(maint_date)})

    if name == "get_maintenance_schedule":
        upcoming_only = args.get("upcoming_only", True)
        days = args.get("days_ahead", 30)
        today = date.today()
        
        q = session.query(Maintenance)
        if upcoming_only:
            future = today + timedelta(days=days)
            q = q.filter(and_(Maintenance.date >= today, Maintenance.date <= future))
        
        maintenances = q.order_by(Maintenance.date).all()
        return _ok([{
            "id": m.id,
            "equipment": m.equipment.name if m.equipment else None,
            "date": str(m.date),
            "description": m.description,
            "cost": m.cost
        } for m in maintenances])

    # ── EXPENSES ───────────────────────────────
    if name == "add_expense":
        parsed_date = _parse_date(args["date"])
        if not parsed_date:
            return _err("Неверный формат даты. Используйте YYYY-MM-DD или DD.MM.YYYY")
        
        cat = session.query(ExpenseCategory).filter(
            ExpenseCategory.name.ilike(f"%{args['category']}%")
        ).first()
        if not cat:
            cat = session.query(ExpenseCategory).first()
        
        exp = Expense(
            date=parsed_date,
            name=args["name"],
            category_id=cat.id if cat else None,
            amount=args["amount"],
            year=parsed_date.year,
            equipment_id=args.get("equipment_id"),
            notes=args.get("notes", "")
        )
        session.add(exp)
        session.commit()
        return _ok({"id": exp.id, "name": exp.name, "amount": exp.amount, "category": cat.name if cat else None})

    if name == "get_expenses":
        q = session.query(Expense)
        if args.get("category"):
            cat = session.query(ExpenseCategory).filter(
                ExpenseCategory.name.ilike(f"%{args['category']}%")
            ).first()
            if cat:
                q = q.filter(Expense.category_id == cat.id)
        if args.get("year"):
            q = q.filter(Expense.year == args["year"])
        if args.get("month"):
            q = q.filter(extract("month", Expense.date) == args["month"])
        if args.get("equipment_id"):
            q = q.filter(Expense.equipment_id == args["equipment_id"])
        
        expenses = q.order_by(Expense.date.desc()).all()
        return _ok({
            "expenses": [{
                "id": e.id, "date": str(e.date), "name": e.name,
                "category": e.category.name if e.category else None,
                "amount": e.amount,
                "equipment": e.equipment.name if e.equipment else None
            } for e in expenses],
            "total": sum(e.amount for e in expenses)
        })

    if name == "get_expense_categories":
        cats = session.query(ExpenseCategory).all()
        return _ok([{"id": c.id, "name": c.name} for c in cats])

    if name == "get_expense_summary":
        year = args["year"]
        rows = session.query(
            ExpenseCategory.name,
            func.sum(Expense.amount).label("total"),
            func.count(Expense.id).label("count")
        ).join(Expense, Expense.category_id == ExpenseCategory.id)\
         .filter(Expense.year == year)\
         .group_by(ExpenseCategory.name)\
         .order_by(func.sum(Expense.amount).desc())\
         .all()
        
        total = sum(r.total for r in rows)
        return _ok({
            "year": year,
            "total": total,
            "by_category": [{"category": r.name, "total": r.total, "count": r.count} for r in rows]
        })

    # ── SERVICES ───────────────────────────────
    if name == "get_services":
        q = session.query(Service)
        if args.get("category"):
            q = q.join(ServiceCategory).filter(
                ServiceCategory.name.ilike(f"%{args['category']}%")
            )
        services = q.all()
        return _ok([{
            "id": s.id, "name": s.name,
            "category": s.category.name if s.category else None,
            "unit": s.unit, "price": s.price, "min_volume": s.min_volume
        } for s in services])

    if name == "get_service_categories":
        cats = session.query(ServiceCategory).all()
        return _ok([{"id": c.id, "name": c.name, "icon": c.icon} for c in cats])

    if name == "calculate_service_cost":
        svc = session.query(Service).get(args["service_id"])
        if not svc:
            return _err(f"Услуга {args['service_id']} не найдена")
        qty = max(float(args["quantity"]), svc.min_volume)
        applied_min = qty > float(args["quantity"])
        return _ok({
            "service": svc.name,
            "unit": svc.unit,
            "price_per_unit": svc.price,
            "quantity_requested": args["quantity"],
            "quantity_applied": qty,
            "min_volume_applied": applied_min,
            "total_cost": qty * svc.price
        })

    # ── REPORTS ────────────────────────────────
    if name == "generate_profit_loss_report":
        year = args["year"]
        
        # Доходы (успешные сделки)
        success_stage_ids = [
            s.id for s in session.query(Stage).filter_by(type="success").all()
        ]
        deals = session.query(Deal).filter(
            extract("year", Deal.created_at) == year,
            Deal.stage_id.in_(success_stage_ids)
        ).all() if success_stage_ids else []
        
        revenue = sum(_deal_total(d, session) for d in deals)
        
        # Расходы
        exp_summary = session.query(
            ExpenseCategory.name,
            func.sum(Expense.amount).label("total")
        ).join(Expense, Expense.category_id == ExpenseCategory.id)\
         .filter(Expense.year == year)\
         .group_by(ExpenseCategory.name)\
         .all()
        
        total_expenses = sum(r.total for r in exp_summary)
        profit = revenue - total_expenses
        margin = (profit / revenue * 100) if revenue > 0 else 0
        
        return _ok({
            "year": year,
            "revenue": revenue,
            "successful_deals": len(deals),
            "total_expenses": total_expenses,
            "profit": profit,
            "margin_percent": round(margin, 1),
            "expense_breakdown": [{"category": r.name, "amount": r.total} for r in exp_summary]
        })

    if name == "generate_equipment_report":
        eqs = session.query(Equipment).all()
        today = date.today()
        result = []
        for e in eqs:
            needs_maintenance = (
                e.next_maintenance and e.next_maintenance <= today + timedelta(days=14)
            )
            result.append({
                "id": e.id, "name": e.name, "model": e.model,
                "status": e.status,
                "purchase_cost": e.purchase_cost,
                "last_maintenance": str(e.last_maintenance) if e.last_maintenance else None,
                "next_maintenance": str(e.next_maintenance) if e.next_maintenance else None,
                "needs_maintenance_soon": needs_maintenance,
                "maintenance_count": len(e.maintenances),
                "total_maintenance_cost": sum(m.cost for m in e.maintenances)
            })
        
        summary = {
            "total": len(eqs),
            "active": sum(1 for e in eqs if e.status == "active"),
            "in_repair": sum(1 for e in eqs if e.status == "repair"),
            "retired": sum(1 for e in eqs if e.status == "retired"),
            "needs_attention": sum(1 for e in result if e["needs_maintenance_soon"])
        }
        return _ok({"summary": summary, "equipment": result})

    if name == "generate_client_history_report":
        client_filter = args["client_name"]
        deals = session.query(Deal).filter(
            Deal.client.ilike(f"%{client_filter}%")
        ).order_by(Deal.created_at.desc()).all()
        
        total_revenue = 0.0
        deal_list = []
        for deal in deals:
            total = _deal_total(deal, session)
            if deal.stage and deal.stage.type == "success":
                total_revenue += total
            deal_list.append({
                "id": deal.id, "title": deal.title,
                "stage": deal.stage.name if deal.stage else None,
                "total": total,
                "services": [
                    {"name": ds.service.name if ds.service else "?", "quantity": ds.quantity, "price": ds.price_at_moment}
                    for ds in deal.deal_services
                ],
                "created_at": str(deal.created_at)
            })
        
        return _ok({
            "client": client_filter,
            "total_deals": len(deals),
            "total_revenue": total_revenue,
            "deals": deal_list
        })

    return _err(f"Неизвестный инструмент: {name}")


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────

async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
