"""
FastAPI REST API для CRM
Запуск: uvicorn api.main:app --reload --port 8000
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, date
from typing import Optional, List, Any
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import extract, func
from sqlalchemy.orm import Session as DBSession
import io

from datetime import datetime, timedelta
from models.user import User
from api.auth_module import (
    hash_password, verify_password,
    create_token, get_current_user, require_admin
)

from models.database import (
    get_engine, get_session_factory, init_db,
    Stage, Service, ServiceCategory, Deal, DealService,
    Equipment, Maintenance, ExpenseCategory, Expense
)
from scripts.init_db import seed_database

engine = get_engine()
init_db(engine)

# Создаём таблицу users если нет
from models.database import Base
from models.user import User as UserModel
Base.metadata.create_all(engine)

SessionFactory = get_session_factory(engine)

# Автозаполнение БД начальными данными (если база пустая)
with SessionFactory() as seed_session:
    seed_database(seed_session)

app = FastAPI(title="Grass CRM API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionFactory()
    try:
        yield db
    finally:
        db.close()


# ── Pydantic schemas ──────────────────────────


# ── Auth schemas & endpoints ──────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreate(BaseModel):
    username:  str
    password:  str
    full_name: str = ""
    role:      str = "user"


@app.post("/api/auth/login")
def login(body: LoginRequest, db: DBSession = Depends(get_db)):
    """Вход — возвращает JWT токен"""
    user = db.query(UserModel).filter_by(username=body.username.lower().strip()).first()
    if not user or not user.is_active:
        raise HTTPException(401, "Неверный логин или пароль")
    if not verify_password(body.password, user.password_hash):
        raise HTTPException(401, "Неверный логин или пароль")
    user.last_login = datetime.utcnow()
    db.commit()
    token = create_token(user.username, user.role)
    return {
        "access_token": token,
        "token_type":   "bearer",
        "username":     user.username,
        "full_name":    user.full_name,
        "role":         user.role,
        "expires_in":   8 * 3600,
    }


@app.get("/api/auth/me")
def get_me(current_user: dict = Depends(get_current_user)):
    return current_user


@app.get("/api/users", dependencies=[Depends(require_admin)])
def list_users(db: DBSession = Depends(get_db)):
    users = db.query(UserModel).all()
    return [{"id": u.id, "username": u.username, "full_name": u.full_name,
             "role": u.role, "is_active": u.is_active,
             "last_login": u.last_login.isoformat() if u.last_login else None}
            for u in users]


@app.post("/api/users", dependencies=[Depends(require_admin)], status_code=201)
def create_user_endpoint(body: UserCreate, db: DBSession = Depends(get_db)):
    existing = db.query(UserModel).filter_by(username=body.username.lower()).first()
    if existing:
        raise HTTPException(400, f"Пользователь '{body.username}' уже существует")
    user = UserModel(
        username=body.username.lower(),
        password_hash=hash_password(body.password),
        full_name=body.full_name,
        role=body.role,
    )
    db.add(user)
    db.commit()
    return {"id": user.id, "username": user.username, "role": user.role}


@app.delete("/api/users/{username}", dependencies=[Depends(require_admin)])
def delete_user(username: str, db: DBSession = Depends(get_db)):
    user = db.query(UserModel).filter_by(username=username).first()
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    if username == "admin":
        raise HTTPException(400, "Нельзя удалить главного администратора")
    user.is_active = False
    db.commit()
    return {"detail": f"Пользователь {username} деактивирован"}



class DealServiceIn(BaseModel):
    service_id: int
    quantity: float

class DealCreate(BaseModel):
    title: str
    client: str
    manager: str = ""
    address: str = ""
    notes: str = ""
    services: Any = []

class DealUpdate(BaseModel):
    title: str
    client: str
    manager: str = ""
    address: str = ""
    notes: str = ""
    services: Any = []

class StageUpdate(BaseModel):
    stage_name: str

class ExpenseCreate(BaseModel):
    date: str
    name: str
    category: str
    amount: float
    equipment_id: Optional[int] = None
    notes: str = ""

class EquipmentCreate(BaseModel):
    name: str
    model: str = ""
    serial: str = ""
    purchase_date: Optional[str] = None
    purchase_cost: float = 0
    engine_hours: float = 0
    status: str = "active"
    notes: str = ""

class MaintenanceCreate(BaseModel):
    date: str
    description: str
    cost: float = 0
    performed_by: str = ""


# ── STAGES ────────────────────────────────────

@app.get("/api/stages")
def get_stages(db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    stages = db.query(Stage).order_by(Stage.order).all()
    return [{
        "id": s.id, "name": s.name, "order": s.order,
        "type": s.type, "is_final": s.is_final, "color": s.color,
        "deals_count": db.query(Deal).filter(Deal.stage_id == s.id).count()
    } for s in stages]


# ── DEALS ─────────────────────────────────────

@app.get("/api/deals")
def get_deals(
    stage: Optional[str] = Query(None),
    client: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    db: DBSession = Depends(get_db)
):
    q = db.query(Deal)
    if stage:
        s = db.query(Stage).filter(Stage.name.ilike(f"%{stage}%")).first()
        if s:
            q = q.filter(Deal.stage_id == s.id)
    if client:
        q = q.filter(Deal.client.ilike(f"%{client}%"))
    if year:
        q = q.filter(extract("year", Deal.created_at) == year)
    if month:
        q = q.filter(extract("month", Deal.created_at) == month)

    deals = q.order_by(Deal.created_at.desc()).all()
    result = []
    for d in deals:
        total = sum(ds.quantity * ds.price_at_moment for ds in d.deal_services)
        result.append({
            "id": d.id, "title": d.title, "client": d.client,
            "stage": d.stage.name if d.stage else None,
            "stage_color": d.stage.color if d.stage else "#6B7280",
            "manager": d.manager, "address": d.address,
            "total": total,
            "services": [{"name": ds.service.name if ds.service else "?", "qty": ds.quantity, "price": ds.price_at_moment}
                         for ds in d.deal_services],
            "created_at": d.created_at.isoformat()
        })
    return {"deals": result, "count": len(result), "total_sum": sum(d["total"] for d in result)}


def _add_services_to_deal(db: DBSession, deal_id: int, services_raw: Any):
    """Поддерживает оба формата услуг: старый API-список и фронтовую строку name:qty,name2:qty."""
    if not services_raw:
        return

    # Формат API: [{service_id, quantity}, ...]
    if isinstance(services_raw, list):
        for svc_in in services_raw:
            try:
                service_id = int(svc_in.get("service_id")) if isinstance(svc_in, dict) else int(getattr(svc_in, "service_id"))
                quantity = float(svc_in.get("quantity")) if isinstance(svc_in, dict) else float(getattr(svc_in, "quantity"))
            except Exception:
                continue
            svc = db.query(Service).get(service_id)
            if svc:
                qty = max(quantity, svc.min_volume)
                db.add(DealService(deal_id=deal_id, service_id=svc.id, quantity=qty, price_at_moment=svc.price))
        return

    # Формат фронта: "Название:2,Название2:1"
    if isinstance(services_raw, str):
        for chunk in services_raw.split(','):
            if ':' not in chunk:
                continue
            name, qty_raw = chunk.split(':', 1)
            name = name.strip()
            if not name:
                continue
            try:
                qty = max(float(qty_raw), 0)
            except Exception:
                qty = 0
            if qty <= 0:
                continue

            svc = db.query(Service).filter(Service.name == name).first()
            if not svc:
                svc = db.query(Service).filter(Service.name.ilike(f"%{name}%")).first()
            if svc:
                db.add(DealService(deal_id=deal_id, service_id=svc.id, quantity=max(qty, svc.min_volume), price_at_moment=svc.price))


@app.post("/api/deals", status_code=201)
def create_deal(body: DealCreate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    stage = db.query(Stage).filter_by(name="Согласовать").first()
    deal = Deal(
        title=body.title, client=body.client,
        manager=body.manager, address=body.address,
        notes=body.notes,
        stage_id=stage.id if stage else None
    )
    db.add(deal)
    db.flush()

    _add_services_to_deal(db, deal.id, body.services)

    db.commit()
    return {"id": deal.id, "title": deal.title, "client": deal.client}


@app.put("/api/deals/{deal_id}")
def update_deal(deal_id: int, body: DealUpdate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    deal = db.query(Deal).get(deal_id)
    if not deal:
        raise HTTPException(404, "Сделка не найдена")

    deal.title = body.title
    deal.client = body.client
    deal.manager = body.manager
    deal.address = body.address
    deal.notes = body.notes
    deal.updated_at = datetime.utcnow()

    db.query(DealService).filter(DealService.deal_id == deal.id).delete()
    _add_services_to_deal(db, deal.id, body.services)

    db.commit()
    return {"id": deal.id, "title": deal.title, "client": deal.client}


@app.delete("/api/deals/{deal_id}", dependencies=[Depends(require_admin)])
def delete_deal(deal_id: int, db: DBSession = Depends(get_db)):
    deal = db.query(Deal).get(deal_id)
    if not deal:
        raise HTTPException(404, "Сделка не найдена")
    db.delete(deal)
    db.commit()
    return {"detail": "Сделка удалена", "id": deal_id}


@app.patch("/api/deals/{deal_id}/stage")
def update_deal_stage(deal_id: int, body: StageUpdate, db: DBSession = Depends(get_db)):
    deal = db.query(Deal).get(deal_id)
    if not deal:
        raise HTTPException(404, "Сделка не найдена")
    stage = db.query(Stage).filter(Stage.name.ilike(f"%{body.stage_name}%")).first()
    if not stage:
        raise HTTPException(404, f"Этап '{body.stage_name}' не найден")
    deal.stage_id = stage.id
    deal.updated_at = datetime.utcnow()
    if stage.is_final:
        deal.closed_at = datetime.utcnow()
    db.commit()
    return {"deal_id": deal.id, "new_stage": stage.name}


@app.get("/api/deals/{deal_id}")
def get_deal(deal_id: int, db: DBSession = Depends(get_db)):
    deal = db.query(Deal).get(deal_id)
    if not deal:
        raise HTTPException(404, "Сделка не найдена")
    total = sum(ds.quantity * ds.price_at_moment for ds in deal.deal_services)
    return {
        "id": deal.id, "title": deal.title, "client": deal.client,
        "stage": deal.stage.name if deal.stage else None,
        "manager": deal.manager, "address": deal.address, "notes": deal.notes,
        "total": total,
        "services": [{"id": ds.id, "service_id": ds.service_id,
                      "name": ds.service.name if ds.service else "?",
                      "quantity": ds.quantity, "price": ds.price_at_moment,
                      "subtotal": ds.quantity * ds.price_at_moment}
                     for ds in deal.deal_services],
        "created_at": deal.created_at.isoformat()
    }


# ── EQUIPMENT ─────────────────────────────────

@app.get("/api/equipment")
def get_equipment(status: Optional[str] = Query(None), db: DBSession = Depends(get_db)):
    q = db.query(Equipment)
    if status:
        q = q.filter(Equipment.status == status)
    eqs = q.order_by(Equipment.name).all()
    return [{"id": e.id, "name": e.name, "model": e.model, "status": e.status,
             "purchase_date": str(e.purchase_date) if e.purchase_date else None,
             "purchase_cost": e.purchase_cost,
             "engine_hours": e.engine_hours,
             "last_maintenance": str(e.last_maintenance) if e.last_maintenance else None,
             "next_maintenance": str(e.next_maintenance) if e.next_maintenance else None,
             "notes": e.notes} for e in eqs]


@app.post("/api/equipment")
def create_equipment(body: EquipmentCreate, db: DBSession = Depends(get_db)):
    eq = Equipment(
        name=body.name, model=body.model, serial=body.serial,
        purchase_cost=body.purchase_cost, engine_hours=body.engine_hours, status=body.status, notes=body.notes
    )
    if body.purchase_date:
        try:
            eq.purchase_date = datetime.strptime(body.purchase_date, "%Y-%m-%d").date()
        except ValueError:
            pass
    db.add(eq)
    db.commit()
    return {"id": eq.id, "name": eq.name}


@app.put("/api/equipment/{equipment_id}", dependencies=[Depends(require_admin)])
def update_equipment(equipment_id: int, body: EquipmentCreate, db: DBSession = Depends(get_db)):
    eq = db.query(Equipment).get(equipment_id)
    if not eq:
        raise HTTPException(404, "Техника не найдена")

    eq.name = body.name
    eq.model = body.model
    eq.serial = body.serial
    eq.purchase_cost = body.purchase_cost
    eq.engine_hours = body.engine_hours
    eq.status = body.status
    eq.notes = body.notes
    if body.purchase_date:
        try:
            eq.purchase_date = datetime.strptime(body.purchase_date, "%Y-%m-%d").date()
        except ValueError:
            pass
    db.commit()
    return {"id": eq.id, "name": eq.name}


@app.get("/api/maintenances")
def get_maintenances(equipment_id: Optional[int] = Query(None), db: DBSession = Depends(get_db)):
    q = db.query(Maintenance)
    if equipment_id:
        q = q.filter(Maintenance.equipment_id == equipment_id)
    rows = q.order_by(Maintenance.date.desc()).all()
    return [{
        "id": m.id,
        "equipment_id": m.equipment_id,
        "equipment": m.equipment.name if m.equipment else None,
        "date": str(m.date),
        "description": m.description,
        "cost": m.cost,
        "performed_by": m.performed_by,
    } for m in rows]


@app.post("/api/equipment/{equipment_id}/maintenance")
def add_maintenance(equipment_id: int, body: MaintenanceCreate, db: DBSession = Depends(get_db)):
    eq = db.query(Equipment).get(equipment_id)
    if not eq:
        raise HTTPException(404, "Техника не найдена")
    maint_date = datetime.strptime(body.date, "%Y-%m-%d").date()
    m = Maintenance(equipment_id=equipment_id, date=maint_date,
                    description=body.description, cost=body.cost, performed_by=body.performed_by)
    db.add(m)
    today = date.today()
    if maint_date <= today:
        eq.last_maintenance = maint_date
    else:
        eq.next_maintenance = maint_date
    db.commit()
    return {"id": m.id, "equipment": eq.name, "date": str(maint_date)}


@app.patch("/api/equipment/{equipment_id}/status")
def update_equipment_status(equipment_id: int, status: str, db: DBSession = Depends(get_db)):
    eq = db.query(Equipment).get(equipment_id)
    if not eq:
        raise HTTPException(404, "Техника не найдена")
    if status not in ("active", "repair", "retired"):
        raise HTTPException(400, "Статус должен быть: active | repair | retired")
    eq.status = status
    db.commit()
    return {"id": eq.id, "name": eq.name, "status": eq.status}


# ── EXPENSES ──────────────────────────────────

@app.get("/api/expenses")
def get_expenses(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    category: Optional[str] = Query(None),
    db: DBSession = Depends(get_db)
):
    q = db.query(Expense)
    if year:
        q = q.filter(Expense.year == year)
    if month:
        q = q.filter(extract("month", Expense.date) == month)
    if category:
        cat = db.query(ExpenseCategory).filter(ExpenseCategory.name.ilike(f"%{category}%")).first()
        if cat:
            q = q.filter(Expense.category_id == cat.id)
    expenses = q.order_by(Expense.date.desc()).all()
    return {
        "expenses": [{"id": e.id, "date": str(e.date), "name": e.name,
                      "category": e.category.name if e.category else None,
                      "amount": e.amount,
                      "equipment": e.equipment.name if e.equipment else None}
                     for e in expenses],
        "total": sum(e.amount for e in expenses)
    }


@app.post("/api/expenses")
def create_expense(body: ExpenseCreate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    try:
        parsed_date = datetime.strptime(body.date, "%Y-%m-%d").date()
    except ValueError:
        try:
            parsed_date = datetime.strptime(body.date, "%d.%m.%Y").date()
        except ValueError:
            raise HTTPException(400, "Неверный формат даты")
    
    cat = db.query(ExpenseCategory).filter(ExpenseCategory.name.ilike(f"%{body.category}%")).first()
    exp = Expense(date=parsed_date, name=body.name, category_id=cat.id if cat else None,
                  amount=body.amount, year=parsed_date.year,
                  equipment_id=body.equipment_id, notes=body.notes)
    db.add(exp)
    db.commit()
    return {"id": exp.id, "name": exp.name, "amount": exp.amount}


@app.delete("/api/expenses/{expense_id}", dependencies=[Depends(require_admin)])
def delete_expense(expense_id: int, db: DBSession = Depends(get_db)):
    exp = db.query(Expense).get(expense_id)
    if not exp:
        raise HTTPException(404, "Расход не найден")
    db.delete(exp)
    db.commit()
    return {"detail": "Расход удалён", "id": expense_id}


# ── SERVICES ──────────────────────────────────

@app.get("/api/services")
def get_services(category: Optional[str] = Query(None), db: DBSession = Depends(get_db)):
    q = db.query(Service)
    if category:
        q = q.join(ServiceCategory).filter(ServiceCategory.name.ilike(f"%{category}%"))
    return [{"id": s.id, "name": s.name, "category": s.category.name if s.category else None,
             "unit": s.unit, "price": s.price, "min_volume": s.min_volume} for s in q.all()]


@app.get("/api/services/categories")
def get_service_categories(db: DBSession = Depends(get_db)):
    return [{"id": c.id, "name": c.name, "icon": c.icon}
            for c in db.query(ServiceCategory).all()]


# ── REPORTS ───────────────────────────────────

@app.get("/api/reports/profit-loss/{year}")
def profit_loss(year: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    success_ids = [s.id for s in db.query(Stage).filter_by(type="success").all()]
    deals = db.query(Deal).filter(
        extract("year", Deal.created_at) == year,
        Deal.stage_id.in_(success_ids)
    ).all() if success_ids else []
    
    revenue = sum(sum(ds.quantity * ds.price_at_moment for ds in d.deal_services) for d in deals)
    
    exp_rows = db.query(
        ExpenseCategory.name, func.sum(Expense.amount).label("total")
    ).join(Expense, Expense.category_id == ExpenseCategory.id)\
     .filter(Expense.year == year)\
     .group_by(ExpenseCategory.name).all()
    
    total_exp = sum(r.total for r in exp_rows)
    profit = revenue - total_exp
    
    return {
        "year": year, "revenue": revenue, "total_expenses": total_exp,
        "profit": profit, "margin_percent": round(profit / revenue * 100, 1) if revenue else 0,
        "successful_deals": len(deals),
        "expense_breakdown": [{"category": r.name, "amount": r.total} for r in exp_rows]
    }


@app.get("/api/reports/export/{report_type}/{year}")
def export_report(report_type: str, year: int, db: DBSession = Depends(get_db)):
    """Экспорт отчёта в CSV (Excel-совместимый)"""
    import csv
    
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    
    if report_type == "deals":
        writer.writerow(["ID", "Название", "Клиент", "Этап", "Менеджер", "Сумма", "Дата создания"])
        deals = db.query(Deal).filter(extract("year", Deal.created_at) == year).all()
        for d in deals:
            total = sum(ds.quantity * ds.price_at_moment for ds in d.deal_services)
            writer.writerow([d.id, d.title, d.client, d.stage.name if d.stage else "", d.manager, total, d.created_at.strftime("%d.%m.%Y")])
    
    elif report_type == "expenses":
        writer.writerow(["ID", "Дата", "Наименование", "Категория", "Сумма", "Техника"])
        exps = db.query(Expense).filter(Expense.year == year).order_by(Expense.date).all()
        for e in exps:
            writer.writerow([e.id, str(e.date), e.name, e.category.name if e.category else "", e.amount, e.equipment.name if e.equipment else ""])
    
    elif report_type == "profit-loss":
        success_ids = [s.id for s in db.query(Stage).filter_by(type="success").all()]
        deals = db.query(Deal).filter(extract("year", Deal.created_at) == year, Deal.stage_id.in_(success_ids)).all() if success_ids else []
        revenue = sum(sum(ds.quantity * ds.price_at_moment for ds in d.deal_services) for d in deals)
        exp_rows = db.query(ExpenseCategory.name, func.sum(Expense.amount).label("total")).join(Expense).filter(Expense.year == year).group_by(ExpenseCategory.name).all()
        total_exp = sum(r.total for r in exp_rows)
        writer.writerow(["Показатель", "Значение"])
        writer.writerow(["Год", year])
        writer.writerow(["Выручка (₽)", revenue])
        writer.writerow(["Расходы (₽)", total_exp])
        writer.writerow(["Прибыль (₽)", revenue - total_exp])
        writer.writerow(["Маржа (%)", round((revenue - total_exp) / revenue * 100, 1) if revenue else 0])
        writer.writerow([])
        writer.writerow(["Категория расходов", "Сумма"])
        for r in exp_rows:
            writer.writerow([r.name, r.total])
    
    output.seek(0)
    filename = f"{report_type}_{year}.csv"
    return StreamingResponse(
        iter([output.getvalue().encode("utf-8-sig")]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
