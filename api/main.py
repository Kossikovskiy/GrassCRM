"""
FastAPI REST API для CRM
Запуск: uvicorn api.main:app --reload --port 8000
"""

import sys
import os
import urllib.request
import urllib.parse
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, date
from contextlib import asynccontextmanager
from typing import Optional, List
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
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

engine = get_engine()
init_db(engine)

# Создаём таблицу users если нет
from models.database import Base
from models.user import User as UserModel
Base.metadata.create_all(engine)

SessionFactory = get_session_factory(engine)


# ════════════════════════════════════════
#  TELEGRAM — настройки
# ════════════════════════════════════════
TG_TOKEN      = os.getenv("TG_TOKEN", "ВСТАВЬТЕ_ТОКЕН_ОТ_BOTFATHER")
TG_CHAT       = os.getenv("TG_CHAT",  "-4993820220")
NOTIFY_HOUR   = int(os.getenv("NOTIFY_HOUR",   "9"))
NOTIFY_MINUTE = int(os.getenv("NOTIFY_MINUTE", "0"))


def tg_send(text: str) -> bool:
    if not TG_TOKEN or TG_TOKEN.startswith("ВСТАВЬТЕ"):
        print("[TG] Токен не задан, пропускаем отправку")
        return False
    url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id":    TG_CHAT,
        "text":       text,
        "parse_mode": "HTML",
    }).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
        print("[TG] ✅ Сообщение отправлено")
        return True
    except Exception as e:
        print(f"[TG] ❌ Ошибка: {e}")
        return False


def build_morning_report() -> str:
    today   = date.today()
    in_week = today + timedelta(days=7)

    with SessionFactory() as session:
        active_names  = ["Запланировано", "В работе", "Ожидание"]
        active_stages = session.query(Stage).filter(Stage.name.in_(active_names)).all()
        active_ids    = [s.id for s in active_stages]
        active_deals  = session.query(Deal).filter(
            Deal.stage_id.in_(active_ids)
        ).order_by(Deal.created_at).all() if active_ids else []

        equip_soon = session.query(Equipment).filter(
            Equipment.next_maintenance >= today,
            Equipment.next_maintenance <= in_week,
            Equipment.status == "active"
        ).all()

        in_repair = session.query(Equipment).filter(
            Equipment.status == "repair"
        ).all()

    DAYS = ["Понедельник","Вторник","Среда","Четверг","Пятница","Суббота","Воскресенье"]
    day_name = DAYS[today.weekday()]

    lines = [
        f"🌿 <b>GreenCRM — Доброе утро!</b>",
        f"📅 {today.strftime('%d.%m.%Y')}, {day_name}",
        "",
    ]

    if active_deals:
        lines.append(f"📋 <b>Активных сделок: {len(active_deals)}</b>")
        icons = {"Запланировано": "🗓", "В работе": "⚡", "Ожидание": "⏳"}
        for deal in active_deals[:10]:
            total   = sum(ds.quantity * ds.price_at_moment for ds in deal.deal_services)
            icon    = icons.get(deal.stage.name if deal.stage else "", "📌")
            sum_str = f"{int(total):,} ₽".replace(",", " ") if total else "—"
            lines.append(f"  {icon} <b>{deal.client}</b> — {sum_str}")
            lines.append(f"      {deal.title[:55]}")
        if len(active_deals) > 10:
            lines.append(f"  … и ещё {len(active_deals) - 10}")
    else:
        lines.append("📋 <b>Активных сделок нет</b> — самое время создать новые! 💪")

    lines.append("")

    if equip_soon:
        lines.append("🔧 <b>Нужно ТО на этой неделе:</b>")
        for eq in equip_soon:
            days_left = (eq.next_maintenance - today).days
            when = "сегодня!" if days_left == 0 else ("завтра" if days_left == 1 else f"через {days_left} дн.")
            lines.append(f"  ⚠️ {eq.name} — {when}")

    if in_repair:
        lines.append("🛠 <b>В ремонте:</b>")
        for eq in in_repair:
            lines.append(f"  🔴 {eq.name} ({eq.model})")

    if not equip_soon and not in_repair:
        lines.append("🔧 <b>Техника:</b> всё в порядке ✅")

    lines += ["", "━━━━━━━━━━━━━━━━━━━━", "🌿 Хорошего рабочего дня!"]
    return "\n".join(lines)


def send_morning_report():
    print(f"[Scheduler] Запуск утреннего отчёта ({datetime.now().strftime('%H:%M')})")
    try:
        report = build_morning_report()
        tg_send(report)
    except Exception as e:
        print(f"[Scheduler] ❌ Ошибка при формировании отчёта: {e}")


# ════════════════════════════════════════
#  LIFESPAN
# ════════════════════════════════════════

@asynccontextmanager
async def lifespan(app):
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
        scheduler.add_job(
            send_morning_report,
            CronTrigger(hour=NOTIFY_HOUR, minute=NOTIFY_MINUTE),
            id="morning_report",
            replace_existing=True,
        )
        scheduler.start()
        print(f"[Scheduler] ✅ Запущен — отчёт каждый день в {NOTIFY_HOUR:02d}:{NOTIFY_MINUTE:02d} МСК")
    except ImportError:
        print("[Scheduler] ⚠️  apscheduler не установлен. Запустите: pip install apscheduler")

    yield

    print("[Scheduler] Остановка...")

app = FastAPI(title="Grass CRM API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Убираем жёсткий CSP, который Apache/Passenger могут добавлять
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

class PermissiveCSPMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src * 'unsafe-inline' 'unsafe-eval' data: blob:;"
        )
        return response

app.add_middleware(PermissiveCSPMiddleware)


# Отдаём crm.html по корневому пути — чтобы Apache не перехватывал
@app.get("/", include_in_schema=False)
@app.get("/crm.html", include_in_schema=False)
async def serve_crm():
    import os
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'crm.html')
    if not os.path.exists(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'crm.html')
    return FileResponse(path, media_type='text/html')


def get_db():
    db = SessionFactory()
    try:
        yield db
    finally:
        db.close()


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


@app.get("/api/users")
def list_users(db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    users = db.query(UserModel).filter(UserModel.is_active == True).all()
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


# ── Pydantic schemas ──────────────────────────

class DealServiceIn(BaseModel):
    service_id: int
    quantity: float

class DealServiceItem(BaseModel):
    name: str
    quantity: float
    price: float = 0
    unit: str = "ед"
    category: str = ""

class DealCreate(BaseModel):
    title: str
    client: str
    manager: str = ""
    address: str = ""
    notes: str = ""
    services: str = ""        # строка вида "Покос травы:5,Уборка листьев:3"
    service_items: List[DealServiceItem] = []  # структурированные услуги с фронтенда
    total: float = 0          # итоговая сумма с фронтенда
    vat_rate: str = "no_vat"  # НДС

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
    equipment_id: Optional[int] = None
    consumables: List[dict] = []


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

def _parse_services_string(raw: str) -> List[DealServiceItem]:
    items: List[DealServiceItem] = []
    if not raw:
        return items
    for part in raw.split(","):
        part = (part or "").strip()
        if not part:
            continue
        name, qty = (part.split(":", 1) + [""])[:2]
        name = (name or "").strip()
        if not name:
            continue
        try:
            q = float(qty)
        except Exception:
            q = 0.0
        if q <= 0:
            continue
        items.append(DealServiceItem(name=name, quantity=q, price=0, unit="ед"))
    return items


def _apply_deal_services(db: DBSession, deal: Deal, body: DealCreate) -> None:
    # Приоритет: service_items (структура). Фолбэк: services (строка).
    items = body.service_items or []
    if not items and body.services:
        items = _parse_services_string(body.services)

    # Полная замена набора услуг в сделке.
    deal.deal_services.clear()

    for item in items:
        if not item or not item.name:
            continue
        qty = float(item.quantity or 0)
        if qty <= 0:
            continue

        name = item.name.strip()
        service = db.query(Service).filter(Service.name == name).first()
        if not service:
            service = Service(
                name=name,
                unit=(item.unit or "ед"),
                price=float(item.price or 0),
                min_volume=1.0,
            )
            db.add(service)
            db.flush()

        price_at_moment = float(item.price or 0) if float(item.price or 0) > 0 else float(service.price or 0)
        ds = DealService(
            deal_id=deal.id,
            service_id=service.id,
            quantity=qty,
            price_at_moment=price_at_moment,
        )
        deal.deal_services.append(ds)


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
        # Если сумма из deal_services равна 0, берём сохранённую total
        if total == 0 and hasattr(d, 'total') and d.total:
            total = d.total
        result.append({
            "id": d.id, "title": d.title, "client": d.client,
            "stage": d.stage.name if d.stage else None,
            "stage_color": d.stage.color if d.stage else "#6B7280",
            "manager": d.manager, "address": d.address,
            "notes": d.notes if hasattr(d, 'notes') else "",
            "vat_rate": d.vat_rate if hasattr(d, 'vat_rate') else "no_vat",
            "total": total,
            "services": [{"name": ds.service.name if ds.service else "?",
                          "qty": ds.quantity, "price": ds.price_at_moment}
                         for ds in d.deal_services],
            "created_at": d.created_at.isoformat()
        })
    return {"deals": result, "count": len(result), "total_sum": sum(d["total"] for d in result)}


@app.post("/api/deals", status_code=201)
def create_deal(body: DealCreate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    stage = db.query(Stage).filter_by(name="Начальная").first()
    if not stage:
        # Берём первый этап если "Начальная" не найдена
        stage = db.query(Stage).order_by(Stage.order).first()

    deal = Deal(
        title=body.title,
        client=body.client,
        manager=body.manager,
        address=body.address,
        notes=body.notes,
        stage_id=stage.id if stage else None,
    )

    # Сохраняем vat_rate и total если модель поддерживает
    if hasattr(deal, 'vat_rate'):
        deal.vat_rate = body.vat_rate
    if hasattr(deal, 'total'):
        deal.total = body.total

    db.add(deal)
    db.commit()
    db.refresh(deal)

    _apply_deal_services(db, deal, body)
    db.commit()
    return {"id": deal.id, "title": deal.title, "client": deal.client}


@app.put("/api/deals/{deal_id}")
def update_deal(deal_id: int, body: DealCreate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    deal = db.query(Deal).get(deal_id)
    if not deal:
        raise HTTPException(404, "Сделка не найдена")

    deal.title   = body.title
    deal.client  = body.client
    deal.manager = body.manager
    deal.address = body.address
    deal.notes   = body.notes
    deal.updated_at = datetime.utcnow()

    if hasattr(deal, 'vat_rate'):
        deal.vat_rate = body.vat_rate
    if hasattr(deal, 'total'):
        deal.total = body.total

    _apply_deal_services(db, deal, body)

    db.commit()
    return {"id": deal.id, "title": deal.title, "client": deal.client}


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
    if total == 0 and hasattr(deal, 'total') and deal.total:
        total = deal.total
    return {
        "id": deal.id, "title": deal.title, "client": deal.client,
        "stage": deal.stage.name if deal.stage else None,
        "manager": deal.manager, "address": deal.address,
        "notes": deal.notes if hasattr(deal, 'notes') else "",
        "vat_rate": deal.vat_rate if hasattr(deal, 'vat_rate') else "no_vat",
        "total": total,
        "services": [{"id": ds.id, "service_id": ds.service_id,
                      "name": ds.service.name if ds.service else "?",
                      "quantity": ds.quantity, "price": ds.price_at_moment,
                      "subtotal": ds.quantity * ds.price_at_moment}
                     for ds in deal.deal_services],
        "created_at": deal.created_at.isoformat()
    }


@app.delete("/api/deals/{deal_id}", dependencies=[Depends(require_admin)])
def delete_deal(deal_id: int, db: DBSession = Depends(get_db)):
    deal = db.query(Deal).get(deal_id)
    if not deal:
        raise HTTPException(404, "Сделка не найдена")
    # Удаляем связанные услуги
    for ds in deal.deal_services:
        db.delete(ds)
    db.delete(deal)
    db.commit()
    return {"detail": "Сделка удалена"}


# ── EQUIPMENT ─────────────────────────────────

@app.get("/api/equipment")
def get_equipment(status: Optional[str] = Query(None), db: DBSession = Depends(get_db)):
    q = db.query(Equipment)
    if status:
        q = q.filter(Equipment.status == status)
    eqs = q.order_by(Equipment.name).all()
    return [{"id": e.id, "name": e.name, "model": e.model, "status": e.status,
             "purchase_cost": e.purchase_cost,
             "engine_hours": getattr(e, 'engine_hours', 0) or 0,
             "purchase_date": str(e.purchase_date) if getattr(e, 'purchase_date', None) else None,
             "last_maintenance": str(e.last_maintenance) if e.last_maintenance else None,
             "next_maintenance": str(e.next_maintenance) if e.next_maintenance else None,
             "notes": e.notes} for e in eqs]


@app.post("/api/equipment", status_code=201)
def create_equipment(body: EquipmentCreate, db: DBSession = Depends(get_db)):
    eq = Equipment(
        name=body.name, model=body.model,
        serial=body.serial if hasattr(Equipment, 'serial') else None,
        purchase_cost=body.purchase_cost, status=body.status, notes=body.notes
    )
    if hasattr(eq, 'engine_hours'):
        eq.engine_hours = body.engine_hours
    if body.purchase_date:
        try:
            eq.purchase_date = datetime.strptime(body.purchase_date, "%Y-%m-%d").date()
        except ValueError:
            pass
    db.add(eq)
    db.commit()
    return {"id": eq.id, "name": eq.name}


@app.put("/api/equipment/{equipment_id}")
def update_equipment(equipment_id: int, body: EquipmentCreate, db: DBSession = Depends(get_db)):
    eq = db.query(Equipment).get(equipment_id)
    if not eq:
        raise HTTPException(404, "Техника не найдена")
    eq.name   = body.name
    eq.model  = body.model
    eq.status = body.status
    eq.purchase_cost = body.purchase_cost
    eq.notes  = body.notes
    if hasattr(eq, 'engine_hours'):
        eq.engine_hours = body.engine_hours
    if body.purchase_date:
        try:
            eq.purchase_date = datetime.strptime(body.purchase_date, "%Y-%m-%d").date()
        except ValueError:
            pass
    db.commit()
    return {"id": eq.id, "name": eq.name, "status": eq.status}


@app.post("/api/equipment/{equipment_id}/maintenance", status_code=201)
def add_maintenance(equipment_id: int, body: MaintenanceCreate, db: DBSession = Depends(get_db)):
    eq = db.query(Equipment).get(equipment_id)
    if not eq:
        raise HTTPException(404, "Техника не найдена")
    maint_date = datetime.strptime(body.date, "%Y-%m-%d").date()
    cost = sum(i.get('quantity', 0) * i.get('unit_cost', 0) for i in body.consumables) if body.consumables else body.cost
    m = Maintenance(
        equipment_id=equipment_id,
        date=maint_date,
        description=body.description,
        cost=cost,
        performed_by=body.performed_by
    )
    db.add(m)
    today = date.today()
    if maint_date <= today:
        eq.last_maintenance = maint_date
    else:
        eq.next_maintenance = maint_date
    db.commit()
    return {"id": m.id, "equipment": eq.name, "date": str(maint_date)}


@app.put("/api/maintenances/{maintenance_id}")
def update_maintenance(maintenance_id: int, body: MaintenanceCreate, db: DBSession = Depends(get_db)):
    m = db.query(Maintenance).get(maintenance_id)
    if not m:
        raise HTTPException(404, "ТО не найдено")
    maint_date = datetime.strptime(body.date, "%Y-%m-%d").date()
    cost = sum(i.get('quantity', 0) * i.get('unit_cost', 0) for i in body.consumables) if body.consumables else body.cost
    m.date         = maint_date
    m.description  = body.description
    m.cost         = cost
    m.performed_by = body.performed_by
    if body.equipment_id:
        m.equipment_id = body.equipment_id
        eq = db.query(Equipment).get(body.equipment_id)
        if eq:
            today = date.today()
            if maint_date <= today:
                eq.last_maintenance = maint_date
            else:
                eq.next_maintenance = maint_date
    db.commit()
    return {"id": m.id, "date": str(maint_date)}


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


# ── MAINTENANCES ──────────────────────────────

@app.get("/api/maintenances")
def get_maintenances(db: DBSession = Depends(get_db)):
    rows = db.query(Maintenance).order_by(Maintenance.date.desc()).all()
    return [{
        "id": m.id,
        "equipment": m.equipment.name if m.equipment else "—",
        "equipment_id": m.equipment_id,
        "description": m.description,
        "cost": m.cost,
        "performed_by": getattr(m, 'performed_by', ''),
        "date": str(m.date),
    } for m in rows]


# ── CONSUMABLES (заглушка) ────────────────────

@app.get("/api/consumables")
def get_consumables(db: DBSession = Depends(get_db)):
    """Возвращает пустой список если таблица расходников не реализована."""
    try:
        from models.database import Consumable
        rows = db.query(Consumable).order_by(Consumable.name).all()
        return [{
            "id": c.id, "name": c.name,
            "stock_quantity": getattr(c, 'stock_quantity', 0),
            "unit": getattr(c, 'unit', 'шт'),
            "price_per_unit": getattr(c, 'price_per_unit', 0),
            "stock_value": getattr(c, 'stock_value', 0),
            "notes": getattr(c, 'notes', ''),
        } for c in rows]
    except Exception:
        return []


@app.post("/api/consumables", status_code=201)
def create_consumable(body: dict, db: DBSession = Depends(get_db)):
    try:
        from models.database import Consumable
        c = Consumable(**body)
        db.add(c)
        db.commit()
        return {"id": c.id, "name": c.name}
    except Exception:
        raise HTTPException(501, "Модель расходников не реализована")


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


@app.post("/api/expenses", status_code=201)
def create_expense(body: ExpenseCreate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    try:
        parsed_date = datetime.strptime(body.date, "%Y-%m-%d").date()
    except ValueError:
        try:
            parsed_date = datetime.strptime(body.date, "%d.%m.%Y").date()
        except ValueError:
            raise HTTPException(400, "Неверный формат даты")

    cat = db.query(ExpenseCategory).filter(ExpenseCategory.name.ilike(f"%{body.category}%")).first()
    exp = Expense(
        date=parsed_date, name=body.name,
        category_id=cat.id if cat else None,
        amount=body.amount, year=parsed_date.year,
        equipment_id=body.equipment_id, notes=body.notes
    )
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
    return {"detail": "Расход удалён"}


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
    import csv

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    if report_type == "deals":
        writer.writerow(["ID", "Название", "Клиент", "Этап", "Менеджер", "Сумма", "Дата создания"])
        deals = db.query(Deal).filter(extract("year", Deal.created_at) == year).all()
        for d in deals:
            total = sum(ds.quantity * ds.price_at_moment for ds in d.deal_services)
            writer.writerow([d.id, d.title, d.client, d.stage.name if d.stage else "",
                             d.manager, total, d.created_at.strftime("%d.%m.%Y")])

    elif report_type == "expenses":
        writer.writerow(["ID", "Дата", "Наименование", "Категория", "Сумма", "Техника"])
        exps = db.query(Expense).filter(Expense.year == year).order_by(Expense.date).all()
        for e in exps:
            writer.writerow([e.id, str(e.date), e.name,
                             e.category.name if e.category else "", e.amount,
                             e.equipment.name if e.equipment else ""])

    elif report_type == "profit-loss":
        success_ids = [s.id for s in db.query(Stage).filter_by(type="success").all()]
        deals = db.query(Deal).filter(
            extract("year", Deal.created_at) == year,
            Deal.stage_id.in_(success_ids)
        ).all() if success_ids else []
        revenue = sum(sum(ds.quantity * ds.price_at_moment for ds in d.deal_services) for d in deals)
        exp_rows = db.query(
            ExpenseCategory.name, func.sum(Expense.amount).label("total")
        ).join(Expense).filter(Expense.year == year).group_by(ExpenseCategory.name).all()
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


# ── TELEGRAM ──────────────────────────────────

@app.post("/api/telegram/test", dependencies=[Depends(require_admin)])
def telegram_test():
    report = build_morning_report()
    ok = tg_send(report)
    return {"sent": ok, "preview": report[:300] + "..."}


# ── WSGI для Passenger (shared хостинг) ──────────────────────
from a2wsgi import ASGIMiddleware
application = ASGIMiddleware(app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
