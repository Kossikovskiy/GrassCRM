
from dotenv import load_dotenv
load_dotenv()

import os
import secrets
import threading
import time as _time
import shutil
from pathlib import Path
from datetime import datetime, date, timedelta
from contextlib import asynccontextmanager
from typing import Optional, List
from functools import lru_cache

from fastapi import FastAPI, Depends, HTTPException, status, Request, Header, UploadFile, File as FastAPIFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, FileResponse
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, DateTime, 
    Boolean, ForeignKey, Text, text, MetaData, extract, Double, func
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session as DBSession, joinedload
from pydantic import BaseModel, Field, ConfigDict
import httpx
from jose import jwt, JWTError

# ── 1. КОНФИГ ─────────────────────────────────────────────────────────────────
DATABASE_URL   = os.getenv("DATABASE_URL")
AUTH0_DOMAIN   = os.getenv("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")
CLIENT_ID      = os.getenv("AUTH0_CLIENT_ID")
CLIENT_SECRET  = os.getenv("AUTH0_CLIENT_SECRET")
APP_BASE_URL   = os.getenv("APP_BASE_URL", "https://crmpokos.ru").rstrip("/")
CALLBACK_URL   = f"{APP_BASE_URL}/api/auth/callback"
SESSION_SECRET = os.getenv("SESSION_SECRET", secrets.token_hex(32))
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")
ROLE_CLAIM     = "https://grass-crm/role"
CACHE_TTL      = 300
TAX_RATE       = float(os.getenv("TAX_RATE", "0.04")) # 4% УСН "Доходы" для самозанятых
UPLOAD_DIR     = Path(os.getenv("UPLOAD_DIR", "/var/www/crm/GCRM-2/uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_FILE_SIZE  = 20 * 1024 * 1024  # 20 MB

# ── 2. КЭШ И УТИЛИТЫ ──────────────────────────────────────────────────────────
class _Cache:
    def __init__(self, ttl: int):
        self._ttl, self._data, self._ts, self._lock = ttl, {}, {}, threading.Lock()
    def get(self, key: str):
        if key not in self._data or _time.monotonic() - self._ts[key] > self._ttl: return None
        return self._data[key]
    def set(self, key: str, value):
        with self._lock: self._data[key], self._ts[key] = value, _time.monotonic()
    def invalidate(self, *keys):
        with self._lock:
            if not keys or "all" in keys: self._data.clear(); self._ts.clear(); return
            for k in keys:
                to_remove = [ek for ek in self._data if ek == k or ek.startswith(f"{k}:")]
                for ek in to_remove: 
                    if ek in self._data: del self._data[ek]
                    if ek in self._ts: del self._ts[ek]
_cache = _Cache(ttl=CACHE_TTL)

# ── 3. БАЗА ДАННЫХ ────────────────────────────────────────────────────────────
Base = declarative_base()
engine = create_engine(DATABASE_URL, client_encoding='utf8')
SessionFactory = sessionmaker(bind=engine, autoflush=False)

class User(Base): __tablename__ = "users"; id,username,name,email = Column(String, primary_key=True),Column(String),Column(String),Column(String); role=Column(String, default="User")
class Service(Base): __tablename__ = "services"; id,name,price,unit = Column(Integer,primary_key=True),Column(String(200),nullable=False),Column(Float,default=0.0),Column(String(50),default="шт"); min_volume=Column(Float,default=1.0); notes=Column(Text)
class DealService(Base): __tablename__ = "deal_services"; id,deal_id,service_id,quantity,price_at_moment = Column(Integer,primary_key=True),Column(Integer,ForeignKey("deals.id",ondelete="CASCADE")),Column(Integer,ForeignKey("services.id",ondelete="RESTRICT")),Column(Float,default=1.0),Column(Float,nullable=False); service = relationship("Service")
class Stage(Base): __tablename__ = "stages"; id,name,order,type,is_final,color = Column(Integer,primary_key=True),Column(String(100),nullable=False,unique=True),Column(Integer,default=0),Column(String(50),default="regular"),Column(Boolean,default=False),Column(String(20),default="#6B7280"); deals = relationship("Deal", back_populates="stage")
class Contact(Base): __tablename__ = "contacts"; id,name,phone,source=Column(Integer,primary_key=True),Column(String(200),nullable=False),Column(String(50),unique=True,index=True),Column(String(100)); deals = relationship("Deal",back_populates="contact")
class Deal(Base): 
    __tablename__ = "deals"
    id=Column(Integer,primary_key=True)
    contact_id=Column(Integer,ForeignKey("contacts.id"),nullable=False)
    stage_id=Column(Integer,ForeignKey("stages.id"))
    title=Column(String(200),nullable=False)
    total=Column(Float,default=0.0)
    notes=Column(Text,default="")
    created_at=Column(DateTime,default=datetime.utcnow)
    deal_date=Column(DateTime)
    closed_at=Column(DateTime)
    is_repeat=Column(Boolean,default=False)
    manager=Column(String(200))
    address=Column(Text)
    tax_rate = Column(Float, default=4.0, nullable=False)
    tax_included = Column(Boolean, default=True, nullable=False)
    discount = Column(Float, default=0.0, nullable=False)

    contact=relationship("Contact",back_populates="deals")
    stage=relationship("Stage",back_populates="deals")
    services=relationship("DealService",cascade="all, delete-orphan",passive_deletes=True)

class Task(Base): __tablename__="tasks"; id,title,description,is_done=Column(Integer,primary_key=True),Column(String,nullable=False),Column(Text),Column(Boolean,default=False); due_date,assignee,priority,status=Column(Date),Column(String),Column(String,default="Обычный"),Column(String,default="Открыта"); contact_id=Column(Integer,ForeignKey("contacts.id",ondelete="SET NULL"),nullable=True); deal_id=Column(Integer,ForeignKey("deals.id",ondelete="SET NULL"),nullable=True); contact=relationship("Contact"); deal=relationship("Deal")

class Interaction(Base):
    __tablename__ = "interactions"
    id = Column(Integer, primary_key=True)
    contact_id = Column(Integer, ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False)
    type = Column(String(50), nullable=False, default="note")  # call, email, meeting, note
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    user_id = Column(String)
    user_name = Column(String)
    contact = relationship("Contact")

class DealComment(Base):
    __tablename__ = "deal_comments"
    id = Column(Integer, primary_key=True)
    deal_id = Column(Integer, ForeignKey("deals.id", ondelete="CASCADE"), nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    user_id = Column(String)
    user_name = Column(String)
    deal = relationship("Deal")

class CRMFile(Base):
    __tablename__ = "crm_files"
    id = Column(Integer, primary_key=True)
    filename = Column(String(300), nullable=False)       # оригинальное имя файла
    stored_name = Column(String(300), nullable=False)    # имя на диске (уникальное)
    size = Column(Integer, default=0)                    # байты
    mime_type = Column(String(100))
    contact_id = Column(Integer, ForeignKey("contacts.id", ondelete="SET NULL"), nullable=True)
    deal_id = Column(Integer, ForeignKey("deals.id", ondelete="SET NULL"), nullable=True)
    uploaded_by = Column(String)
    uploaded_by_name = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    contact = relationship("Contact")
    deal = relationship("Deal")

class ExpenseCategory(Base): __tablename__="expense_categories"; id,name=Column(Integer,primary_key=True),Column(String(100),nullable=False,unique=True); expenses=relationship("Expense",back_populates="category")
class Consumable(Base): __tablename__="consumables"; id,name,unit=Column(Integer,primary_key=True),Column(String(200),nullable=False,unique=True),Column(String(50),default="шт"); stock_quantity,notes,price=Column(Float,default=0.0),Column(Text),Column(Float,default=0.0)
class MaintenanceConsumable(Base): __tablename__="maintenance_consumables"; id=Column(Integer,primary_key=True); maintenance_id=Column(Integer,ForeignKey("equipment_maintenance.id",ondelete="CASCADE"),nullable=False); consumable_id=Column(Integer,ForeignKey("consumables.id",ondelete="RESTRICT"),nullable=False); quantity=Column(Float,nullable=False); price_at_moment=Column(Float,nullable=False); consumable=relationship("Consumable"); maintenance_record=relationship("EquipmentMaintenance",back_populates="consumables_used")
class EquipmentMaintenance(Base): __tablename__ = "equipment_maintenance"; id=Column(Integer,primary_key=True); equipment_id=Column(Integer,ForeignKey("equipment.id",ondelete="CASCADE"),nullable=False); date=Column(Date,nullable=False); work_description=Column(Text,nullable=False); cost=Column(Float); notes=Column(Text); equipment = relationship("Equipment", back_populates="maintenance_records"); consumables_used = relationship("MaintenanceConsumable", back_populates="maintenance_record", cascade="all, delete-orphan")
class Equipment(Base): __tablename__="equipment"; id,name,model,serial=Column(Integer,primary_key=True),Column(String(200),nullable=False),Column(String(200),default=""),Column(String(100)); purchase_date,purchase_cost=Column(Date),Column(Double,default=0.0); status,notes=Column(String(50),default="active"),Column(Text); engine_hours=Column(Double,default=0.0); fuel_norm=Column(Double,default=0.0); last_maintenance_date=Column(Date); next_maintenance_date=Column(Date); expenses=relationship("Expense",back_populates="equipment"); maintenance_records = relationship("EquipmentMaintenance", back_populates="equipment", cascade="all, delete-orphan")
class Expense(Base): __tablename__="expenses"; id,date,name,amount=Column(Integer,primary_key=True),Column(Date,nullable=False,default=date.today),Column(String(300),nullable=False),Column(Float,nullable=False); category_id,equipment_id=Column(Integer,ForeignKey("expense_categories.id")),Column(Integer,ForeignKey("equipment.id")); category=relationship("ExpenseCategory",back_populates="expenses"); equipment=relationship("Equipment",back_populates="expenses")

class TaxPayment(Base):
    __tablename__ = "tax_payments"
    id = Column(Integer, primary_key=True)
    amount = Column(Float, nullable=False)
    date = Column(Date, nullable=False, default=date.today)
    note = Column(Text)
    year = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class DailyPhrase(Base):
    __tablename__ = "daily_phrases"
    id = Column(Integer, primary_key=True)
    phrase = Column(Text, nullable=False)
    category = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

def init_db_structure(): Base.metadata.create_all(engine)
def seed_initial_data(s: DBSession):
    if s.query(Stage).count()==0: s.add_all([Stage(**d) for d in [{"name":"Согласовать","order":1,"color":"#3B82F6"},{"name":"Ожидание","order":2,"color":"#F59E0B"},{"name":"В работе","order":3,"color":"#EC4899"},{"name":"Успешно","order":4,"color":"#10B981","is_final":True},{"name":"Провалена","order":5,"color":"#EF4444","is_final":True}]])
    if s.query(ExpenseCategory).count()==0: s.add_all([ExpenseCategory(name=n) for n in ["Техника","Топливо","Расходники","Реклама","Запчасти","Прочее"]])
    s.commit()

# ── 4. HELPERS ────────────────────────────────────────────────────────────────
def update_equipment_last_maintenance(db: DBSession, equipment_id: int):
    equipment_item = db.query(Equipment).filter(Equipment.id == equipment_id).first();
    if not equipment_item: return
    latest_maintenance_date = db.query(func.max(EquipmentMaintenance.date)).filter(EquipmentMaintenance.equipment_id == equipment_id).scalar()
    equipment_item.last_maintenance_date = latest_maintenance_date; db.commit()
    _cache.invalidate("equipment")

# ── 5. PYDANTIC MODELS ───────────────────────────────────────────────────────
class DealServiceItem(BaseModel): service_id: int; quantity: float
class DealCreate(BaseModel): 
    title:str; 
    stage_id:int; 
    contact_id:Optional[int]=None; 
    new_contact_name:Optional[str]=None; 
    manager:Optional[str]=None; 
    services:List[DealServiceItem]=[]
    tax_rate: Optional[float] = 4.0
    tax_included: Optional[bool] = True
    discount: Optional[float] = 0.0
    work_date: Optional[str] = None
    work_time: Optional[str] = None
    address: Optional[str] = None

class DealUpdate(BaseModel): 
    title:Optional[str]=None; 
    stage_id:Optional[int]=None; 
    contact_id:Optional[int]=None; 
    new_contact_name:Optional[str]=None; 
    manager:Optional[str]=None; 
    services:Optional[List[DealServiceItem]]=None
    tax_rate: Optional[float] = None
    tax_included: Optional[bool] = None
    discount: Optional[float] = None
    work_date: Optional[str] = None
    work_time: Optional[str] = None
    address: Optional[str] = None

class TaskCreate(BaseModel): title: str; description: Optional[str]=None; due_date: Optional[date]=None; priority: Optional[str]="Обычный"; status: Optional[str]="Открыта"; assignee: Optional[str]=None; contact_id: Optional[int]=None; deal_id: Optional[int]=None
class TaskUpdate(BaseModel): title: Optional[str]=None; description: Optional[str]=None; due_date: Optional[date]=None; priority: Optional[str]=None; status: Optional[str]=None; assignee: Optional[str]=None; is_done: Optional[bool]=None; contact_id: Optional[int]=None; deal_id: Optional[int]=None
class ContactCreate(BaseModel): name: str = Field(..., min_length=1); phone: Optional[str] = None; source: Optional[str] = None
class ContactUpdate(BaseModel): name: Optional[str] = Field(None, min_length=1); phone: Optional[str] = None; source: Optional[str] = None
class ServiceCreate(BaseModel): name: str = Field(...,min_length=1); price: float; unit: str; min_volume: Optional[float]=1.0; notes: Optional[str]=None
class ServiceUpdate(BaseModel): name: Optional[str]=Field(None,min_length=1); price: Optional[float]=None; unit: Optional[str]=None; min_volume: Optional[float]=None; notes: Optional[str]=None
class EquipmentCreate(BaseModel): name: str; model: Optional[str]=None; serial: Optional[str]=None; purchase_date: Optional[date]=None; purchase_cost: Optional[float]=None; status: Optional[str]='active'; notes: Optional[str]=None; engine_hours: Optional[float]=None; fuel_norm: Optional[float]=None; last_maintenance_date: Optional[date]=None; next_maintenance_date: Optional[date]=None
class EquipmentUpdate(BaseModel): name: Optional[str]=None; model: Optional[str]=None; serial: Optional[str]=None; purchase_date: Optional[date]=None; purchase_cost: Optional[float]=None; status: Optional[str]=None; notes: Optional[str]=None; engine_hours: Optional[float]=None; fuel_norm: Optional[float]=None; last_maintenance_date: Optional[date]=None; next_maintenance_date: Optional[date]=None
class ConsumableCreate(BaseModel): name: str; unit: Optional[str] = 'шт'; stock_quantity: Optional[float] = 0.0; price: Optional[float] = 0.0; notes: Optional[str] = None
class ConsumableUpdate(BaseModel): name: Optional[str] = None; unit: Optional[str] = None; stock_quantity: Optional[float] = None; price: Optional[float] = None; notes: Optional[str] = None
class MaintenanceConsumableItem(BaseModel): consumable_id: int; quantity: float
class MaintenanceCreate(BaseModel): equipment_id: int; date: date; work_description: str; notes: Optional[str]=None; consumables: List[MaintenanceConsumableItem] = []
class MaintenanceUpdate(BaseModel): date: Optional[date]=None; work_description: Optional[str]=None; notes: Optional[str]=None; consumables: Optional[List[MaintenanceConsumableItem]] = None
class ExpenseCreate(BaseModel): name: str; amount: float; date: date; category: str
class ExpenseUpdate(BaseModel): name: Optional[str] = None; amount: Optional[float] = None; date: Optional[date] = None; category: Optional[str] = None
class TaxPaymentCreate(BaseModel): amount: float; date: date; note: Optional[str] = None; year: int
class InteractionCreate(BaseModel): type: str = "note"; text: str = Field(..., min_length=1)
class DealCommentCreate(BaseModel): text: str = Field(..., min_length=1)


# --- Response Models to prevent serialization cycles ---
class EquipmentForMaintResponse(BaseModel):
    id: int; name: str
    model_config = ConfigDict(from_attributes=True)

class MaintenanceForListResponse(BaseModel):
    id: int; date: date; work_description: str; cost: Optional[float]; equipment_id: int
    equipment: EquipmentForMaintResponse
    model_config = ConfigDict(from_attributes=True)

class ConsumableForMaintResponse(BaseModel):
    id: int; name: str; unit: Optional[str]; stock_quantity: Optional[float] = 0.0
    model_config = ConfigDict(from_attributes=True)

class MaintConsumableForDetailResponse(BaseModel):
    quantity: float; price_at_moment: float
    consumable: ConsumableForMaintResponse
    model_config = ConfigDict(from_attributes=True)

class MaintenanceDetailResponse(BaseModel):
    id: int; equipment_id: int; date: date; work_description: str; notes: Optional[str]; cost: Optional[float]
    consumables_used: List[MaintConsumableForDetailResponse]
    model_config = ConfigDict(from_attributes=True)

class EquipmentResponse(BaseModel):
    id:int; name:str; model:Optional[str]; serial:Optional[str]; purchase_date:Optional[date]; purchase_cost:Optional[float]; status:Optional[str]; notes:Optional[str]; engine_hours:Optional[float]; fuel_norm:Optional[float]; last_maintenance_date:Optional[date]; next_maintenance_date:Optional[date]
    model_config = ConfigDict(from_attributes=True)

class BudgetResponse(BaseModel):
    id: int
    year: int
    period: str
    name: str
    planned_revenue: Optional[float] = 0.0
    planned_expenses: Optional[float] = 0.0
    notes: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)

# ── 6. FASTAPI APP ────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("App starting (v12.1)...",flush=True)
    init_db_structure()
    with SessionFactory() as db: seed_initial_data(db)
    _ensure_budget_table()
    yield
    print("App shutting down.",flush=True)

app = FastAPI(title="GrassCRM API", version="12.2.0", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, https_only=True, same_site="lax")
app.add_middleware(CORSMiddleware, allow_origins=[APP_BASE_URL], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@lru_cache(maxsize=1)
def get_jwks(): return httpx.get(f"https://{AUTH0_DOMAIN}/.well-known/jwks.json", timeout=10).raise_for_status().json()
def get_db(): db = SessionFactory(); yield db; db.close()

def get_current_user(req: Request, x_internal_api_key: Optional[str] = Header(None, alias="X-Internal-API-Key")):
    # API Key authentication for internal services (like the Telegram bot)
    if INTERNAL_API_KEY and x_internal_api_key == INTERNAL_API_KEY:
        # This is a trusted internal service. We grant it a synthetic admin user profile.
        return {"sub": "internal-bot", "name": "Telegram Bot", "role": "Admin"}

    # Standard session-based authentication for web users
    if user := req.session.get("user"):
        return user

    # If neither authentication method is successful, deny access.
    raise HTTPException(status_code=401, detail="Not authenticated")

def is_admin(user: dict) -> bool:
    return (user.get("role") or "").strip().lower() == "admin"

def require_admin(user: dict = Depends(get_current_user)):
    if not is_admin(user):
        raise HTTPException(403, "Доступ запрещён: требуется роль Admin")
    return user

# ── 7. ЭНДПОИНТЫ AUTH ─────────────────────────────────────────────────────────
@app.get("/api/auth/login")
def login(req:Request): state=secrets.token_urlsafe(16); req.session["oauth_state"]=state; return RedirectResponse(f"https://{AUTH0_DOMAIN}/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri={CALLBACK_URL}&scope=openid%20profile%20email&audience={AUTH0_AUDIENCE}&state={state}")
@app.get("/api/auth/callback")
def callback(req:Request, code:str=None, state:str=None, error:str=None):
    if error: return RedirectResponse(f"/?auth_error={error}")
    if not code or state!=req.session.pop("oauth_state",None): raise HTTPException(400,"Invalid state or no code")
    with httpx.Client() as c:
        tokens=c.post(f"https://{AUTH0_DOMAIN}/oauth/token",json={"grant_type":"authorization_code","client_id":CLIENT_ID,"client_secret":CLIENT_SECRET,"code":code,"redirect_uri":CALLBACK_URL}).raise_for_status().json()
        profile=c.get(f"https://{AUTH0_DOMAIN}/userinfo",headers={"Authorization":f"Bearer {tokens['access_token']}"}).raise_for_status().json()
    user_id=profile.get("sub"); user_name=profile.get("name") or profile.get("nickname") or user_id
    with SessionFactory() as db:
        user=db.query(User).filter(User.id==user_id).first()
        if not user: db.add(User(id=user_id,name=user_name,email=profile.get("email")))
        else: user.name,user.email = user_name,profile.get("email")
        db.commit()
    # Роль берём из БД (задаётся вручную в Supabase: Admin / User)
    with SessionFactory() as _db:
        _u = _db.query(User).filter(User.id == user_id).first()
        user_role = (_u.role or "User") if _u else "User"
    req.session["user"] = {"sub": user_id, "name": user_name, "role": user_role}
    return RedirectResponse("/")
@app.get("/api/auth/logout")
def logout(req:Request): req.session.clear(); return RedirectResponse(f"https://{AUTH0_DOMAIN}/v2/logout?client_id={CLIENT_ID}&returnTo={APP_BASE_URL}")

# ── 8. ЭНДПОИНТЫ API ──────────────────────────────────────────────────────────
@app.get("/api/me")
def get_me(user:dict=Depends(get_current_user)): return user
@app.get("/api/users")
def get_users(db:DBSession=Depends(get_db),_=Depends(get_current_user)): return db.query(User).all()
@app.get("/api/stages")
def get_stages(db:DBSession=Depends(get_db),_=Depends(get_current_user)): return db.query(Stage).order_by(Stage.order).all()
@app.post("/api/cache/invalidate")
def invalidate_cache(_=Depends(get_current_user)): _cache.invalidate("all"); return {"status":"ok"}

# --- SERVICES ---
@app.get("/api/services")
def get_services(db:DBSession=Depends(get_db),_=Depends(get_current_user)): return db.query(Service).order_by(Service.id).all()
@app.post("/api/services", status_code=201)
def create_service(data: ServiceCreate, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    new_item = Service(**data.model_dump()); db.add(new_item); db.commit(); db.refresh(new_item)
    _cache.invalidate("services"); return new_item
@app.patch("/api/services/{service_id}")
def update_service(service_id: int, data: ServiceUpdate, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    item = db.query(Service).filter(Service.id == service_id).first()
    if not item: raise HTTPException(404, "Услуга не найдена")
    for key, value in data.model_dump(exclude_unset=True).items(): setattr(item, key, value)
    db.commit(); db.refresh(item)
    _cache.invalidate("services", "deals"); return item
@app.delete("/api/services/{service_id}", status_code=204)
def delete_service(service_id: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    if db.query(DealService).filter(DealService.service_id == service_id).count() > 0:
        raise HTTPException(400, "Нельзя удалить услугу, которая используется в сделках.")
    item = db.query(Service).filter(Service.id == service_id).first()
    if item: db.delete(item); db.commit(); _cache.invalidate("services")
    return None

# --- DEALS ---
@app.get("/api/deals")
def get_deals(year:Optional[int]=None,db:DBSession=Depends(get_db),user:dict=Depends(get_current_user)):
    q=db.query(Deal).options(joinedload(Deal.contact),joinedload(Deal.stage)).order_by(Deal.created_at.desc())
    if year: q = q.filter(extract("year", Deal.deal_date) == year)
    if not is_admin(user): q = q.filter(Deal.manager == user["name"])  # менеджер видит только свои
    deals_list=[{"id":d.id,"title":d.title or "","total":d.total or 0.0,"client":d.contact.name if d.contact else "","contact_id":d.contact_id,"stage":d.stage.name if d.stage else "","stage_id":d.stage_id,"created_at":(d.created_at or datetime.utcnow()).isoformat()} for d in q.all()]
    return {"deals": deals_list}

@app.post("/api/deals", status_code=201)
def create_deal(deal_data: DealCreate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    contact_id = deal_data.contact_id
    if deal_data.new_contact_name:
        new_contact = Contact(name=deal_data.new_contact_name)
        db.add(new_contact)
        db.flush()
        db.refresh(new_contact)
        contact_id = new_contact.id
    if not contact_id:
        raise HTTPException(400, "Не указан клиент")

    subtotal = 0
    service_items_for_db = []
    for item in deal_data.services:
        service = db.query(Service).filter(Service.id == item.service_id).first()
        if not service: continue
        price = service.price or 0
        subtotal += price * item.quantity
        service_items_for_db.append(DealService(service_id=service.id, quantity=item.quantity, price_at_moment=price))

    discount_percent = deal_data.discount or 0
    tax_rate_percent = deal_data.tax_rate or 0
    
    discount_amount = subtotal * (discount_percent / 100.0)
    subtotal_after_discount = subtotal - discount_amount
    
    # Налог включён: сумма = subtotal, налог отображается отдельно но не прибавляется
    # Налог сверху: сумма = subtotal + налог
    tax_amount = subtotal_after_discount * (tax_rate_percent / 100.0)
    if deal_data.tax_included:
        final_total = subtotal_after_discount
    else:
        final_total = subtotal_after_discount + tax_amount

    new_deal = Deal(
        title=deal_data.title,
        stage_id=deal_data.stage_id,
        contact_id=contact_id,
        deal_date=datetime.utcnow(),
        manager=deal_data.manager,
        services=service_items_for_db,
        total=round(final_total, 2),
        discount=discount_percent,
        tax_rate=tax_rate_percent,
        tax_included=deal_data.tax_included,
        address=deal_data.address or None
    )
    # Собираем дату+время выезда из отдельных строк
    if deal_data.work_date:
        try:
            dt_str = deal_data.work_date
            if deal_data.work_time:
                dt_str += f"T{deal_data.work_time}"
            new_deal.deal_date = datetime.fromisoformat(dt_str)
        except Exception:
            pass
    db.add(new_deal)
    db.commit()
    _cache.invalidate("deals", "years")
    return {"status": "ok"}

@app.get("/api/deals/{deal_id}")
def get_deal_details(deal_id: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    deal = db.query(Deal).options(
        joinedload(Deal.contact), 
        joinedload(Deal.services).joinedload(DealService.service)
    ).filter(Deal.id == deal_id).first()

    if not deal:
        raise HTTPException(404, "Сделка не найдена")

    services_list = []
    for ds in deal.services:
        service_info = {"quantity": ds.quantity, "price_at_moment": ds.price_at_moment}
        if ds.service:
            service_info["service"] = {"id": ds.service.id, "name": ds.service.name, "price": ds.service.price, "unit": ds.service.unit}
        else:
            service_info["service"] = {"id": -1, "name": "[Удаленная услуга]", "price": ds.price_at_moment, "unit": "?"}
        services_list.append(service_info)
    
    return {
        "id": deal.id,
        "title": deal.title,
        "total": deal.total,
        "stage_id": deal.stage_id,
        "manager": deal.manager,
        "contact": {"id": deal.contact.id, "name": deal.contact.name} if deal.contact else None,
        "services": services_list,
        "discount": deal.discount,
        "tax_rate": deal.tax_rate,
        "tax_included": deal.tax_included,
        "deal_date": deal.deal_date.isoformat() if deal.deal_date else None,
        "address": deal.address or "",
    }

@app.patch("/api/deals/{deal_id}")
def update_deal(deal_id: int, deal_data: DealUpdate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    deal = db.query(Deal).filter(Deal.id == deal_id).first()
    if not deal: raise HTTPException(404, "Сделка не найдена")

    update_data = deal_data.model_dump(exclude_unset=True)

    if "new_contact_name" in update_data:
        new_contact = Contact(name=update_data["new_contact_name"])
        db.add(new_contact)
        db.flush()
        db.refresh(new_contact)
        deal.contact_id = new_contact.id
    elif "contact_id" in update_data:
        deal.contact_id = update_data["contact_id"]

    recalculate = "services" in update_data or "discount" in update_data or "tax_rate" in update_data or "tax_included" in update_data

    if "services" in update_data:
        db.query(DealService).filter(DealService.deal_id == deal_id).delete(synchronize_session=False)
        for item_data in update_data["services"]:
            item = DealServiceItem(**item_data)
            service = db.query(Service).filter(Service.id == item.service_id).first()
            if service:
                db.add(DealService(deal_id=deal_id, service_id=service.id, quantity=item.quantity, price_at_moment=(service.price or 0)))
        db.flush()
    
    if "discount" in update_data: deal.discount = update_data["discount"]
    if "tax_rate" in update_data: deal.tax_rate = update_data["tax_rate"]
    if "tax_included" in update_data: deal.tax_included = update_data["tax_included"]
    
    if recalculate:
        subtotal = sum(ds.price_at_moment * ds.quantity for ds in deal.services)
        discount_percent = deal.discount or 0
        tax_rate_percent = deal.tax_rate or 0
        discount_amount = subtotal * (discount_percent / 100.0)
        subtotal_after_discount = subtotal - discount_amount
        
        tax_amount = subtotal_after_discount * (tax_rate_percent / 100.0)
        if deal.tax_included:
            final_total = subtotal_after_discount
        else:
            final_total = subtotal_after_discount + tax_amount
        
        deal.total = round(final_total, 2)

    if "title" in update_data: deal.title = update_data["title"]
    if "stage_id" in update_data: deal.stage_id = update_data["stage_id"]
    if "manager" in update_data: deal.manager = update_data["manager"]
    if "address" in update_data: deal.address = update_data["address"]
    if "work_date" in update_data or "work_time" in update_data:
        work_date = update_data.get("work_date") or (deal.deal_date.date().isoformat() if deal.deal_date else None)
        work_time = update_data.get("work_time") or (deal.deal_date.strftime("%H:%M") if deal.deal_date else None)
        if work_date:
            try:
                dt_str = work_date
                if work_time:
                    dt_str += f"T{work_time}"
                deal.deal_date = datetime.fromisoformat(dt_str)
            except Exception:
                pass

    db.commit()
    _cache.invalidate("deals", "years")
    return {"status": "ok"}

@app.delete("/api/deals/{deal_id}", status_code=204)
def delete_deal(deal_id: int, db:DBSession=Depends(get_db),_=Depends(require_admin)):
    deal=db.query(Deal).filter(Deal.id==deal_id).first()
    if deal:
        db.query(DealService).filter(DealService.deal_id==deal_id).delete(synchronize_session=False)
        db.delete(deal); db.commit(); _cache.invalidate("deals","years")
    return None

# --- CONTACTS ---
@app.get("/api/contacts")
def get_contacts(db:DBSession=Depends(get_db),_=Depends(get_current_user)):
    return db.query(Contact).order_by(Contact.name).all()

@app.post("/api/contacts", status_code=201)
def create_contact(contact_data: ContactCreate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    if contact_data.phone and contact_data.phone.strip():
        existing = db.query(Contact).filter(Contact.phone == contact_data.phone).first()
        if existing: raise HTTPException(status_code=409, detail="Контакт с таким телефоном уже существует")
    new_contact = Contact(**contact_data.model_dump()); db.add(new_contact); db.commit(); db.refresh(new_contact)
    _cache.invalidate("contacts"); return new_contact

@app.patch("/api/contacts/{contact_id}")
def update_contact(contact_id: int, contact_data: ContactUpdate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact: raise HTTPException(status_code=404, detail="Контакт не найден")
    
    update_data = contact_data.model_dump(exclude_unset=True)
    if "phone" in update_data and update_data["phone"] and update_data["phone"].strip():
        existing = db.query(Contact).filter(Contact.phone == update_data["phone"], Contact.id != contact_id).first()
        if existing: raise HTTPException(status_code=409, detail="Контакт с таким телефоном уже существует")

    for key, value in update_data.items(): setattr(contact, key, value)
    
    db.commit(); db.refresh(contact)
    _cache.invalidate("contacts", "deals"); return contact


@app.get("/api/contacts/{contact_id}/deals")
def get_contact_all_deals(contact_id: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    """Все сделки контакта за всё время, от новых к старым"""
    deals = (db.query(Deal)
             .options(joinedload(Deal.stage))
             .filter(Deal.contact_id == contact_id)
             .order_by(Deal.created_at.desc())
             .all())
    won_names = {s.name for s in db.query(Stage).all() if s.is_final and "успешно" in (s.name or "").lower()}
    result = []
    for d in deals:
        result.append({
            "id": d.id,
            "title": d.title or "",
            "total": d.total or 0.0,
            "stage": d.stage.name if d.stage else "",
            "stage_color": d.stage.color if d.stage else "#6b7280",
            "stage_is_won": (d.stage.name in won_names) if d.stage else False,
            "stage_is_final": d.stage.is_final if d.stage else False,
            "deal_date": d.deal_date.isoformat() if d.deal_date else None,
            "created_at": (d.created_at or datetime.utcnow()).isoformat(),
        })
    total_revenue = sum(r["total"] for r in result if r["stage_is_won"])
    return {
        "deals": result,
        "total_count": len(result),
        "won_count": sum(1 for r in result if r["stage_is_won"]),
        "total_revenue": round(total_revenue, 2),
    }

@app.delete("/api/contacts/{contact_id}", status_code=204)
def delete_contact(contact_id: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact: return None
    if db.query(Deal).filter(Deal.contact_id == contact_id).count() > 0:
        raise HTTPException(status_code=400, detail="Нельзя удалить контакт, к которому привязаны сделки.")
    db.delete(contact); db.commit()
    _cache.invalidate("contacts"); return None

# --- EXPENSES ---
@app.get("/api/expense-categories")
def get_expense_categories(db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    return db.query(ExpenseCategory).order_by(ExpenseCategory.name).all()

@app.post("/api/expenses", status_code=201)
def create_expense(data: ExpenseCreate, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    category_name = data.category.strip()
    category = None
    if category_name:
        category = db.query(ExpenseCategory).filter(func.lower(ExpenseCategory.name) == func.lower(category_name)).first()
        if not category:
            category = ExpenseCategory(name=category_name)
            db.add(category)
            db.flush()
            _cache.invalidate("expense_categories")

    new_expense = Expense(name=data.name, amount=data.amount, date=data.date, category_id=category.id if category else None)
    db.add(new_expense); db.commit(); db.refresh(new_expense)
    _cache.invalidate("expenses", "years")
    return new_expense

@app.patch("/api/expenses/{expense_id}")
def update_expense(expense_id: int, data: ExpenseUpdate, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    expense = db.query(Expense).filter(Expense.id == expense_id).first()
    if not expense: raise HTTPException(404, "Расход не найден")
    
    update_data = data.model_dump(exclude_unset=True)
    if "category" in update_data:
        category_name = update_data.pop("category").strip()
        category = None
        if category_name:
            category = db.query(ExpenseCategory).filter(func.lower(ExpenseCategory.name) == func.lower(category_name)).first()
            if not category:
                category = ExpenseCategory(name=category_name)
                db.add(category); db.flush()
                _cache.invalidate("expense_categories")
        expense.category_id = category.id if category else None

    for key, value in update_data.items(): setattr(expense, key, value)
    
    db.commit(); db.refresh(expense)
    _cache.invalidate("expenses", "years")
    return expense

@app.delete("/api/expenses/{expense_id}", status_code=204)
def delete_expense(expense_id: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    expense = db.query(Expense).filter(Expense.id == expense_id).first()
    if expense: db.delete(expense); db.commit(); _cache.invalidate("expenses", "years")
    return None

@app.get("/api/expenses")
def get_expenses(year: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    q = db.query(Expense).options(joinedload(Expense.category)).filter(extract("year", Expense.date) == year).order_by(Expense.date.desc())
    return [{"id": e.id, "name": e.name, "amount": e.amount, "category": e.category.name if e.category else "", "date": e.date.isoformat() if e.date else None} for e in q.all()]


# --- EQUIPMENT & MAINTENANCE ---
@app.get("/api/equipment", response_model=List[EquipmentResponse])
def get_equipment(db: DBSession=Depends(get_db), _=Depends(get_current_user)):
    return db.query(Equipment).order_by(Equipment.name).all()

@app.post("/api/equipment", status_code=201, response_model=EquipmentResponse)
def create_equipment(data: EquipmentCreate, db:DBSession=Depends(get_db), _=Depends(require_admin)):
    new_item = Equipment(**data.model_dump()); db.add(new_item); db.commit(); db.refresh(new_item)
    _cache.invalidate("equipment"); return new_item

@app.patch("/api/equipment/{eq_id}", response_model=EquipmentResponse)
def update_equipment(eq_id: int, data: EquipmentUpdate, db:DBSession=Depends(get_db), _=Depends(require_admin)):
    item = db.query(Equipment).filter(Equipment.id == eq_id).first()
    if not item: raise HTTPException(404, "Техника не найдена")
    for key, value in data.model_dump(exclude_unset=True).items(): setattr(item, key, value)
    db.commit(); db.refresh(item)
    _cache.invalidate("equipment"); return item

@app.delete("/api/equipment/{eq_id}", status_code=204)
def delete_equipment(eq_id: int, db:DBSession=Depends(get_db), _=Depends(require_admin)):
    item = db.query(Equipment).filter(Equipment.id == eq_id).first()
    if item: db.delete(item); db.commit(); _cache.invalidate("equipment")
    return None

@app.get("/api/maintenance", response_model=List[MaintenanceForListResponse])
def get_all_maintenance(year: Optional[int] = None, db: DBSession=Depends(get_db), _=Depends(get_current_user)):
    q = db.query(EquipmentMaintenance).options(joinedload(EquipmentMaintenance.equipment)).order_by(EquipmentMaintenance.date.desc())
    if year: q = q.filter(extract("year", EquipmentMaintenance.date) == year)
    return q.all()

@app.get("/api/maintenance/{m_id}", response_model=MaintenanceDetailResponse)
def get_maintenance_details(m_id: int, db:DBSession=Depends(get_db), _=Depends(get_current_user)):
    m_record = db.query(EquipmentMaintenance).options(joinedload(EquipmentMaintenance.consumables_used).joinedload(MaintenanceConsumable.consumable), joinedload(EquipmentMaintenance.equipment)).filter(EquipmentMaintenance.id == m_id).first()
    if not m_record: raise HTTPException(404, "Запись о ТО не найдена")
    return m_record

@app.post("/api/maintenance", status_code=201, response_model=MaintenanceDetailResponse)
def create_maintenance_record(data: MaintenanceCreate, db:DBSession=Depends(get_db), _=Depends(get_current_user)):
    total_cost = 0
    try:
        for item_data in data.consumables:
            consumable = db.query(Consumable).filter(Consumable.id == item_data.consumable_id).with_for_update().first()
            if not consumable or consumable.stock_quantity < item_data.quantity:
                raise HTTPException(400, f"Недостаточно '{consumable.name if consumable else 'ID:'+str(item_data.consumable_id)}' на складе.")
            consumable.stock_quantity -= item_data.quantity
            total_cost += (consumable.price or 0) * item_data.quantity
        
        new_item = EquipmentMaintenance(equipment_id=data.equipment_id, date=data.date, work_description=data.work_description, notes=data.notes, cost=total_cost)
        db.add(new_item)
        db.flush()

        for item_data in data.consumables:
            consumable = db.query(Consumable).filter(Consumable.id == item_data.consumable_id).first()
            db.add(MaintenanceConsumable(maintenance_id=new_item.id, consumable_id=item_data.consumable_id, quantity=item_data.quantity, price_at_moment=(consumable.price or 0)))
        
        db.commit()
        db.refresh(new_item)
        update_equipment_last_maintenance(db, data.equipment_id)
        _cache.invalidate("consumables", "equipment", "maintenance")
        return new_item
    except:
        db.rollback()
        raise

@app.patch("/api/maintenance/{m_id}", response_model=MaintenanceDetailResponse)
def update_maintenance_record(m_id: int, data: MaintenanceUpdate, db:DBSession=Depends(get_db), _=Depends(get_current_user)):
    try:
        m_record = db.query(EquipmentMaintenance).options(joinedload(EquipmentMaintenance.consumables_used)).filter(EquipmentMaintenance.id == m_id).first()
        if not m_record: raise HTTPException(404, "Запись о ТО не найдена")

        if data.consumables is not None:
            for old_item in m_record.consumables_used:
                consumable = db.query(Consumable).filter(Consumable.id == old_item.consumable_id).with_for_update().first()
                if consumable: consumable.stock_quantity += old_item.quantity
            
            db.query(MaintenanceConsumable).filter(MaintenanceConsumable.maintenance_id == m_id).delete(synchronize_session=False)
            db.flush()

            total_cost = 0
            for item_data in data.consumables:
                consumable = db.query(Consumable).filter(Consumable.id == item_data.consumable_id).with_for_update().first()
                if not consumable or consumable.stock_quantity < item_data.quantity:
                    raise HTTPException(400, f"Недостаточно '{consumable.name if consumable else 'ID:'+str(item_data.consumable_id)}' на складе.")
                consumable.stock_quantity -= item_data.quantity
                total_cost += (consumable.price or 0) * item_data.quantity
                db.add(MaintenanceConsumable(maintenance_id=m_id, consumable_id=item_data.consumable_id, quantity=item_data.quantity, price_at_moment=(consumable.price or 0)))
            m_record.cost = total_cost

        update_data = data.model_dump(exclude_unset=True)
        if 'date' in update_data: m_record.date = update_data['date']
        if 'work_description' in update_data: m_record.work_description = update_data['work_description']
        if 'notes' in update_data: m_record.notes = update_data['notes']
        
        db.commit()
        db.refresh(m_record)
        update_equipment_last_maintenance(db, m_record.equipment_id)
        _cache.invalidate("consumables", "equipment", "maintenance")
        return m_record
    except:
        db.rollback()
        raise

@app.delete("/api/maintenance/{m_id}", status_code=204)
def delete_maintenance_record(m_id: int, db:DBSession=Depends(get_db), _=Depends(get_current_user)):
    try:
        item = db.query(EquipmentMaintenance).options(joinedload(EquipmentMaintenance.consumables_used)).filter(EquipmentMaintenance.id == m_id).first()
        if item:
            equipment_id = item.equipment_id
            for used in item.consumables_used:
                consumable = db.query(Consumable).filter(Consumable.id == used.consumable_id).with_for_update().first()
                if consumable: consumable.stock_quantity += used.quantity
            
            db.delete(item)
            db.commit()
            update_equipment_last_maintenance(db, equipment_id)
            _cache.invalidate("consumables", "equipment", "maintenance")
    except:
        db.rollback()
        raise
    return None

# --- CONSUMABLES ---
@app.get("/api/consumables")
def get_consumables(db:DBSession=Depends(get_db), _=Depends(get_current_user)):
    return db.query(Consumable).order_by(Consumable.name).all()

@app.post("/api/consumables", status_code=201)
def create_consumable(data: ConsumableCreate, db:DBSession=Depends(get_db), _=Depends(require_admin)):
    new_item = Consumable(**data.model_dump()); db.add(new_item); db.commit(); db.refresh(new_item)
    _cache.invalidate("consumables"); return new_item

@app.patch("/api/consumables/{c_id}")
def update_consumable(c_id: int, data: ConsumableUpdate, db:DBSession=Depends(get_db), _=Depends(require_admin)):
    item = db.query(Consumable).filter(Consumable.id == c_id).first()
    if not item: raise HTTPException(404, "Расходник не найден")
    update_data = data.model_dump(exclude_unset=True)
    for key, value in update_data.items(): setattr(item, key, value)
    db.commit(); db.refresh(item)
    _cache.invalidate("consumables"); return item

@app.delete("/api/consumables/{c_id}", status_code=204)
def delete_consumable(c_id: int, db:DBSession=Depends(get_db), _=Depends(require_admin)):
    if db.query(MaintenanceConsumable).filter(MaintenanceConsumable.consumable_id == c_id).count() > 0:
        raise HTTPException(400, "Нельзя удалить расходник, который используется в записях о ТО.")
    item = db.query(Consumable).filter(Consumable.id == c_id).first()
    if item: db.delete(item); db.commit(); _cache.invalidate("consumables")

# --- OTHER ---
@app.get("/api/years")
def get_years(db: DBSession=Depends(get_db),_=Depends(get_current_user)):
    if (cached := _cache.get("years")) is not None: return cached
    years=sorted(set([r[0] for r in db.execute(text("SELECT DISTINCT EXTRACT(YEAR FROM deal_date)::int FROM deals WHERE deal_date IS NOT NULL")).fetchall() if r[0]] + [r[0] for r in db.execute(text("SELECT DISTINCT EXTRACT(YEAR FROM date)::int FROM expenses WHERE date IS NOT NULL")).fetchall() if r[0]]), reverse=True) or [datetime.utcnow().year]
    _cache.set("years", years); return years

@app.get("/api/tasks")
def get_tasks(year: Optional[int] = None, is_done: Optional[bool] = None, priority: Optional[str] = None, contact_id: Optional[int] = None, deal_id: Optional[int] = None, db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    q = db.query(Task).options(joinedload(Task.contact), joinedload(Task.deal)).order_by(Task.due_date.asc())
    if year: q = q.filter(extract("year", Task.due_date) == year)
    if is_done is not None: q = q.filter(Task.is_done == is_done)
    if priority: q = q.filter(Task.priority == priority)
    if contact_id: q = q.filter(Task.contact_id == contact_id)
    if deal_id: q = q.filter(Task.deal_id == deal_id)
    if not is_admin(user): q = q.filter(Task.assignee == user["sub"])
    users = {u.id: u.name for u in db.query(User).all()}
    today = date.today()
    tasks_with_names = []
    for t in q.all():
        task_dict = {c.name: getattr(t, c.name) for c in t.__table__.columns}
        task_dict["assignee_name"] = users.get(t.assignee, "Не назначен")
        task_dict["contact_name"] = t.contact.name if t.contact else None
        task_dict["deal_title"] = t.deal.title if t.deal else None
        task_dict["is_overdue"] = bool(t.due_date and t.due_date < today and not t.is_done)
        tasks_with_names.append(task_dict)
    return tasks_with_names

@app.post("/api/tasks", status_code=201)
def create_task(task: TaskCreate, db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    new_task = Task(
        title=task.title, description=task.description,
        due_date=task.due_date or (date.today() + timedelta(days=1)),
        priority=task.priority, status=task.status,
        assignee=task.assignee or user['sub'],
        contact_id=task.contact_id, deal_id=task.deal_id
    )
    db.add(new_task); db.commit(); db.refresh(new_task)
    _cache.invalidate("tasks"); return new_task

@app.patch("/api/tasks/{task_id}")
def update_task(task_id: int, task_data: TaskUpdate, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task: raise HTTPException(404, "Task not found")
    for key, value in task_data.model_dump(exclude_unset=True).items(): setattr(task, key, value)
    if task_data.is_done: task.status = "Выполнена"
    elif task_data.status == "Выполнена": task.is_done = True
    db.commit(); _cache.invalidate("tasks"); return {"status": "ok"}

@app.delete("/api/tasks/{task_id}", status_code=204)
def delete_task(task_id: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    task = db.query(Task).filter(Task.id == task_id).first()
    if task: db.delete(task); db.commit(); _cache.invalidate("tasks")

# --- TAXES ---
@app.get("/api/taxes/summary")
def get_tax_summary(year: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    cache_key = f"tax_summary:{year}"
    if (cached := _cache.get(cache_key)) is not None: return cached

    success_stage = db.query(Stage).filter(Stage.name == "Успешно").first()
    if not success_stage: raise HTTPException(status_code=404, detail="Этап 'Успешно' не найден для расчета налога.")

    total_revenue = db.query(func.sum(Deal.total)).filter(
        Deal.stage_id == success_stage.id,
        extract("year", Deal.deal_date) == year
    ).scalar() or 0

    tax_accrued = total_revenue * TAX_RATE

    total_paid = db.query(func.sum(TaxPayment.amount)).filter(TaxPayment.year == year).scalar() or 0

    summary = {
        "revenue": round(total_revenue, 2),
        "tax_accrued": round(tax_accrued, 2),
        "paid": round(total_paid, 2),
        "balance": round(tax_accrued - total_paid, 2)
    }
    _cache.set(cache_key, summary)
    return summary

@app.get("/api/taxes/payments")
def get_tax_payments(year: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    cache_key = f"tax_payments:{year}"
    if (cached := _cache.get(cache_key)) is not None: return cached
    
    payments = db.query(TaxPayment).filter(TaxPayment.year == year).order_by(TaxPayment.date.desc()).all()
    _cache.set(cache_key, payments)
    return payments

@app.post("/api/taxes/payments", status_code=201)
def create_tax_payment(payment_data: TaxPaymentCreate, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    if payment_data.amount <= 0: raise HTTPException(status_code=400, detail="Сумма платежа должна быть положительной.")
    new_payment = TaxPayment(**payment_data.model_dump())
    db.add(new_payment); db.commit(); db.refresh(new_payment)
    _cache.invalidate(f"tax_summary:{payment_data.year}", f"tax_payments:{payment_data.year}", "years")
    return new_payment

class TaxPaymentUpdate(BaseModel):
    amount: Optional[float] = None
    date: Optional[date] = None
    note: Optional[str] = None

@app.patch("/api/taxes/payments/{payment_id}")
def update_tax_payment(payment_id: int, data: TaxPaymentUpdate, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    payment = db.query(TaxPayment).filter(TaxPayment.id == payment_id).first()
    if not payment: raise HTTPException(404, "Платёж не найден")
    if data.amount is not None and data.amount <= 0: raise HTTPException(400, "Сумма должна быть положительной.")
    for key, value in data.model_dump(exclude_unset=True).items(): setattr(payment, key, value)
    db.commit(); db.refresh(payment)
    _cache.invalidate(f"tax_summary:{payment.year}", f"tax_payments:{payment.year}")
    return payment

@app.delete("/api/taxes/payments/{payment_id}", status_code=204)
def delete_tax_payment(payment_id: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    payment = db.query(TaxPayment).filter(TaxPayment.id == payment_id).first()
    if payment:
        year = payment.year
        db.delete(payment); db.commit()
        _cache.invalidate(f"tax_summary:{year}", f"tax_payments:{year}")
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# НОВЫЕ ФИЧИ v13.0: Аналитика, Экспорт, Бюджет
# ═══════════════════════════════════════════════════════════════════════════════

# ── МОДЕЛЬ БЮДЖЕТ ─────────────────────────────────────────────────────────────
class Budget(Base):
    __tablename__ = "budgets"
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    period = Column(String(20), nullable=False)   # "year" | "q1"-"q4" | "jan"-"dec"
    name = Column(String(200), nullable=False)
    planned_revenue = Column(Float, default=0.0)
    planned_expenses = Column(Float, default=0.0)
    notes = Column(Text)

# Создать новую таблицу если ещё нет
from sqlalchemy import inspect as sa_inspect
def _ensure_budget_table():
    insp = sa_inspect(engine)
    if not insp.has_table("budgets"):
        Budget.__table__.create(engine)

# ── PYDANTIC для бюджета ──────────────────────────────────────────────────────
class BudgetCreate(BaseModel):
    year: int
    period: str
    name: str
    planned_revenue: Optional[float] = 0.0
    planned_expenses: Optional[float] = 0.0
    notes: Optional[str] = None

class BudgetUpdate(BaseModel):
    name: Optional[str] = None
    planned_revenue: Optional[float] = None
    planned_expenses: Optional[float] = None
    notes: Optional[str] = None

# ── АНАЛИТИКА ─────────────────────────────────────────────────────────────────
@app.get("/api/analytics/funnel")  # kept for backwards compat
def get_funnel(year: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    return get_analytics(year, db, _)

@app.get("/api/analytics")
def get_analytics(year: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    from collections import defaultdict
    MONTHS_RU = ["Янв","Фев","Мар","Апр","Май","Июн","Июл","Авг","Сен","Окт","Ноя","Дек"]

    stages = db.query(Stage).order_by(Stage.order).all()
    stage_map = {s.id: s for s in stages}

    deals = (db.query(Deal)
             .options(joinedload(Deal.services).joinedload(DealService.service),
                      joinedload(Deal.contact))
             .filter(extract("year", Deal.created_at) == year)
             .all())

    won_stage_ids  = {s.id for s in stages if s.is_final and "успешно" in s.name.lower()}
    lost_stage_ids = {s.id for s in stages if s.is_final and "успешно" not in s.name.lower()}
    won  = [d for d in deals if d.stage_id in won_stage_ids]
    lost = [d for d in deals if d.stage_id in lost_stage_ids]

    total_revenue = sum(d.total or 0 for d in won)
    avg_check     = round(total_revenue / len(won), 2) if won else 0
    win_rate      = round(len(won) / len(deals) * 100, 1) if deals else 0

    # 1. Воронка
    funnel = []
    for st in stages:
        sd = [d for d in deals if d.stage_id == st.id]
        funnel.append({
            "stage_id": st.id, "stage_name": st.name, "color": st.color,
            "is_final": st.is_final, "count": len(sd),
            "total": sum(d.total or 0 for d in sd),
        })

    # 2. Помесячная динамика
    rev_by_month = defaultdict(float)
    for d in won:
        m = (d.closed_at or d.created_at).month
        rev_by_month[m] += d.total or 0

    expenses_year = db.query(Expense).filter(extract("year", Expense.date) == year).all()
    exp_by_month  = defaultdict(float)
    for e in expenses_year:
        exp_by_month[e.date.month] += e.amount

    monthly = [
        {"month": i, "label": MONTHS_RU[i-1],
         "revenue":  round(rev_by_month[i], 2),
         "expenses": round(exp_by_month[i], 2),
         "profit":   round(rev_by_month[i] - exp_by_month[i], 2)}
        for i in range(1, 13)
    ]

    # 3. Топ услуг по выручке
    svc_revenue = defaultdict(float)
    svc_count   = defaultdict(int)
    svc_names   = {}
    for d in won:
        for ds in d.services:
            sname = ds.service.name if ds.service else f"#{ds.service_id}"
            svc_revenue[ds.service_id] += ds.price_at_moment * ds.quantity
            svc_count[ds.service_id]   += 1
            svc_names[ds.service_id]    = sname
    top_services = sorted(
        [{"id": sid, "name": svc_names[sid],
          "revenue": round(svc_revenue[sid], 2), "count": svc_count[sid]}
         for sid in svc_revenue],
        key=lambda x: x["revenue"], reverse=True
    )[:8]

    # 4. Расходы по категориям
    cats = {c.id: c.name for c in db.query(ExpenseCategory).all()}
    exp_by_cat = defaultdict(float)
    for e in expenses_year:
        exp_by_cat[cats.get(e.category_id, "Прочее")] += e.amount
    expense_by_category = sorted(
        [{"name": k, "amount": round(v, 2)} for k, v in exp_by_cat.items()],
        key=lambda x: x["amount"], reverse=True
    )

    # 5. Повторные клиенты
    repeat_won = [d for d in won if d.is_repeat]
    new_won    = [d for d in won if not d.is_repeat]
    repeat_rev = sum(d.total or 0 for d in repeat_won)
    new_rev    = sum(d.total or 0 for d in new_won)

    return {
        "total_deals":   len(deals),
        "won_deals":     len(won),
        "lost_deals":    len(lost),
        "win_rate":      win_rate,
        "avg_check":     avg_check,
        "total_revenue": round(total_revenue, 2),
        "total_expenses": round(sum(e.amount for e in expenses_year), 2),
        "funnel":              funnel,
        "monthly":             monthly,
        "top_services":        top_services,
        "expense_by_category": expense_by_category,
        "repeat": {
            "repeat_count":   len(repeat_won),
            "new_count":      len(new_won),
            "repeat_revenue": round(repeat_rev, 2),
            "new_revenue":    round(new_rev, 2),
            "repeat_rate":    round(len(repeat_won) / len(won) * 100, 1) if won else 0,
        },
    }


# ── БЮДЖЕТ — CRUD ─────────────────────────────────────────────────────────────
@app.get("/api/budget", response_model=List[BudgetResponse])
def get_budget(year: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    items = db.query(Budget).filter(Budget.year == year).order_by(Budget.id).all()
    return items

@app.post("/api/budget", status_code=201)
def create_budget(data: BudgetCreate, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    item = Budget(**data.model_dump())
    db.add(item); db.commit(); db.refresh(item)
    return item

@app.patch("/api/budget/{item_id}")
def update_budget(item_id: int, data: BudgetUpdate, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    item = db.query(Budget).filter(Budget.id == item_id).first()
    if not item: raise HTTPException(404, "Запись не найдена")
    for k, v in data.model_dump(exclude_unset=True).items():
        setattr(item, k, v)
    db.commit(); db.refresh(item)
    return item

@app.delete("/api/budget/{item_id}", status_code=204)
def delete_budget(item_id: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    item = db.query(Budget).filter(Budget.id == item_id).first()
    if item: db.delete(item); db.commit()
    return None

# ── ЭКСПОРТ EXCEL ─────────────────────────────────────────────────────────────
from fastapi.responses import StreamingResponse
import io

def _openpyxl_available():
    try:
        import openpyxl
        return True
    except ImportError:
        return False

@app.get("/api/export/excel")
def export_excel(year: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    if not _openpyxl_available():
        raise HTTPException(503, "openpyxl не установлен на сервере")
    
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    
    wb = openpyxl.Workbook()
    wb.remove(wb.active)  # убрать дефолтный лист

    GREEN = "1a3318"; SAGE = "4a7c3f"; LIGHT = "f5f2eb"; WHITE = "FFFFFF"
    
    def make_header(ws, cols, header_color=GREEN):
        for c, (title, width) in enumerate(cols, 1):
            cell = ws.cell(1, c, title)
            cell.font = Font(bold=True, color=WHITE, size=10)
            cell.fill = PatternFill("solid", fgColor=header_color)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            ws.column_dimensions[get_column_letter(c)].width = width
        ws.row_dimensions[1].height = 28

    def style_row(ws, row, r, alt=False):
        for c in range(1, len(row)+1):
            cell = ws.cell(r, c)
            cell.fill = PatternFill("solid", fgColor="F0ECE3" if alt else WHITE)
            cell.alignment = Alignment(vertical="center")
            thin = Side(style="thin", color="E2DDD4")
            cell.border = Border(bottom=Border(bottom=thin).bottom)

    # ── Лист 1: Сделки ────────────────────────────────────────────────────────
    ws1 = wb.create_sheet("Сделки")
    stages = {s.id: s for s in db.query(Stage).all()}
    contacts = {c.id: c for c in db.query(Contact).all()}
    deals_q = db.query(Deal).filter(extract('year', Deal.created_at) == year).order_by(Deal.created_at).all()
    cols1 = [("ID",6),("Название",30),("Клиент",22),("Этап",18),("Сумма",14),("Дата создания",18),("Дата закрытия",18),("Менеджер",18)]
    make_header(ws1, cols1)
    for i, d in enumerate(deals_q, 2):
        row = [d.id, d.title, contacts.get(d.contact_id, type("x",(),{"name":"—"})()).name,
               stages.get(d.stage_id, type("x",(),{"name":"—"})()).name,
               d.total or 0,
               d.created_at.strftime("%d.%m.%Y") if d.created_at else "",
               d.closed_at.strftime("%d.%m.%Y") if d.closed_at else "",
               d.manager or ""]
        for c, v in enumerate(row, 1): ws1.cell(i, c, v)
        style_row(ws1, row, i, i%2==0)
    ws1.freeze_panes = "A2"
    ws1.auto_filter.ref = f"A1:H{len(deals_q)+1}"

    # ── Лист 2: Расходы ───────────────────────────────────────────────────────
    ws2 = wb.create_sheet("Расходы")
    expenses_q = db.query(Expense).filter(extract('year', Expense.date) == year).order_by(Expense.date).all()
    cats = {c.id: c.name for c in db.query(ExpenseCategory).all()}
    cols2 = [("ID",6),("Дата",14),("Название",35),("Категория",20),("Сумма",14)]
    make_header(ws2, cols2)
    for i, e in enumerate(expenses_q, 2):
        row = [e.id, e.date.strftime("%d.%m.%Y") if e.date else "", e.name, cats.get(e.category_id,"—"), e.amount]
        for c, v in enumerate(row, 1): ws2.cell(i, c, v)
        style_row(ws2, row, i, i%2==0)
    ws2.freeze_panes = "A2"
    
    # Итого расходов
    total_row = len(expenses_q)+2
    ws2.cell(total_row, 4, "ИТОГО").font = Font(bold=True)
    ws2.cell(total_row, 5, sum(e.amount for e in expenses_q)).font = Font(bold=True)

    # ── Лист 3: Аналитика ─────────────────────────────────────────────────────
    ws3 = wb.create_sheet("Аналитика")
    won_stages = [s for s in stages.values() if s.is_final and "успешно" in s.name.lower()]
    won_ids = {s.id for s in won_stages}
    won_deals = [d for d in deals_q if d.stage_id in won_ids]
    total_rev = sum(d.total or 0 for d in won_deals)
    total_exp = sum(e.amount for e in expenses_q)
    avg_check = round(total_rev / len(won_deals), 2) if won_deals else 0

    ws3.column_dimensions["A"].width = 35
    ws3.column_dimensions["B"].width = 22
    summary_data = [
        ("Год", year), ("Всего сделок", len(deals_q)),
        ("Успешных сделок", len(won_deals)),
        ("Выручка", total_rev), ("Расходы", total_exp),
        ("Прибыль", total_rev - total_exp), ("Средний чек", avg_check),
    ]
    ws3.cell(1,1,"СВОДКА ЗА ГОД").font = Font(bold=True, size=13, color=GREEN)
    ws3.row_dimensions[1].height = 26
    for r, (label, val) in enumerate(summary_data, 2):
        c1 = ws3.cell(r, 1, label); c2 = ws3.cell(r, 2, val)
        c1.font = Font(bold=True, size=10)
        c2.fill = PatternFill("solid", fgColor=LIGHT)
        c2.alignment = Alignment(horizontal="right")

    ws3.cell(len(summary_data)+3, 1, "ВОРОНКА ПРОДАЖ").font = Font(bold=True, size=11, color=GREEN)
    funnel_cols = [("Этап",22),("Кол-во",12),("Сумма",16),("Конверсия от первого",22),("Конверсия от предыдущего",26)]
    frow = len(summary_data)+4
    for c,(t,w) in enumerate(funnel_cols,1):
        cell = ws3.cell(frow, c, t)
        cell.font = Font(bold=True, color=WHITE); cell.fill = PatternFill("solid", fgColor=SAGE)
        ws3.column_dimensions[get_column_letter(c)].width = w
    
    stage_list = sorted(stages.values(), key=lambda s: s.order)
    first_count = None
    for i, st in enumerate(stage_list):
        cnt = sum(1 for d in deals_q if d.stage_id == st.id)
        amt = sum(d.total or 0 for d in deals_q if d.stage_id == st.id)
        if first_count is None: first_count = cnt
        conv_first = f"{round(cnt/first_count*100,1)}%" if first_count else "—"
        prev_cnt = sum(1 for d in deals_q if d.stage_id == stage_list[i-1].id) if i>0 else cnt
        conv_step = f"{round(cnt/prev_cnt*100,1)}%" if (i>0 and prev_cnt) else "100%"
        r = frow+1+i
        for c,v in enumerate([st.name, cnt, amt, conv_first, conv_step],1):
            ws3.cell(r, c, v)
        if (r-frow)%2==0:
            for c in range(1,6): ws3.cell(r,c).fill = PatternFill("solid", fgColor="F0ECE3")

    # ── Лист 4: Бюджет ────────────────────────────────────────────────────────
    ws4 = wb.create_sheet("Бюджет")
    budgets_q = db.query(Budget).filter(Budget.year == year).all()
    cols4 = [("Название",28),("Период",14),("Плановая выручка",20),("Плановые расходы",20),("Плановая прибыль",20),("Заметки",30)]
    make_header(ws4, cols4)
    for i, b in enumerate(budgets_q, 2):
        profit = (b.planned_revenue or 0) - (b.planned_expenses or 0)
        row = [b.name, b.period, b.planned_revenue or 0, b.planned_expenses or 0, profit, b.notes or ""]
        for c, v in enumerate(row, 1): ws4.cell(i, c, v)
        style_row(ws4, row, i, i%2==0)

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="grasscrm_{year}.xlsx"'}
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

# ── ЭКСПОРТ PDF ───────────────────────────────────────────────────────────────
@app.get("/api/export/pdf")
def export_pdf(year: int, db: DBSession = Depends(get_db), _=Depends(require_admin)):
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.units import cm
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        import glob
        # Попытка зарегистрировать шрифт с поддержкой кириллицы
        font_registered = False
        for path in ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                     "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
                     "/usr/share/fonts/TTF/DejaVuSans.ttf"]:
            if os.path.exists(path):
                pdfmetrics.registerFont(TTFont("CyrFont", path))
                font_registered = True; break
        FONT = "CyrFont" if font_registered else "Helvetica"
    except ImportError:
        raise HTTPException(503, "reportlab не установлен на сервере")

    stages = {s.id: s for s in db.query(Stage).all()}
    contacts = {c.id: c for c in db.query(Contact).all()}
    deals_q = db.query(Deal).filter(extract('year', Deal.created_at) == year).order_by(Deal.created_at).all()
    expenses_q = db.query(Expense).filter(extract('year', Expense.date) == year).order_by(Expense.date).all()
    
    won_ids = {s.id for s in stages.values() if s.is_final and "успешно" in s.name.lower()}
    won_deals = [d for d in deals_q if d.stage_id in won_ids]
    total_rev = sum(d.total or 0 for d in won_deals)
    total_exp = sum(e.amount for e in expenses_q)
    avg_check = round(total_rev / len(won_deals), 2) if won_deals else 0

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    
    h1 = ParagraphStyle("h1", fontName=FONT, fontSize=18, spaceAfter=6, textColor=colors.HexColor("#1a3318"))
    h2 = ParagraphStyle("h2", fontName=FONT, fontSize=13, spaceAfter=4, textColor=colors.HexColor("#2d5426"), spaceBefore=16)
    normal = ParagraphStyle("n", fontName=FONT, fontSize=9, leading=14)
    
    FOREST = colors.HexColor("#1a3318"); SAGE_C = colors.HexColor("#4a7c3f")
    LIGHT_C = colors.HexColor("#f5f2eb"); ALT_C = colors.HexColor("#f0ece3")
    
    def tbl_style(header_color=FOREST):
        return TableStyle([
            ("BACKGROUND",(0,0),(-1,0), header_color),
            ("TEXTCOLOR",(0,0),(-1,0), colors.white),
            ("FONTNAME",(0,0),(-1,-1), FONT),
            ("FONTSIZE",(0,0),(-1,0), 8), ("FONTSIZE",(0,1),(-1,-1), 8),
            ("FONTNAME",(0,0),(-1,0), FONT),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white, ALT_C]),
            ("GRID",(0,0),(-1,-1), 0.3, colors.HexColor("#E2DDD4")),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ("LEFTPADDING",(0,0),(-1,-1),6), ("RIGHTPADDING",(0,0),(-1,-1),6),
            ("TOPPADDING",(0,0),(-1,-1),4), ("BOTTOMPADDING",(0,0),(-1,-1),4),
        ])

    def fmt_money(v): return f"{v:,.0f} руб.".replace(",","_")

    story = []
    story.append(Paragraph(f"GrassCRM — Отчёт за {year} год", h1))
    story.append(Paragraph(f"Сформирован: {date.today().strftime('%d.%m.%Y')}", normal))
    story.append(Spacer(1, 12))

    # Сводка
    story.append(Paragraph("Сводка", h2))
    summary_rows = [['Показатель','Значение'],
        ["Всего сделок", str(len(deals_q))],
        ["Успешных сделок", str(len(won_deals))],
        ["Выручка", fmt_money(total_rev)],
        ["Расходы", fmt_money(total_exp)],
        ["Прибыль", fmt_money(total_rev-total_exp)],
        ["Средний чек", fmt_money(avg_check)],
    ]
    t = Table(summary_rows, colWidths=[8*cm, 8*cm])
    t.setStyle(tbl_style())
    story.append(t); story.append(Spacer(1,8))

    # Воронка
    story.append(Paragraph("Воронка продаж", h2))
    funnel_rows = [['Этап','Кол-во','Сумма','Конверсия']]
    stage_list = sorted(stages.values(), key=lambda s: s.order)
    first_cnt = None
    for st in stage_list:
        cnt = sum(1 for d in deals_q if d.stage_id == st.id)
        amt = sum(d.total or 0 for d in deals_q if d.stage_id == st.id)
        if first_cnt is None: first_cnt = cnt
        conv = f"{round(cnt/first_cnt*100,1)}%" if first_cnt else "—"
        funnel_rows.append([st.name, str(cnt), fmt_money(amt), conv])
    t2 = Table(funnel_rows, colWidths=[7*cm,3*cm,5*cm,3*cm])
    t2.setStyle(tbl_style(SAGE_C)); story.append(t2); story.append(Spacer(1,8))

    # Топ-10 сделок
    story.append(Paragraph("Топ-10 успешных сделок", h2))
    top10 = sorted(won_deals, key=lambda d: d.total or 0, reverse=True)[:10]
    deal_rows = [['Название','Клиент','Сумма','Дата']]
    for d in top10:
        deal_rows.append([d.title[:35], contacts.get(d.contact_id, type("x",(),{"name":"—"})()).name,
                         fmt_money(d.total or 0), d.closed_at.strftime("%d.%m.%Y") if d.closed_at else ""])
    t3 = Table(deal_rows, colWidths=[7*cm,4*cm,4*cm,3*cm])
    t3.setStyle(tbl_style()); story.append(t3)

    doc.build(story)
    buf.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="grasscrm_{year}.pdf"'}
    return StreamingResponse(buf, media_type="application/pdf", headers=headers)


# ── INTERACTIONS ───────────────────────────────────────────────────────────────

@app.get("/api/contacts/{contact_id}/interactions")
def get_interactions(contact_id: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    items = db.query(Interaction).filter(
        Interaction.contact_id == contact_id
    ).order_by(Interaction.created_at.desc()).all()
    return [
        {
            "id": i.id,
            "type": i.type,
            "text": i.text,
            "created_at": i.created_at.isoformat() if i.created_at else None,
            "user_name": i.user_name or "—"
        }
        for i in items
    ]

@app.post("/api/contacts/{contact_id}/interactions", status_code=201)
def create_interaction(
    contact_id: int,
    data: InteractionCreate,
    db: DBSession = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    contact = db.query(Contact).filter(Contact.id == contact_id).first()
    if not contact:
        raise HTTPException(404, "Контакт не найден")
    item = Interaction(
        contact_id=contact_id,
        type=data.type,
        text=data.text,
        user_id=user.get("sub"),
        user_name=user.get("name")
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return {
        "id": item.id,
        "type": item.type,
        "text": item.text,
        "created_at": item.created_at.isoformat(),
        "user_name": item.user_name or "—"
    }

@app.delete("/api/interactions/{interaction_id}", status_code=204)
def delete_interaction(
    interaction_id: int,
    db: DBSession = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    item = db.query(Interaction).filter(Interaction.id == interaction_id).first()
    if not item:
        raise HTTPException(404, "Запись не найдена")
    if item.user_id != user.get("sub") and not is_admin(user):
        raise HTTPException(403, "Нет доступа")
    db.delete(item)
    db.commit()
    return None


# ── DEAL COMMENTS ──────────────────────────────────────────────────────────────

@app.get("/api/deals/{deal_id}/comments")
def get_deal_comments(deal_id: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    items = db.query(DealComment).filter(
        DealComment.deal_id == deal_id
    ).order_by(DealComment.created_at.asc()).all()
    return [
        {
            "id": i.id,
            "text": i.text,
            "created_at": i.created_at.isoformat() if i.created_at else None,
            "user_name": i.user_name or "—",
            "user_id": i.user_id
        }
        for i in items
    ]

@app.post("/api/deals/{deal_id}/comments", status_code=201)
def create_deal_comment(
    deal_id: int,
    data: DealCommentCreate,
    db: DBSession = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    deal = db.query(Deal).filter(Deal.id == deal_id).first()
    if not deal:
        raise HTTPException(404, "Сделка не найдена")
    item = DealComment(
        deal_id=deal_id,
        text=data.text,
        user_id=user.get("sub"),
        user_name=user.get("name")
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return {
        "id": item.id,
        "text": item.text,
        "created_at": item.created_at.isoformat(),
        "user_name": item.user_name or "—",
        "user_id": item.user_id
    }

@app.delete("/api/deal-comments/{comment_id}", status_code=204)
def delete_deal_comment(
    comment_id: int,
    db: DBSession = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    item = db.query(DealComment).filter(DealComment.id == comment_id).first()
    if not item:
        raise HTTPException(404, "Комментарий не найден")
    if item.user_id != user.get("sub") and not is_admin(user):
        raise HTTPException(403, "Нет доступа")
    db.delete(item)
    db.commit()
    return None


# ── FILES ──────────────────────────────────────────────────────────────────────

ALLOWED_EXTENSIONS = {
    '.pdf','.doc','.docx','.xls','.xlsx','.ppt','.pptx',
    '.jpg','.jpeg','.png','.gif','.webp',
    '.txt','.csv','.zip','.rar',
    '.mp4','.mov','.avi'
}

def _fmt_size(b: int) -> str:
    if b < 1024: return f"{b} Б"
    if b < 1024**2: return f"{b/1024:.1f} КБ"
    return f"{b/1024**2:.1f} МБ"

@app.get("/api/files")
def get_files(contact_id: Optional[int] = None, deal_id: Optional[int] = None,
              db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    q = db.query(CRMFile).order_by(CRMFile.created_at.desc())
    if contact_id: q = q.filter(CRMFile.contact_id == contact_id)
    if deal_id:    q = q.filter(CRMFile.deal_id == deal_id)
    return [{
        "id": f.id, "filename": f.filename, "size": f.size,
        "size_fmt": _fmt_size(f.size), "mime_type": f.mime_type,
        "contact_id": f.contact_id, "deal_id": f.deal_id,
        "uploaded_by_name": f.uploaded_by_name or "—",
        "created_at": f.created_at.isoformat() if f.created_at else None,
        "contact_name": f.contact.name if f.contact else None,
        "deal_title": f.deal.title if f.deal else None,
    } for f in q.all()]

@app.post("/api/files", status_code=201)
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    contact_id: Optional[int] = Form(None),
    deal_id: Optional[int] = Form(None),
    db: DBSession = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, f"Тип файла не разрешён: {ext}")

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(400, "Файл слишком большой (макс. 20 МБ)")

    stored_name = f"{secrets.token_hex(12)}{ext}"
    dest = UPLOAD_DIR / stored_name
    dest.write_bytes(content)

    rec = CRMFile(
        filename=file.filename,
        stored_name=stored_name,
        size=len(content),
        mime_type=file.content_type,
        contact_id=contact_id,
        deal_id=deal_id,
        uploaded_by=user.get("sub"),
        uploaded_by_name=user.get("name"),
    )
    db.add(rec); db.commit(); db.refresh(rec)
    return {
        "id": rec.id, "filename": rec.filename, "size": rec.size,
        "size_fmt": _fmt_size(rec.size), "mime_type": rec.mime_type,
    }

@app.get("/api/files/{file_id}/download")
def download_file(file_id: int, db: DBSession = Depends(get_db), _=Depends(get_current_user)):
    from fastapi.responses import FileResponse as FR
    rec = db.query(CRMFile).filter(CRMFile.id == file_id).first()
    if not rec: raise HTTPException(404, "Файл не найден")
    path = UPLOAD_DIR / rec.stored_name
    if not path.exists(): raise HTTPException(404, "Файл удалён с диска")
    return FR(str(path), filename=rec.filename, media_type=rec.mime_type or "application/octet-stream")

@app.delete("/api/files/{file_id}", status_code=204)
def delete_file(file_id: int, db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    rec = db.query(CRMFile).filter(CRMFile.id == file_id).first()
    if not rec: raise HTTPException(404, "Файл не найден")
    if rec.uploaded_by != user.get("sub") and not is_admin(user):
        raise HTTPException(403, "Нет доступа")
    path = UPLOAD_DIR / rec.stored_name
    if path.exists(): path.unlink()
    db.delete(rec); db.commit()
    return None


# ── SERVICE PANEL ─────────────────────────────────────────────────────────────

@app.get("/api/service/status")
def service_status(db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    if not is_admin(user): raise HTTPException(403, "Admin only")
    
    system_payload = {}
    try:
        import psutil
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        cpu = psutil.cpu_percent(interval=1.0)
        system_payload = {
            "cpu": round(cpu, 1),
            "mem_used_mb": round(mem.used / 1024**2),
            "mem_total_mb": round(mem.total / 1024**2),
            "mem_pct": round(mem.percent, 1),
            "disk_used_gb": round(disk.used / 1024**3, 1),
            "disk_total_gb": round(disk.total / 1024**3, 1),
            "disk_pct": round(disk.percent, 1),
        }
    except ImportError:
        system_payload = {"error": "psutil is not installed"}
    except Exception as e:
        system_payload = {"error": str(e)}

    active_tasks = db.query(Task).filter(Task.is_done == False).count()
    overdue = db.query(Task).filter(
        Task.is_done == False, Task.due_date < datetime.utcnow().date()
    ).count()
    active_deals = db.query(Deal).join(Stage).filter(Stage.is_final == False).count()
    cache_keys = list(_cache._data.keys())

    # Дата последнего бэкапа
    import glob as _glob
    backup_files = sorted(_glob.glob("/var/www/crm/GCRM-2/backups/*.sql.gz"))
    last_backup = None
    if backup_files:
        mtime = os.path.getmtime(backup_files[-1])
        last_backup = datetime.fromtimestamp(mtime).strftime("%d.%m.%Y %H:%M")

    return {
        "db": {
            "deals": db.query(Deal).count(),
            "contacts": db.query(Contact).count(),
            "active_tasks": active_tasks,
            "overdue_tasks": overdue,
            "active_deals": active_deals,
            "last_backup": last_backup,
        },
        "cache": {"keys": cache_keys, "count": len(cache_keys), "ttl": _cache._ttl},
        "system": system_payload,
        "tg_configured": bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID")),
    }

@app.post("/api/service/cache/clear")
def service_clear_cache(user: dict = Depends(get_current_user)):
    if not is_admin(user): raise HTTPException(403, "Admin only")
    _cache._data.clear()
    return {"ok": True, "message": "Кэш полностью очищен"}

@app.post("/api/service/bot/report")
async def service_send_report(db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    if not is_admin(user): raise HTTPException(403, "Admin only")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat  = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        raise HTTPException(400, "TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID не заданы в .env")

    today = datetime.utcnow().date()
    lines = []

    # ── Заголовок
    lines.append(f"🌿 GrassCRM — отчёт за {today.strftime('%d.%m.%Y')}")
    lines.append("")

    # ── Сделки в работе
    active_deals = (
        db.query(Deal)
        .join(Stage)
        .filter(Stage.is_final == False)
        .options(joinedload(Deal.contact), joinedload(Deal.stage))
        .order_by(Stage.order, Deal.created_at.desc())
        .all()
    )
    if active_deals:
        lines.append("📋 Сделки в работе:")
        current_stage = None
        for deal in active_deals:
            stage_name = deal.stage.name if deal.stage else "Без стадии"
            if stage_name != current_stage:
                current_stage = stage_name
                lines.append(f"\n  ▸ {stage_name}")
            client = deal.contact.name if deal.contact else "Без клиента"
            total = f"{int(deal.total or 0):,}".replace(",", " ")
            lines.append(f"    · {deal.title} ({client}) — {total} руб.")
            if deal.deal_date:
                lines.append(f"      📅 {deal.deal_date.strftime('%d.%m.%Y %H:%M')}")
            if deal.address:
                lines.append(f"      📍 {deal.address}")
    else:
        lines.append("📋 Активных сделок нет.")

    lines.append("")

    # ── Задачи
    today_tasks = (
        db.query(Task)
        .filter(Task.is_done == False, Task.due_date == today)
        .all()
    )
    overdue_tasks = (
        db.query(Task)
        .filter(Task.is_done == False, Task.due_date < today)
        .order_by(Task.due_date)
        .all()
    )

    if today_tasks:
        lines.append("✅ Задачи на сегодня:")
        for t in today_tasks:
            lines.append(f"    · {t.title}")
        lines.append("")

    if overdue_tasks:
        lines.append(f"⚠️ Просрочено ({len(overdue_tasks)}):")
        for t in overdue_tasks[:5]:
            due = t.due_date.strftime("%d.%m") if t.due_date else "—"
            lines.append(f"    · {t.title} (до {due})")
        if len(overdue_tasks) > 5:
            lines.append(f"    ...и ещё {len(overdue_tasks) - 5}")
        lines.append("")

    if not today_tasks and not overdue_tasks:
        lines.append("✅ Задач на сегодня нет.")
        lines.append("")

    # ── Цитата
    try:
        row = db.execute(text("SELECT phrase FROM daily_phrases ORDER BY RANDOM() LIMIT 1")).fetchone()
        if row:
            phrase = row[0].replace("\\n", "\n")
            lines.append("· · · · · · · · · ·")
            lines.append(phrase)
    except Exception:
        pass

    msg_text = "\n".join(lines)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": msg_text}
        )
        if not r.is_success:
            raise HTTPException(500, f"Telegram ошибка {r.status_code}: {r.text[:200]}")
    return {"ok": True, "message": "Отчёт отправлен в Telegram"}

@app.post("/api/service/backup")
def service_backup(user: dict = Depends(get_current_user)):
    if not is_admin(user): raise HTTPException(403, "Admin only")
    import subprocess as _sp, glob as _glob, re as _re
    db_url = os.getenv("DATABASE_URL", "")
    m = _re.match(r"postgresql(?:\+\w+)?://([^:]+):([^@]+)@([^:/]+):?(\d+)?/(\S+)", db_url)
    if not m:
        raise HTTPException(500, "Не удалось разобрать DATABASE_URL")
    db_user, db_pass, db_host, db_port, db_name = m.group(1), m.group(2), m.group(3), m.group(4) or "5432", m.group(5)
    db_name = db_name.split("?")[0]

    backup_dir = "/var/www/crm/GCRM-2/backups"
    os.makedirs(backup_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_file = f"{backup_dir}/backup_{ts}.sql.gz"

    # Ищем pg_dump подходящей версии (17 → 16 → системный)
    pg_dump_cmd = None
    for candidate in ["/usr/lib/postgresql/17/bin/pg_dump", "/usr/lib/postgresql/16/bin/pg_dump", "pg_dump"]:
        try:
            r = _sp.run([candidate, "--version"], capture_output=True, text=True, timeout=3)
            if r.returncode == 0:
                pg_dump_cmd = candidate
                break
        except FileNotFoundError:
            continue
    if not pg_dump_cmd:
        raise HTTPException(500, "pg_dump не найден. Установите: apt install postgresql-client-17")

    env = {**os.environ, "PGPASSWORD": db_pass}
    try:
        dump = _sp.Popen(
            [pg_dump_cmd, "-h", db_host, "-p", db_port, "-U", db_user, "-d", db_name,
             "--no-password", "-F", "p"],
            stdout=_sp.PIPE, stderr=_sp.PIPE, env=env
        )
        with open(out_file, "wb") as f_out:
            gzip_proc = _sp.Popen(["gzip", "-c"], stdin=dump.stdout, stdout=f_out, stderr=_sp.PIPE)
        dump.stdout.close()
        _, dump_err = dump.communicate(timeout=120)
        gzip_proc.communicate(timeout=30)
        if dump.returncode != 0:
            if os.path.exists(out_file): os.remove(out_file)
            raise HTTPException(500, f"pg_dump ошибка: {dump_err.decode()[:300]}")
    except _sp.TimeoutExpired:
        raise HTTPException(500, "pg_dump timeout (120s)")

    # Оставляем последние 10 бэкапов
    all_backups = sorted(_glob.glob(f"{backup_dir}/backup_*.sql.gz"))
    for old in all_backups[:-10]:
        try: os.remove(old)
        except: pass

    size_kb = round(os.path.getsize(out_file) / 1024, 1)
    return {"ok": True, "message": f"Бэкап создан: backup_{ts}.sql.gz ({size_kb} КБ)"}


@app.post("/api/service/db/check")
def service_db_check(db: DBSession = Depends(get_db), user: dict = Depends(get_current_user)):
    if not is_admin(user): raise HTTPException(403, "Admin only")
    try:
        ver = db.execute(text("SELECT version()")).scalar()
        return {"ok": True, "message": "БД доступна", "detail": str(ver)[:80]}
    except Exception as e:
        raise HTTPException(500, f"Ошибка: {e}")

@app.get("/api/service/logs")
def service_get_logs(n: int = 60, user: dict = Depends(get_current_user)):
    if not is_admin(user): raise HTTPException(403, "Admin only")
    import subprocess as _sp
    try:
        r = _sp.run(["journalctl", "-u", "crm.service", f"-n{n}", "--no-pager", "--output=short"],
                    capture_output=True, text=True, timeout=6)
        return {"ok": True, "logs": r.stdout}
    except Exception as e:
        return {"ok": False, "logs": str(e)}

@app.post("/api/service/restart")
def service_restart(user: dict = Depends(get_current_user)):
    if not is_admin(user): raise HTTPException(403, "Admin only")
    import subprocess as _sp, threading as _th
    def _do():
        import time as _t; _t.sleep(2)
        _sp.Popen(["systemctl", "restart", "crm"])
    _th.Thread(target=_do, daemon=True).start()
    return {"ok": True, "message": "Перезапуск через 2 секунды…"}

@app.get("/{full_path:path}", response_class=FileResponse)
async def serve_frontend(full_path: str):
    path = f"./{full_path.strip()}" if full_path else "./index.html"
    return FileResponse(path if os.path.isfile(path) else "./index.html")

print(f"main.py (v14.0) loaded — backup, cpu fix, analytics.", flush=True)
