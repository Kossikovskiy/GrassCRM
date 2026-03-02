"""
CRM для бизнеса по покосу травы — модели базы данных
"""

from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime, Boolean,
    ForeignKey, Text, create_engine, inspect, text
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from datetime import datetime
import os

Base = declarative_base()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./crm.db")


class Stage(Base):
    """Этапы сделок (канбан)"""
    __tablename__ = "stages"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    order = Column(Integer, default=0)
    type = Column(String(50), default="regular")   # regular | success | failed
    is_final = Column(Boolean, default=False)
    color = Column(String(20), default="#6B7280")  # HEX цвет для канбана

    deals = relationship("Deal", back_populates="stage")


class ServiceCategory(Base):
    """Категории услуг"""
    __tablename__ = "service_categories"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    icon = Column(String(50), default="🌿")

    services = relationship("Service", back_populates="category")


class Service(Base):
    """Прайс-лист услуг"""
    __tablename__ = "services"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    category_id = Column(Integer, ForeignKey("service_categories.id"))
    unit = Column(String(50), default="ед")       # сотка, м², ед, час, м.п.
    price = Column(Float, nullable=False)
    min_volume = Column(Float, default=1.0)        # минимальный объём
    description = Column(Text, default="")

    category = relationship("ServiceCategory", back_populates="services")
    deal_services = relationship("DealService", back_populates="service")


class Deal(Base):
    """Сделки"""
    __tablename__ = "deals"

    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    client = Column(String(200), nullable=False)
    stage_id = Column(Integer, ForeignKey("stages.id"))
    manager = Column(String(100), default="")
    address = Column(String(300), default="")
    notes = Column(Text, default="")
    vat_rate = Column(String(10), default="no_vat")  # no_vat | vat_4 | vat_6
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)

    stage = relationship("Stage", back_populates="deals")
    deal_services = relationship("DealService", back_populates="deal", cascade="all, delete-orphan")


class DealService(Base):
    """Услуги в сделке (many-to-many с доп. полями)"""
    __tablename__ = "deal_services"

    id = Column(Integer, primary_key=True)
    deal_id = Column(Integer, ForeignKey("deals.id"))
    service_id = Column(Integer, ForeignKey("services.id"))
    quantity = Column(Float, default=1.0)
    price_at_moment = Column(Float, nullable=False)   # цена на момент создания
    notes = Column(String(300), default="")

    deal = relationship("Deal", back_populates="deal_services")
    service = relationship("Service", back_populates="deal_services")


class Equipment(Base):
    """Техника и инвентарь"""
    __tablename__ = "equipment"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    model = Column(String(200), default="")
    serial = Column(String(100), default="")
    purchase_date = Column(Date, nullable=True)
    purchase_cost = Column(Float, default=0.0)
    engine_hours = Column(Float, default=0.0)
    status = Column(String(50), default="active")   # active | repair | retired
    last_maintenance = Column(Date, nullable=True)
    next_maintenance = Column(Date, nullable=True)
    notes = Column(Text, default="")

    maintenances = relationship("Maintenance", back_populates="equipment")
    expenses = relationship("Expense", back_populates="equipment")


class Maintenance(Base):
    """История технического обслуживания"""
    __tablename__ = "maintenances"

    id = Column(Integer, primary_key=True)
    equipment_id = Column(Integer, ForeignKey("equipment.id"))
    date = Column(Date, nullable=False)
    description = Column(Text, nullable=False)
    cost = Column(Float, default=0.0)
    performed_by = Column(String(100), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    equipment = relationship("Equipment", back_populates="maintenances")
    consumables = relationship("MaintenanceConsumable", back_populates="maintenance", cascade="all, delete-orphan")


class Consumable(Base):
    """Складской учёт расходников/масел"""
    __tablename__ = "consumables"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    base_unit = Column(String(20), nullable=False, default="ml")  # ml | g | pcs
    stock_quantity = Column(Float, default=0.0)
    price_per_unit = Column(Float, default=0.0)
    notes = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    maintenance_items = relationship("MaintenanceConsumable", back_populates="consumable")


class MaintenanceConsumable(Base):
    """Списания расходников в рамках ТО"""
    __tablename__ = "maintenance_consumables"

    id = Column(Integer, primary_key=True)
    maintenance_id = Column(Integer, ForeignKey("maintenances.id"), nullable=False)
    consumable_id = Column(Integer, ForeignKey("consumables.id"), nullable=False)
    quantity = Column(Float, nullable=False)  # в base_unit расходника
    unit_cost = Column(Float, nullable=False)
    subtotal = Column(Float, nullable=False)

    maintenance = relationship("Maintenance", back_populates="consumables")
    consumable = relationship("Consumable", back_populates="maintenance_items")


class ExpenseCategory(Base):
    """Категории расходов"""
    __tablename__ = "expense_categories"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)

    expenses = relationship("Expense", back_populates="category")


class Expense(Base):
    """Расходы"""
    __tablename__ = "expenses"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    name = Column(String(300), nullable=False)
    category_id = Column(Integer, ForeignKey("expense_categories.id"))
    amount = Column(Float, nullable=False)
    year = Column(Integer, nullable=False)
    equipment_id = Column(Integer, ForeignKey("equipment.id"), nullable=True)
    notes = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    category = relationship("ExpenseCategory", back_populates="expenses")
    equipment = relationship("Equipment", back_populates="expenses")


def get_engine(url: str = DATABASE_URL):
    return create_engine(url, echo=False)


def get_session_factory(engine):
    return sessionmaker(bind=engine)


def init_db(engine):
    Base.metadata.create_all(engine)

    # Лёгкая миграция для существующих БД: добавляем колонку моточасов при отсутствии.
    inspector = inspect(engine)
    if "equipment" in inspector.get_table_names():
        columns = {c["name"] for c in inspector.get_columns("equipment")}
        if "engine_hours" not in columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE equipment ADD COLUMN engine_hours FLOAT DEFAULT 0"))

    if "deals" in inspector.get_table_names():
        deal_columns = {c["name"] for c in inspector.get_columns("deals")}
        if "vat_rate" not in deal_columns:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE deals ADD COLUMN vat_rate VARCHAR(10) DEFAULT 'no_vat'"))
