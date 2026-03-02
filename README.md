# 🌿 Grass CRM — CRM для бизнеса по покосу травы

Полноценная CRM-система с MCP-сервером для интеграции с Claude Desktop.

## Структура проекта

```
grass-crm/
├── models/
│   └── database.py          # Модели SQLAlchemy (7 таблиц)
├── data/
│   └── seed_data.py         # Начальные данные (37 услуг, 8 техники, 6 этапов)
├── mcp_server/
│   └── server.py            # MCP-сервер (20 инструментов)
├── api/
│   └── main.py              # FastAPI REST API (15+ эндпоинтов)
├── scripts/
│   └── init_db.py           # Инициализация и заполнение БД
├── requirements.txt
└── claude_desktop_config.json
```

## Быстрый старт

### 1. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 2. Инициализация базы данных
```bash
python scripts/init_db.py
```
БД создаётся автоматически как `crm.db` в корне проекта.

### 3. Запуск REST API (для фронтенда)
```bash
uvicorn api.main:app --reload --port 8000
```
Swagger UI: http://localhost:8000/docs

### 4. Подключение MCP к Claude Desktop

Откройте конфиг Claude Desktop:
- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

Добавьте в `mcpServers`:
```json
{
  "mcpServers": {
    "grass-crm": {
      "command": "python",
      "args": ["/ПОЛНЫЙ/ПУТЬ/grass-crm/mcp_server/server.py"],
      "env": {
        "DATABASE_URL": "sqlite:////ПОЛНЫЙ/ПУТЬ/grass-crm/crm.db"
      }
    }
  }
}
```

Перезапустите Claude Desktop. В чате появятся инструменты CRM.

---

## Инструменты MCP (20 штук)

### 📋 Сделки
| Инструмент | Описание |
|---|---|
| `create_deal` | Создать сделку с набором услуг |
| `get_deals` | Список сделок (фильтры: клиент, этап, год, месяц) |
| `update_deal_stage` | Переместить сделку на другой этап |
| `get_deal_stages` | Все этапы канбана с количеством сделок |
| `get_deal_statistics` | Статистика по году: по месяцам и этапам |

### 🔧 Техника
| Инструмент | Описание |
|---|---|
| `add_equipment` | Добавить технику |
| `get_equipment` | Список техники (фильтр по статусу) |
| `update_equipment_status` | Изменить статус (active/repair/retired) |
| `schedule_maintenance` | Добавить/запланировать ТО |
| `get_maintenance_schedule` | Предстоящие ТО (по умолчанию 30 дней) |

### 💰 Расходы
| Инструмент | Описание |
|---|---|
| `add_expense` | Добавить расход |
| `get_expenses` | Список расходов (фильтры: год, месяц, категория) |
| `get_expense_categories` | Все категории расходов |
| `get_expense_summary` | Сводка по категориям за год |

### 🌿 Услуги
| Инструмент | Описание |
|---|---|
| `get_services` | Прайс-лист (фильтр по категории) |
| `get_service_categories` | Все категории услуг |
| `calculate_service_cost` | Расчёт стоимости с учётом мин. объёма |

### 📊 Отчёты
| Инструмент | Описание |
|---|---|
| `generate_profit_loss_report` | П&У за год (доходы - расходы) |
| `generate_equipment_report` | Состояние всей техники |
| `generate_client_history_report` | История сделок клиента |

---

## REST API эндпоинты

```
GET    /api/stages                           — этапы канбана
GET    /api/deals?stage=&client=&year=&month= — список сделок
POST   /api/deals                            — создать сделку
GET    /api/deals/{id}                       — детали сделки
PATCH  /api/deals/{id}/stage                 — изменить этап

GET    /api/equipment?status=                — список техники
POST   /api/equipment                        — добавить технику
POST   /api/equipment/{id}/maintenance       — добавить ТО
PATCH  /api/equipment/{id}/status            — статус техники

GET    /api/expenses?year=&month=&category=  — расходы
POST   /api/expenses                         — добавить расход

GET    /api/services?category=               — прайс-лист
GET    /api/services/categories              — категории услуг

GET    /api/reports/profit-loss/{year}       — П&У отчёт
GET    /api/reports/export/{type}/{year}     — экспорт CSV
         type: deals | expenses | profit-loss
```

---

## Примеры запросов к Claude

После подключения MCP пишите Claude в обычном чате:

```
Покажи все сделки клиента 'Антон Пески' за 2025 год

Какая техника сейчас в ремонте?

Какие расходы были на топливо в мае 2025?

Создай сделку для Иванова: покос 10 соток + обкос триммером 2 часа

Какова прибыль за 2025 год?

Добавь расход: бензин 5000 рублей, 15 мая 2025

Покажи статистику сделок за 2024 год по месяцам

Какое ТО нужно сделать в ближайшие 2 недели?
```

---

## База данных

SQLite файл `crm.db` создаётся автоматически. 7 таблиц:

- **stages** — 6 этапов (Согласовать → Провалена)
- **service_categories** — 8 категорий услуг
- **services** — 37 услуг из прайс-листа
- **deals** — сделки
- **deal_services** — услуги в каждой сделке (many-to-many)
- **equipment** — 8 единиц техники
- **maintenances** — история ТО
- **expense_categories** — 8 категорий расходов
- **expenses** — расходы

## Импорт исторических данных

Для импорта ваших Excel-данных добавьте в `scripts/import_history.py`:

```python
# Пример импорта сделок из CSV/Excel
import pandas as pd
from models.database import get_engine, get_session_factory, Deal, Stage
from datetime import datetime

engine = get_engine()
Session = get_session_factory(engine)

df = pd.read_excel("deals_2024.xlsx")
with Session() as session:
    for _, row in df.iterrows():
        stage = session.query(Stage).filter_by(name=row["Этап"]).first()
        deal = Deal(
            title=row["Название"],
            client=row["Клиент"],
            stage_id=stage.id if stage else None,
            created_at=pd.to_datetime(row["Дата"]),
        )
        session.add(deal)
    session.commit()
```
