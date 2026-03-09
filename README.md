# GrassCRM

CRM-система для компании по покосу газонов и ландшафтным работам. Бэкенд на FastAPI, фронтенд — одностраничное приложение на чистом JS. Развёрнута на VPS под Ubuntu 24.04, данные хранятся в PostgreSQL (Supabase).

---

## Стек

| Слой | Технология |
|---|---|
| Бэкенд | Python 3.12, FastAPI, SQLAlchemy |
| Веб-сервер | Uvicorn + systemd |
| База данных | PostgreSQL (Supabase, Session Pooler) |
| Аутентификация | Auth0 (вход через Яндекс) |
| Экспорт | openpyxl (Excel), reportlab (PDF) |
| Уведомления | python-telegram-bot |

---

## Возможности

- **Сделки** — канбан-доска с drag & drop, карточки с услугами, скидкой, налогом
- **Контакты** — карточка клиента с раздельными полями ФИО и Организация, история сделок и KPI
- **Задачи** — список с приоритетами, назначением на сотрудника и сроками
- **Расходы** — учёт по категориям с фильтром по году
- **Техника** — учёт оборудования и история ТО (расходники в мл с авто-конвертацией из л)
- **Склад** — остатки расходных материалов
- **Прайс-лист** — услуги с единицами измерения и ценами
- **Налог** — расчёт налога по ставке с режимами «включён» / «сверху»
- **Аналитика** — воронка, динамика выручки/расходов, топ услуг, выручка по сотрудникам (только Admin)
- **Бюджет** — планирование по периодам с процентом исполнения (только Admin)
- **Экспорт** — отчёт в Excel (4 листа) и PDF за выбранный год
- **AI-агент** — в разделе «Сервис» можно отправить запрос в OpenAI-совместимый API с контекстом CRM
- **Роли** — Admin видит всё, User видит только свои сделки и задачи

---

## Структура проекта

```
GCRM-2/
├── main.py            # FastAPI-приложение (v13+)
├── index.html         # Фронтенд (SPA, без фреймворков)
├── bot.py             # Telegram-бот с отчётами и созданием сделок
├── assistant_bot.py   # Личный AI-ассистент владельца (чеки, сделки, задачи, фото)
├── client_bot.py      # Клиентский AI-бот (отключён, сохранён для будущего)
├── deploy.sh          # Скрипт деплоя (git pull + перезапуск сервисов)
├── backup.sh          # Скрипт резервного копирования БД
├── start.sh           # Запуск uvicorn для systemd
├── requirements.txt   # Зависимости Python
├── .env               # Секреты (не коммитить!)
└── backups/           # Дампы БД (не коммитить!)
```

---

## Быстрый старт (локально)

```bash
git clone <URL> && cd GCRM-2
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # заполнить переменные
uvicorn main:app --reload --port 8000
```

Swagger UI: `http://127.0.0.1:8000/docs`

### Переменные окружения (`.env`)

```
DATABASE_URL=postgresql://user:password@host:5432/dbname
AUTH0_DOMAIN=your-domain.auth0.com
AUTH0_CLIENT_ID=...
AUTH0_CLIENT_SECRET=...
AUTH0_AUDIENCE=...
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_ACCESS_ID=...
OPENAI_MODEL=gpt-4o-mini
OPENAI_VISION_MODEL=gpt-4o          # для распознавания чеков в assistant_bot
APP_BASE_URL=http://localhost:8000
SESSION_SECRET=случайная-строка
INTERNAL_API_KEY=случайная-строка   # для бота и внутренних вызовов
TELEGRAM_BOT_TOKEN=...              # основной бот (bot.py)
TELEGRAM_CHAT_ID=...                # chat_id владельца (для отчётов и assistant_bot)
TELEGRAM_ASSISTANT_BOT_TOKEN=...    # личный AI-ассистент (assistant_bot.py)
TELEGRAM_CLIENT_BOT_TOKEN=...       # клиентский бот (отключён, хранится для будущего)
```

---

## Деплой на сервере

### Параметры

| | |
|---|---|
| Сервер | VPS Timeweb Cloud, Ubuntu 24.04 |
| IP | 77.232.134.112 |
| Путь | `/var/www/crm/GCRM-2` |
| Venv | `/var/www/crm/venv` |
| Порт | 127.0.0.1:8000 (за nginx/proxy) |

### Деплой новой версии

```bash
cd /var/www/crm/GCRM-2 && ./deploy.sh
```

Скрипт делает: `git fetch + reset --hard origin/main → установка прав → pip install -r requirements.txt → перезапуск CRM и Telegram-бота → проверка статуса сервисов`.

> ℹ️ `git reset --hard` не удаляет `.env`, если он не отслеживается Git (обычный случай).  
> ✅ `deploy.sh` перезапускает **и CRM, и Telegram-бота**.

### systemd unit (`/etc/systemd/system/crm.service`)

```ini
[Unit]
Description=Grass CRM FastAPI
After=network.target

[Service]
User=root
WorkingDirectory=/var/www/crm/GCRM-2
EnvironmentFile=/var/www/crm/GCRM-2/.env
ExecStart=/var/www/crm/GCRM-2/start.sh
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
```

После изменения файла: `systemctl daemon-reload`

---

## Роли пользователей

Роль задаётся вручную в таблице `users` в Supabase (колонка `role`).

| Роль | Сделки | Задачи | Расходы | Аналитика | Бюджет |
|---|---|---|---|---|---|
| `Admin` | Все | Все | Полный доступ | ✅ | ✅ |
| `User` | Только свои | Только свои | Скрыты | ❌ | ❌ |

---

## Telegram-боты

### Основной бот (`bot.py`)

Бот для команды: создание сделок через диалог, ежедневные отчёты, выезды на день.

Запускается через systemd (`greencrm-bot.service`).

```bash
systemctl status greencrm-bot
```

#### Команды

| Команда | Описание |
|---|---|
| `/newdeal` | Создать сделку (3 шага: название → контакт → услуги) |
| `/newexpense` | Записать расход (категория → название → сумма) |
| `/today` | Выезды на сегодня |
| `/tomorrow` | Выезды на завтра |
| `/sendreport` | Отправить отчёт вручную |

---

### Личный AI-ассистент (`assistant_bot.py`)

Личный бот владельца. Работает только с авторизованными `TELEGRAM_CHAT_ID`. Запускается через systemd (`greencrm-assistant-bot.service`).

```bash
systemctl status greencrm-assistant-bot
```

#### Возможности

| Что отправить | Что сделает бот |
|---|---|
| Фото с подписью **«чек»** / «расход» | Распознаёт OCR → предлагает занести расход |
| Фото с подписью **«до»** / **«после»** | Прикрепляет к последней активной сделке |
| Фото без подписи | Спрашивает: чек, фото до или после? |
| Текст **«задача: ...»** | AI разбирает → создаёт задачу |
| Длинный текст / переписка с клиентом | AI извлекает данные → предлагает создать сделку |
| Голосовое сообщение | Расшифровывает (Whisper) → роутит как текст |

Всё с кнопками подтверждения — ничего не создаётся без явного ОК.

#### Установка (первый раз)

```bash
cp /etc/systemd/system/greencrm-assistant-bot.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable greencrm-assistant-bot
systemctl start greencrm-assistant-bot
```

> ℹ️ Для распознавания чеков используется vision-модель (`OPENAI_VISION_MODEL=gpt-4o`). Убедитесь, что ключ API имеет доступ к ней.

---

### Клиентский AI-бот (`client_bot.py`) — **отключён**

Бот для диалога с входящими клиентами. Собирал данные по заявке через OpenAI и создавал лид в CRM. Сохранён на сервере для возможного возобновления.

Активация при необходимости:
```bash
systemctl enable greencrm-client-bot
systemctl start greencrm-client-bot
```

---

### Ответственный по Telegram

Бот автоматически определяет сотрудника, который пишет, и проставляет его как ответственного в сделке. Для этого нужно привязать Telegram ID каждого сотрудника в панели управления CRM → раздел «Пользователи системы».

Как получить Telegram ID: написать боту [@userinfobot](https://t.me/userinfobot).

### Авто-отчёт

Время и статус (вкл/выкл) настраиваются в панели управления CRM → блок «Telegram». По умолчанию: ежедневно в 18:00 МСК.

> ⚠️ Настройки хранятся **в памяти процесса** — при перезапуске бота сбрасываются на 18:00.

---

## Резервное копирование

### Автоматически

Через cron в 03:00. Хранятся последние 2 дампа (скрипт `backup.sh`).

```
/var/www/crm/GCRM-2/backups/backup-YYYY-MM-DD.sql.gz
```

### Вручную из CRM

Панель управления → блок «База данных» → кнопка **«💾 Создать бэкап сейчас»**.

- Сохраняет в `/var/www/crm/GCRM-2/backups/backup_YYYYMMDD_HHMMSS.sql.gz`
- Автоматически удаляет старые, оставляя последние **10 бэкапов**
- Требует `pg_dump`: `apt install postgresql-client`

### Вручную из консоли

```bash
/var/www/crm/GCRM-2/backup.sh
```

### Восстановление

```bash
gunzip < backups/backup-2026-03-06.sql.gz | psql -d "$DATABASE_URL"
```

> ⚠️ Восстановление **перезаписывает** текущую БД.

---

## Схема БД — миграции

Применить вручную в Supabase SQL Editor:

```sql
-- Контакты: разбивка имени и поле организации
ALTER TABLE contacts
  ADD COLUMN IF NOT EXISTS lastname   VARCHAR(100),
  ADD COLUMN IF NOT EXISTS middlename VARCHAR(100),
  ADD COLUMN IF NOT EXISTS org        VARCHAR(200);

-- Пользователи: привязка Telegram
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS telegram_user_id VARCHAR(50) UNIQUE;
```

---

## API — ключевые эндпоинты

### Контакты

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/api/contacts` | Список контактов (включает `lastname`, `middlename`, `org`) |
| `POST` | `/api/contacts` | Создать контакт |
| `PATCH` | `/api/contacts/{id}` | Обновить контакт |

### Пользователи и Telegram

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/api/users` | Список пользователей (включает `telegram_id`) |
| `PATCH` | `/api/users/{user_id}/telegram` | Привязать/отвязать Telegram ID к пользователю (Admin) |
| `GET` | `/api/users/by-telegram/{telegram_id}` | Найти пользователя CRM по Telegram ID (для бота) |

### Сервис и бот

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/api/service/status` | Статус системы (CPU, RAM, диск, кэш, бэкап) |
| `POST` | `/api/service/backup` | Создать бэкап БД прямо сейчас (Admin) |
| `GET` | `/api/service/bot/schedule` | Получить настройки авто-отчёта |
| `POST` | `/api/service/bot/schedule` | Включить/выключить авто-отчёт, задать время |
| `POST` | `/api/service/bot/report` | Отправить отчёт в Telegram вручную |

### Аналитика

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/api/analytics?year=YYYY` | Полная аналитика, включает `manager_stats` — выручка по сотрудникам |

- синхронизирует ветку с `main` через `./sync_pr.sh`;
- если установлен и авторизован `gh`, автоматически создаёт PR (если его нет);
- снимает режим Draft (`Ready for review`);
- показывает `mergeStateStatus`, чтобы сразу увидеть, есть ли конфликт.

