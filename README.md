# GrassCRM

CRM-система для компании по покосу газонов и ландшафтным работам. Бэкенд на FastAPI, фронтенд — одностраничное приложение на чистом JS. Развёрнута на VPS под Ubuntu 24.04, данные хранятся в PostgreSQL (Supabase).

---

## Стек

| Слой | Технология |
|---|---|
| Бэкенд | Python 3.12, FastAPI, SQLAlchemy |
| Веб-сервер | Uvicorn + systemd |
| База данных | PostgreSQL (Supabase, Session Pooler) + pgvector 0.8.0 |
| Аутентификация | Auth0 (вход через Яндекс) |
| Экспорт | openpyxl (Excel), reportlab (PDF) |
| Уведомления | python-telegram-bot |
| AI | DeepSeek (`deepseek-chat`) через OpenAI-совместимый API (Timeweb Cloud AI) |
| Парсинг файлов | pdfplumber, openpyxl, python-docx |

---

## Возможности

- **Сделки** — канбан-доска с drag & drop, карточки с услугами, скидкой (% или фиксированная), налогом, фото до/после, файлами, комментариями и **AI-панелью**
- **Контакты** — карточка клиента с раздельными полями ФИО и Организация, история сделок и KPI
- **Задачи** — список с приоритетами, назначением на сотрудника и сроками
- **Расходы** — учёт по категориям с фильтром по году
- **Техника** — учёт оборудования и история ТО (расходники в мл с авто-конвертацией из л)
- **Склад** — остатки расходных материалов
- **Прайс-лист** — услуги с единицами измерения и ценами
- **Налог** — расчёт налога по ставке с режимами «включён» / «сверху»
- **Аналитика** — воронка, динамика выручки/расходов, топ услуг, выручка по сотрудникам + **AI-разбор** (только Admin)
- **Бюджет** — планирование по периодам с процентом исполнения (только Admin)
- **Экспорт** — отчёт в Excel (4 листа) и PDF за выбранный год
- **Заметки** — свободные текстовые записи
- **AI-агент** — встроен в карточку сделки, раздел «Аналитика» и раздел «Сервис»
- **Роли** — Admin видит всё, User видит только свои сделки и задачи

---

## Структура проекта

```
GCRM-2/
├── main.py            # FastAPI-приложение (v1.2)
├── index.html         # Фронтенд (SPA, без фреймворков)
├── bot.py             # Telegram-бот с отчётами и созданием сделок (v1.3)
├── assistant_bot.py   # Личный AI-агент владельца (v2.1)
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
APP_BASE_URL=http://localhost:8000
SESSION_SECRET=случайная-строка
INTERNAL_API_KEY=случайная-строка       # для бота и внутренних вызовов
OPENAI_BASE_URL=https://...             # OpenAI-совместимый эндпоинт
OPENAI_ACCESS_ID=...                    # ID агента / ключ
OPENAI_MODEL=deepseek-chat
API_BASE_URL=http://127.0.0.1:8000/api  # используется assistant_bot
TELEGRAM_BOT_TOKEN=...                  # основной бот (bot.py)
TELEGRAM_CHAT_ID=...                    # chat_id группы для отчётов и уведомлений о сделках
TELEGRAM_ASSISTANT_BOT_TOKEN=...        # личный AI-ассистент (assistant_bot.py)
```

---

## Деплой на сервере

### Параметры

| | |
|---|---|
| Сервер | VPS Timeweb Cloud, Ubuntu 24.04 |
| IP |  |
| Путь | `/var/www/crm/GCRM-2` |
| Venv | `/var/www/crm/venv` |
| Порт | 127.0.0.1:8000 (за nginx/proxy) |
| Домен CRM |  |
| Домен сайта |  → `/var/www/site` |

### Деплой новой версии

```bash
cd /var/www/crm/GCRM-2
git add -A && git commit -m "описание изменений"
git push
git fetch --all && git reset --hard origin/main
systemctl restart crm greencrm-bot greencrm-assistant-bot
```

> ℹ️ `git reset --hard` не удаляет `.env`, если он не отслеживается Git.

### systemd сервисы

| Сервис | Файл | Описание |
|---|---|---|
| `crm` | `crm.service` | FastAPI бэкенд (main.py) |
| `greencrm-bot` | `greencrm-bot.service` | Основной Telegram-бот (bot.py) |
| `greencrm-assistant-bot` | `greencrm-assistant-bot.service` | AI-ассистент (assistant_bot.py) |

```bash
systemctl status crm
systemctl status greencrm-bot
systemctl status greencrm-assistant-bot
```

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

### nginx

Конфиги в `/etc/nginx/sites-enabled/`:

| Файл | Домен | Описание |
|---|---|---|
| (основной) | crmpokos.ru | HTTPS → FastAPI + статика CRM |
| `pokos-ropsha` | покос-ропша.рф | HTTPS → `/var/www/site` (лендинг) |

SSL-сертификаты Let's Encrypt, авто-обновление через certbot.

---

## Роли пользователей

Роль задаётся вручную в таблице `users` в Supabase (колонка `role`).

| Роль | Сделки | Задачи | Расходы | Аналитика | Бюджет |
|---|---|---|---|---|---|
| `Admin` | Все | Все | Полный доступ | ✅ | ✅ |
| `User` | Только свои | Только свои | Скрыты | ❌ | ❌ |

---

## Telegram-боты

### Основной бот (`bot.py`) v1.3

Бот для команды: создание сделок через диалог, ежедневные отчёты, выезды на день, уведомления о новых сделках в группу.

Запускается через systemd (`greencrm-bot.service`).

```bash
systemctl status greencrm-bot
```

#### Команды

| Команда | Описание |
|---|---|
| `/newdeal` | Создать сделку (название → контакт → услуги) |
| `/newexpense` | Записать расход (категория → название → сумма) |
| `/today` | Выезды на сегодня |
| `/tomorrow` | Выезды на завтра |
| `/sendreport` | Отправить отчёт вручную |
| `/mydeals` | Мои сделки на сегодня |
| `/editlastdeal` | Редактировать последнюю сделку |
| `/voicenote` | Голосовая заметка к сделке |
| `/status` | Статус всех сервисов (только владелец) |

#### Уведомления о сделках

При создании сделки через **любой источник** (бот, AI-ассистент) в группу отправляется уведомление единого формата:

```
Сделка №N — Название сделки

ДД.ММ.ГГГГ ЧЧ:ММ
Клиент: Имя
Телефон: +7...
Адрес: ...

Услуги:
  · Название × кол-во — сумма ₽

Итого: сумма ₽
```

#### Startup-уведомление

При каждом запуске бота владелец получает в личку сообщение с подтверждением что сервер и CRM API работают.

---

### Личный AI-агент (`assistant_bot.py`) v2.1

Личный бот владельца. Работает только с авторизованными `TELEGRAM_CHAT_ID`. Запускается через systemd (`greencrm-assistant-bot.service`).

```bash
systemctl status greencrm-assistant-bot
```

#### Архитектура (v2.1)

Все сообщения проходят через единый **tool router** — AI выбирает инструмент и возвращает структурированный JSON.

Изменения v2.1:
- При создании сделки отправляет уведомление в группу через основного бота — единый формат с ценами, итогом, телефоном

#### Возможности

| Что отправить | Что сделает бот |
|---|---|
| Любой текстовый запрос | Tool router → нужное действие или ответ |
| «Создай сделку и задачу позвонить завтра» | Цепочка: сделка + задача за один запрос |
| «Сколько стоит покос?» | Открывает прайс-лист с ценами |
| Голосовое сообщение | Расшифровывает (Whisper) → обрабатывает как текст |
| Фото с подписью **«чек»** / «расход» | OCR → предлагает занести расход |
| Фото с подписью **«до»** / **«после»** | Прикрепляет к последней активной сделке |
| Фото без подписи | Спрашивает: чек, фото до или после? |
| **PDF / Word / Excel / TXT** | Извлекает текст → анализирует → предлагает действия |

#### Доступные инструменты (tools)

| Инструмент | Что делает |
|---|---|
| `create_task` | Создать задачу |
| `create_deal` | Создать сделку с матчингом услуг по прайсу |
| `create_expense` | Записать расход |
| `create_contact` | Добавить контакт |
| `create_note` | Сохранить заметку |
| `get_tasks` | Показать задачи (фильтр: today/tomorrow/overdue/all) |
| `get_deals` | Показать сделки (фильтр: active/all) |
| `get_expenses` | Показать расходы (фильтр: today/week/month) |
| `get_contacts` | Найти контакты по имени/телефону |
| `get_services` | Показать прайс-лист с ценами |
| `get_stats` | Статистика бизнеса (выручка, прибыль, сделки) |
| `get_report` | Сводный отчёт за период |
| `change_status` | Сменить стадию сделки |
| `add_comment` | Добавить комментарий к сделке |
| `answer` | Ответить на вопрос без создания записей |

#### Бизнес-правила при создании сделок

Вывоз травы и сбор скошенной травы автоматически получают то же количество (в сотках), что и покос. Задаётся в `SERVICE_AUTO_ADD_RULES` и `SERVICE_QUANTITY_INHERIT`.

#### Команды ассистента

| Команда | Описание |
|---|---|
| `/forget` | Очистить память ассистента |
| `/checkup` | Ручной запуск proactive-анализа CRM |

#### Память (ai_memory)

Ассистент помнит контекст разговора и факты о бизнесе. Память хранится в таблице `ai_memory` в PostgreSQL:

- **message** — история диалога (последние 10 сообщений)
- **fact** — извлечённые факты (клиенты, адреса, особенности объектов)

#### Proactive AI (автоматические проверки)

| Время | Что проверяет |
|---|---|
| 09:00 МСК | Просроченные задачи, выезды на сегодня и завтра |
| 19:00 МСК | Итоги дня: активные сделки, расходы/выручка за месяц |

Если проблем нет — бот молчит.

#### Парсинг документов

| Формат | Библиотека | Ограничение |
|---|---|---|
| PDF | pdfplumber | до 10 страниц |
| Excel (.xlsx/.xls) | openpyxl | до 3 листов × 200 строк |
| Word (.docx/.doc) | python-docx | все параграфы |
| TXT / CSV | встроенный decode | UTF-8 → CP1251 fallback |

---

### Клиентский AI-бот (`client_bot.py`) — **отключён**

Сохранён для возможного возобновления.

```bash
systemctl enable greencrm-client-bot
systemctl start greencrm-client-bot
```

---

### Ответственный по Telegram

Бот определяет сотрудника по Telegram ID и проставляет его как ответственного. Привязка: CRM → «Пользователи» → поле Telegram ID.

Как получить Telegram ID: [@userinfobot](https://t.me/userinfobot)

### Авто-отчёт

Время и статус настраиваются в панели CRM → блок «Telegram». По умолчанию: ежедневно в 18:00 МСК.

> ⚠️ Настройки хранятся **в памяти процесса** — при перезапуске бота сбрасываются на 18:00.

---

## AI внутри CRM (веб-интерфейс)

### AI-панель в карточке сделки

При открытии любой существующей сделки в нижней части карточки появляется AI-панель.

**Быстрые вопросы (чипы):**
- 💡 Рекомендации — что можно улучшить по сделке
- 📋 Итог сделки — краткое резюме в 2-3 предложения
- ✅ Предложи задачи — что нужно сделать дальше
- 💬 Сообщение клиенту — готовый текст для отправки
- 📝 Добавить итог в комментарий — одним кликом генерирует и сохраняет

**После ответа AI — кнопки действий:**
- 💬 Сохранить как комментарий к сделке
- ✅ Создать задачу (первая строка ответа → заголовок)

### AI-разбор аналитики

В разделе **Аналитика** → кнопка **✨ Проанализировать** внизу страницы.

Эндпоинт: `POST /api/analytics/ai-insight?year=YYYY`

### AI в разделе «Сервис» (Admin)

Произвольный запрос к AI с полным контекстом CRM.

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

-- Память AI-ассистента
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS ai_memory (
    id          BIGSERIAL PRIMARY KEY,
    chat_id     VARCHAR(50)  NOT NULL,
    role        VARCHAR(20)  NOT NULL DEFAULT 'user',
    content     TEXT         NOT NULL,
    memory_type VARCHAR(20)  DEFAULT 'message',
    importance  SMALLINT     DEFAULT 1,
    metadata    JSONB        DEFAULT '{}',
    created_at  TIMESTAMPTZ  DEFAULT NOW(),
    expires_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_ai_memory_chat_id ON ai_memory(chat_id);
CREATE INDEX IF NOT EXISTS idx_ai_memory_type    ON ai_memory(chat_id, memory_type);
```

---

## API — ключевые эндпоинты

### Сделки

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/api/deals` | Список сделок |
| `POST` | `/api/deals` | Создать сделку (триггерит уведомление в группу) |
| `PATCH` | `/api/deals/{id}` | Обновить сделку |
| `DELETE` | `/api/deals/{id}` | Удалить сделку |

### Контакты

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/api/contacts` | Список контактов |
| `POST` | `/api/contacts` | Создать контакт |
| `PATCH` | `/api/contacts/{id}` | Обновить контакт |

### Пользователи и Telegram

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/api/users` | Список пользователей |
| `PATCH` | `/api/users/{user_id}/telegram` | Привязать Telegram ID (Admin) |
| `GET` | `/api/users/by-telegram/{telegram_id}` | Найти пользователя по Telegram ID |

### AI

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/api/service/ai/ask` | Запрос к AI с контекстом CRM |
| `POST` | `/api/service/ai/action` | Выполнить действие CRM |
| `POST` | `/api/analytics/ai-insight?year=YYYY` | AI-разбор аналитики за год |
| `POST` | `/api/service/ai/memory` | Сохранить факт в память |
| `GET` | `/api/service/ai/memory` | Поиск по памяти |
| `DELETE` | `/api/service/ai/memory` | Очистить память |

### Сервис и бот

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/api/service/status` | Статус системы (CPU, RAM, диск, кэш, бэкап) |
| `POST` | `/api/service/backup` | Создать бэкап БД (Admin) |
| `GET` | `/api/service/bot/schedule` | Настройки авто-отчёта |
| `POST` | `/api/service/bot/schedule` | Включить/выключить авто-отчёт |
| `POST` | `/api/service/bot/report` | Отправить отчёт вручную |

---

## Резервное копирование

### Автоматически

Через cron в 03:00. Хранятся последние 2 дампа.

```
/var/www/crm/GCRM-2/backups/backup-YYYY-MM-DD.sql.gz
```

### Вручную из CRM

Панель управления → «База данных» → **«💾 Создать бэкап сейчас»**. Хранит последние 10 бэкапов.

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
