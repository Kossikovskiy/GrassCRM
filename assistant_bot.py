# -*- coding: utf-8 -*-
"""
GrassCRM Assistant Bot — личный AI-агент владельца. v2.1

Что умеет:
  1. 🤖 Любой текстовый запрос → tool system (создание сделок/задач/расходов, ответы на вопросы)
  2. 🔗 Цепочки действий — "создай сделку и задачу" выполняется за один запрос
  3. 🧠 Память — помнит факты и историю разговора (ai_memory)
  4. 🧾 Фото чека → OCR → расход в CRM
  5. 📸 Фото ДО/ПОСЛЕ → привязывает к последней активной сделке
  6. 🎤 Голосовые сообщения → Whisper → обрабатывает как текст
  7. 📄 Документы (PDF/Word/Excel/TXT) → анализ, создание сделок/задач
  8. 📊 Proactive AI — утром и вечером сам пишет если есть проблемы

Использует: TELEGRAM_ASSISTANT_BOT_TOKEN
Работает только с авторизованными пользователями (TELEGRAM_CHAT_ID).
"""

import asyncio
import json
import logging
import os
import re
import tempfile
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import httpx
import io
from dotenv import load_dotenv

# faster-whisper: локальная расшифровка голоса
try:
    from faster_whisper import WhisperModel as _WhisperModel
    _whisper_instance = None
    FASTER_WHISPER_OK = True
except ImportError:
    FASTER_WHISPER_OK = False
    _whisper_instance = None
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("assistant_bot")

# ════════════════════════════════════════
#  ⚙️  КОНФИГ
# ════════════════════════════════════════

ASSISTANT_BOT_TOKEN = os.getenv("TELEGRAM_ASSISTANT_BOT_TOKEN")
INTERNAL_API_KEY    = os.getenv("INTERNAL_API_KEY")
API_BASE_URL        = os.getenv("API_BASE_URL", "http://127.0.0.1:8000/api")
MAIN_BOT_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN")        # основной бот — для уведомлений в группу
GROUP_CHAT_ID       = os.getenv("TELEGRAM_CHAT_ID", "").split(",")[0].strip()  # первый chat_id = группа

# Основной AI: DeepSeek (работает из России)
OPENAI_BASE_URL  = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com/v1")
OPENAI_ACCESS_ID = os.getenv("OPENAI_ACCESS_ID") or os.getenv("OPENAI_API_KEY")
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "deepseek-chat")

# Vision: DeepSeek тоже поддерживает картинки и работает из России
OPENAI_VISION_URL   = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com/v1")
OPENAI_VISION_KEY   = OPENAI_ACCESS_ID
OPENAI_VISION_MODEL = os.getenv("OPENAI_VISION_MODEL", "deepseek-chat")

# Whisper: локальный faster-whisper, не требует внешних API
# pip install faster-whisper  (скачает модель ~150 МБ при первом запуске)
WHISPER_MODEL_SIZE  = os.getenv("WHISPER_MODEL_SIZE", "small")

# Список chat_id, которым разрешено пользоваться ботом
_raw_allowed = os.getenv("TELEGRAM_CHAT_ID", "")
ALLOWED_CHAT_IDS: set = {
    int(x.strip()) for x in _raw_allowed.split(",") if x.strip().isdigit()
}

if not ASSISTANT_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_ASSISTANT_BOT_TOKEN не задан в .env")
if not INTERNAL_API_KEY:
    raise RuntimeError("INTERNAL_API_KEY не задан в .env")
if not OPENAI_ACCESS_ID:
    raise RuntimeError("OPENAI_ACCESS_ID / OPENAI_API_KEY не задан в .env")

API_HEADERS = {"X-Internal-API-Key": INTERNAL_API_KEY}

# ════════════════════════════════════════
#  🔒  АВТОРИЗАЦИЯ
# ════════════════════════════════════════

def _is_allowed(update: Update) -> bool:
    if not ALLOWED_CHAT_IDS:
        return True  # если не задан список — пропускаем всех (осторожно!)
    return update.effective_chat.id in ALLOWED_CHAT_IDS

async def _deny(update: Update):
    await update.effective_message.reply_text("⛔ Нет доступа.")

# ════════════════════════════════════════
#  🌐  CRM API HELPERS
# ════════════════════════════════════════

async def _crm_get(path: str) -> Any:
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{API_BASE_URL}{path}", headers=API_HEADERS)
        r.raise_for_status()
        return r.json()

async def _crm_post(path: str, payload: Dict) -> Any:
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(f"{API_BASE_URL}{path}", headers=API_HEADERS, json=payload)
        r.raise_for_status()
        return r.json() if r.text else {}

async def _crm_patch(path: str, payload: Dict) -> Any:
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.patch(f"{API_BASE_URL}{path}", headers=API_HEADERS, json=payload)
        r.raise_for_status()
        return r.json() if r.text else {}

async def _crm_upload_file(path: str, file_bytes: bytes, filename: str,
                            extra_fields: Dict[str, str] = None) -> Any:
    """Загрузить файл через multipart/form-data на /api/files."""
    import io
    data = httpx.AsyncClient()
    async with httpx.AsyncClient(timeout=30) as c:
        files = {"file": (filename, io.BytesIO(file_bytes), "image/jpeg")}
        form  = extra_fields or {}
        r = await c.post(
            f"{API_BASE_URL}{path}",
            headers=API_HEADERS,
            files=files,
            data=form,
        )
        r.raise_for_status()
        return r.json() if r.text else {}

# ════════════════════════════════════════
#  🤖  AI HELPERS
# ════════════════════════════════════════

async def _ask_ai_text(system: str, user: str) -> str:
    url = f"{OPENAI_BASE_URL.rstrip('/')}/chat/completions"
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
    }
    headers = {"Authorization": f"Bearer {OPENAI_ACCESS_ID}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(url, json=payload, headers=headers)
    if not r.is_success:
        raise RuntimeError(f"AI error {r.status_code}: {r.text[:300]}")
    return ((r.json().get("choices") or [{}])[0].get("message") or {}).get("content", "")


async def _ask_ai_vision(image_b64: str, prompt: str) -> str:
    """Запрос к vision-модели с base64-изображением. Всегда идёт на OpenAI (не DeepSeek)."""
    url = f"{OPENAI_VISION_URL.rstrip('/')}/chat/completions"
    payload = {
        "model": OPENAI_VISION_MODEL,
        "temperature": 0.1,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{image_b64}",
                            "detail": "high",
                        },
                    },
                ],
            }
        ],
    }
    headers = {"Authorization": f"Bearer {OPENAI_VISION_KEY}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(url, json=payload, headers=headers)
    if not r.is_success:
        raise RuntimeError(f"Vision AI error {r.status_code}: {r.text[:300]}")
    return ((r.json().get("choices") or [{}])[0].get("message") or {}).get("content", "")


def _get_whisper() -> "_WhisperModel":
    """Ленивая инициализация faster-whisper."""
    global _whisper_instance
    if _whisper_instance is None:
        logger.info(f"Загружаю Whisper '{WHISPER_MODEL_SIZE}'...")
        _whisper_instance = _WhisperModel(WHISPER_MODEL_SIZE, device="cpu", compute_type="int8")
        logger.info("Whisper готов.")
    return _whisper_instance


async def _transcribe_voice(file_bytes: bytes) -> str:
    """Расшифровать голосовое локально через faster-whisper."""
    if not FASTER_WHISPER_OK:
        raise RuntimeError(
            "faster-whisper не установлен.\n"
            "Выполните на сервере: pip install faster-whisper"
        )
    import asyncio as _aio
    def _run(data: bytes) -> str:
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
            f.write(data)
            path = f.name
        try:
            segs, _ = _get_whisper().transcribe(path, language="ru", beam_size=5)
            return " ".join(s.text for s in segs).strip()
        finally:
            try: os.remove(path)
            except: pass
    loop = _aio.get_event_loop()
    return await loop.run_in_executor(None, _run, file_bytes)


def _parse_json_safe(text: str) -> Optional[Dict]:
    text = (text or "").strip()
    # strip fenced code blocks
    text = re.sub(r"```(?:json)?\s*", "", text)
    text = text.replace("```", "")
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        return json.loads(text[start:end+1])
    except Exception:
        return None

# ════════════════════════════════════════
#  📋  SYSTEM PROMPTS
# ════════════════════════════════════════

RECEIPT_PROMPT = """
Ты OCR-ассистент. Тебе дано фото чека или квитанции.
Верни СТРОГО JSON без markdown-обёрток:
{
  "name": "название товара/услуги (строка)",
  "amount": число (только цифры, без валюты),
  "date": "YYYY-MM-DD или null если не разобрать",
  "category": "одна из: Топливо, Расходники, Оборудование, Транспорт, Прочее"
}
Если не можешь разобрать чек — верни {"error": "описание проблемы"}.
""".strip()

DEAL_FROM_CHAT_PROMPT = """
Ты CRM-ассистент компании по уходу за газонами и участками.
Тебе дана переписка с клиентом. Извлеки данные для создания сделки.
Верни СТРОГО JSON без markdown-обёрток:
{
  "contact_name": "имя клиента или null",
  "phone": "номер телефона в формате +7XXXXXXXXXX или null",
  "address": "адрес объекта или null",
  "title": "краткое название сделки (например: Покос 10 сот, Ивановка)",
  "notes": "детали: площадь, состояние газона, пожелания клиента",
  "services": [
    {"name": "название услуги как в переписке", "quantity": число_или_1}
  ]
}
Поле services — список упомянутых услуг. Quantity — количество или площадь (в сотках/м²) если упомянута, иначе 1.
Примеры услуг: покос, стрижка, вывоз травы, обработка, вертикуттирование, аэрация, подкормка.
Если данных недостаточно — всё равно верни JSON с тем, что есть (null для неизвестных полей, [] для services).
""".strip()

TASK_PROMPT = """
Ты CRM-ассистент. Тебе дан текст заметки или задачи.
Извлеки из него структурированную задачу. Верни СТРОГО JSON:
{
  "title": "краткий заголовок задачи",
  "description": "полное описание или null",
  "due_date": "YYYY-MM-DD или null",
  "priority": "Обычный или Высокий или Критический",
  "contact_name": "имя контакта если упомянут или null"
}
""".strip()

# ════════════════════════════════════════
#  🤖  TOOL SYSTEM (AI Agent)
# ════════════════════════════════════════

TOOL_PROMPT_TEMPLATE = """
Ты — AI-агент CRM-системы газонокосилочного бизнеса GrassCRM.
Сегодня: {today}. Завтра: {tomorrow}.
Тебе доступны инструменты (tools). Верни СТРОГО JSON без markdown.

Если нужно одно действие:
{
  "action": "название_инструмента",
  "data": { ... },
  "reply": "короткий ответ пользователю на русском"
}

Если нужно несколько действий (например: создать сделку И задачу):
{
  "reply": "создаю сделку и задачу",
  "actions": [
    {"action": "create_deal", "data": {...}, "reply": ""},
    {"action": "create_task", "data": {...}, "reply": ""}
  ]
}

Доступные инструменты:

create_task — создать задачу
  data: {"title": str, "description": str|null, "due_date": "YYYY-MM-DD"|null, "priority": "Обычный|Высокий|Критический", "contact_name": str|null}

create_deal — создать сделку/заявку
  data: {"contact_name": str, "phone": str|null, "address": str|null, "title": str, "notes": str|null}

create_expense — создать расход
  data: {"name": str, "amount": float, "date": "YYYY-MM-DD"|null, "category": "Топливо|Расходники|Оборудование|Транспорт|Прочее"}

create_contact — добавить нового клиента/контакт
  data: {"name": str, "phone": str|null, "email": str|null, "notes": str|null}

create_note — сохранить заметку (просто текст, не задача)
  data: {"text": str}

get_tasks — показать задачи
  data: {"filter": "today|tomorrow|all|overdue"}

get_deals — показать сделки
  data: {"filter": "active|all"}

get_expenses — показать расходы
  data: {"period": "today|week|month"}

get_contacts — найти/показать контакты
  data: {"query": "имя или телефон или пусто для всех"}

get_stats — статистика бизнеса (прибыль, выручка, сделки)
  data: {}

get_report — сводный отчёт за период
  data: {"period": "day|week|month"}

get_services — показать прайс-лист услуг с ценами
  data: {}

change_status — сменить стадию/статус сделки
  data: {"deal_id": int, "stage_name": "название стадии"}

add_comment — добавить комментарий к сделке
  data: {"deal_id": int, "text": "текст комментария"}

answer — просто ответить на вопрос (если не нужно создавать/получать данные)
  data: {}

Правила выбора action:
- сделать/напомни/звонок/встреча → create_task
- клиент/заявка/покос/адрес → create_deal
- потратил/купил/заправил/расход → create_expense
- добавь клиента/новый клиент/телефон → create_contact
- запомни/заметка/пометка → create_note
- покажи задачи → get_tasks
- покажи сделки → get_deals
- покажи расходы → get_expenses
- найди клиента/покажи клиентов → get_contacts
- статистика/прибыль/аналитика → get_stats
- отчёт/итоги/сводка → get_report
- прайс/услуги/сколько стоит/цена → get_services
- иначе → answer

Примеры:
"Позвони Иванову завтра" → {"action":"create_task","data":{"title":"Позвонить Иванову","due_date":null,"priority":"Обычный","contact_name":"Иванов","description":null},"reply":"Создаю задачу: позвонить Иванову завтра"}
"Клиент из Ропши 15 соток" → {"action":"create_deal","data":{"contact_name":"Неизвестный","phone":null,"address":"Ропша","title":"Покос 15 сот, Ропша","notes":"15 соток"},"reply":"Создаю заявку..."}
"Заправил на 2500р" → {"action":"create_expense","data":{"name":"Заправка","amount":2500,"date":null,"category":"Топливо"},"reply":"Записываю расход 2500₽"}
"Добавь Петрова +79001234567" → {"action":"create_contact","data":{"name":"Петров","phone":"+79001234567","email":null,"notes":null},"reply":"Добавляю контакт Петров"}
"Запомни: забрать ключи у Сидорова" → {"action":"create_note","data":{"text":"Забрать ключи у Сидорова"},"reply":"Заметка сохранена"}
"Покажи задачи на сегодня" → {"action":"get_tasks","data":{"filter":"today"},"reply":"Загружаю задачи на сегодня..."}
"Найди клиента Смирнов" → {"action":"get_contacts","data":{"query":"Смирнов"},"reply":"Ищу клиента..."}
"Какая прибыль за месяц?" → {"action":"get_stats","data":{},"reply":"Считаю статистику..."}
"Покажи отчёт за неделю" → {"action":"get_report","data":{"period":"week"},"reply":"Формирую отчёт за неделю..."}
"Переведи сделку 5 в Выполнено" → {"action":"change_status","data":{"deal_id":5,"stage_name":"Выполнено"},"reply":"Меняю стадию..."}
"Добавь комментарий к сделке 3: клиент перезвонит" → {"action":"add_comment","data":{"deal_id":3,"text":"Клиент перезвонит"},"reply":"Добавляю комментарий..."}
"Сколько стоит покос?" → {"action":"get_services","data":{},"reply":"Смотрю прайс-лист..."}
"Есть ли услуга вертикуттирование?" → {"action":"get_services","data":{},"reply":"Проверяю услуги..."}
"Создай сделку с Ивановым и задачу позвонить ему завтра" → {"reply":"Создаю сделку и задачу","actions":[{"action":"create_deal","data":{"contact_name":"Иванов","title":"Заявка от Иванова","address":null,"notes":null},"reply":""},{"action":"create_task","data":{"title":"Позвонить Иванову","due_date":"{tomorrow}","priority":"Обычный"},"reply":""}]}
""".strip()


def _get_tool_prompt() -> str:
    """Возвращает TOOL_PROMPT с подставленной текущей датой."""
    from datetime import timedelta
    today = date.today()
    tomorrow = today + timedelta(days=1)
    return TOOL_PROMPT_TEMPLATE.format(
        today=today.strftime("%d.%m.%Y (%A)").replace(
            "Monday","пн").replace("Tuesday","вт").replace("Wednesday","ср")
            .replace("Thursday","чт").replace("Friday","пт")
            .replace("Saturday","сб").replace("Sunday","вс"),
        tomorrow=tomorrow.strftime("%d.%m.%Y"),
    )


# ── Tool executor ──────────────────────────────────────────────────────────────

async def _tool_get_tasks(data: Dict) -> str:
    """Получить задачи из CRM и отформатировать."""
    try:
        tasks = await _crm_get("/tasks")
        if not tasks:
            return "📋 Задач нет."

        filter_by = (data.get("filter") or "all").lower()
        today_str = date.today().isoformat()
        tomorrow_str = (date.today().replace(day=date.today().day + 1)).isoformat() \
            if date.today().day < 28 else date.today().isoformat()

        try:
            from datetime import timedelta
            tomorrow_str = (date.today() + timedelta(days=1)).isoformat()
        except Exception:
            pass

        filtered = []
        for t in tasks:
            due = (t.get("due_date") or "")[:10]
            status = (t.get("status") or "").lower()
            if status in ("done", "завершена", "выполнена"):
                continue
            if filter_by == "today" and due != today_str:
                continue
            if filter_by == "tomorrow" and due != tomorrow_str:
                continue
            if filter_by == "overdue" and (not due or due >= today_str):
                continue
            filtered.append(t)

        if not filtered:
            labels = {"today": "на сегодня", "tomorrow": "на завтра",
                      "overdue": "просроченных", "all": ""}
            return f"📋 Задач {labels.get(filter_by, '')} нет."

        lines = [f"📋 <b>Задачи ({len(filtered)}):</b>"]
        for t in filtered[:10]:
            due = (t.get("due_date") or "")[:10] or "—"
            pri = t.get("priority") or ""
            icon = "🔴" if pri == "Критический" else ("🟡" if pri == "Высокий" else "⚪")
            lines.append(f"{icon} <b>{t.get('title','?')}</b> · {due}")
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"get_tasks error: {e}")
        return f"❌ Не удалось получить задачи: {e}"


async def _tool_get_deals(data: Dict) -> str:
    """Получить сделки из CRM."""
    try:
        deals = await _crm_get("/deals")
        if not deals:
            return "💬 Сделок нет."

        filter_by = (data.get("filter") or "active").lower()
        if filter_by == "active":
            deals = [d for d in deals if not d.get("is_closed")]

        if not deals:
            return "💬 Активных сделок нет."

        lines = [f"💬 <b>Сделки ({min(len(deals),10)} из {len(deals)}):</b>"]
        for d in deals[:10]:
            title = d.get("title") or "Без названия"
            total = d.get("total") or 0
            lines.append(f"• <b>{title}</b> · {total:,.0f} ₽")
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"get_deals error: {e}")
        return f"❌ Не удалось получить сделки: {e}"


async def _tool_get_expenses(data: Dict) -> str:
    """Получить расходы из CRM."""
    try:
        expenses = await _crm_get("/expenses?year=2026")
        if not expenses:
            return "💸 Расходов нет."

        period = (data.get("period") or "month").lower()
        today = date.today()
        if period == "today":
            cutoff = today.isoformat()
            expenses = [e for e in expenses if (e.get("date") or "")[:10] == cutoff]
        elif period == "week":
            from datetime import timedelta
            cutoff = (today - timedelta(days=7)).isoformat()
            expenses = [e for e in expenses if (e.get("date") or "")[:10] >= cutoff]
        else:  # month
            cutoff = today.strftime("%Y-%m")
            expenses = [e for e in expenses if (e.get("date") or "")[:7] == cutoff]

        if not expenses:
            return f"💸 Расходов за период «{period}» нет."

        total = sum(float(e.get("amount") or 0) for e in expenses)
        lines = [f"💸 <b>Расходы · {total:,.0f} ₽</b>"]
        for e in expenses[:8]:
            name = e.get("name") or e.get("description") or "—"
            amt = float(e.get("amount") or 0)
            lines.append(f"• {name} · <b>{amt:,.0f} ₽</b>")
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"get_expenses error: {e}")
        return f"❌ Не удалось получить расходы: {e}"


async def _tool_get_stats() -> str:
    """Получить краткую статистику из CRM."""
    try:
        deals = await _crm_get("/deals")
        expenses = await _crm_get("/expenses?year=2026")
        tasks = await _crm_get("/tasks")

        today = date.today()
        month_prefix = today.strftime("%Y-%m")

        month_expenses = [e for e in (expenses or [])
                          if (e.get("date") or "")[:7] == month_prefix]
        total_exp = sum(float(e.get("amount") or 0) for e in month_expenses)

        active_deals = [d for d in (deals or []) if not d.get("is_closed")]
        won_deals = [d for d in (deals or []) if d.get("is_closed") and d.get("total")]
        revenue = sum(float(d.get("total") or 0) for d in won_deals)

        open_tasks = [t for t in (tasks or [])
                      if (t.get("status") or "").lower() not in ("done", "завершена", "выполнена")]

        return (
            f"📊 <b>Статистика GrassCRM</b>\n\n"
            f"💬 Активных сделок: <b>{len(active_deals)}</b>\n"
            f"✅ Завершённых сделок: <b>{len(won_deals)}</b>\n"
            f"💰 Выручка (всего): <b>{revenue:,.0f} ₽</b>\n"
            f"💸 Расходы за {today.strftime('%B')}: <b>{total_exp:,.0f} ₽</b>\n"
            f"📋 Открытых задач: <b>{len(open_tasks)}</b>"
        )
    except Exception as e:
        logger.warning(f"get_stats error: {e}")
        return f"❌ Не удалось получить статистику: {e}"


async def _tool_get_contacts(query: str) -> str:
    """Найти контакты в CRM."""
    try:
        contacts = await _crm_get("/contacts")
        if not contacts:
            return "👤 Контактов нет."

        if query:
            q = query.lower()
            contacts = [
                c for c in contacts
                if q in (c.get("name") or "").lower()
                or q in (c.get("phone") or "").lower()
                or q in (c.get("email") or "").lower()
            ]

        if not contacts:
            return f"👤 Контакты по запросу «{query}» не найдены."

        lines = [f"👤 <b>Контакты ({min(len(contacts), 10)} из {len(contacts)}):</b>"]
        for c in contacts[:10]:
            name = c.get("name") or "—"
            phone = c.get("phone") or ""
            phone_str = f" · {phone}" if phone else ""
            lines.append(f"• <b>{name}</b>{phone_str}")
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"get_contacts error: {e}")
        return f"❌ Не удалось получить контакты: {e}"


async def _tool_get_report(period: str) -> str:
    """Сводный отчёт за период."""
    try:
        from datetime import timedelta
        deals = await _crm_get("/deals")
        expenses = await _crm_get("/expenses?year=2026")
        tasks = await _crm_get("/tasks")

        today = date.today()
        if period == "day":
            cutoff = today.isoformat()
            label = f"сегодня ({today.strftime('%d.%m')})"
            deals_f = [d for d in (deals or []) if (d.get("created_at") or "")[:10] == cutoff]
            exp_f   = [e for e in (expenses or []) if (e.get("date") or "")[:10] == cutoff]
            tasks_f = [t for t in (tasks or []) if (t.get("due_date") or "")[:10] == cutoff]
        elif period == "week":
            cutoff = (today - timedelta(days=7)).isoformat()
            label = "последние 7 дней"
            deals_f = [d for d in (deals or []) if (d.get("created_at") or "")[:10] >= cutoff]
            exp_f   = [e for e in (expenses or []) if (e.get("date") or "")[:10] >= cutoff]
            tasks_f = [t for t in (tasks or []) if (t.get("due_date") or "")[:10] >= cutoff]
        else:  # month
            cutoff = today.strftime("%Y-%m")
            label = today.strftime("%B %Y")
            deals_f = [d for d in (deals or []) if (d.get("created_at") or "")[:7] == cutoff]
            exp_f   = [e for e in (expenses or []) if (e.get("date") or "")[:7] == cutoff]
            tasks_f = [t for t in (tasks or []) if (t.get("due_date") or "")[:7] == cutoff]

        total_exp = sum(float(e.get("amount") or 0) for e in exp_f)
        won = [d for d in deals_f if d.get("is_closed") and d.get("total")]
        revenue = sum(float(d.get("total") or 0) for d in won)
        profit = revenue - total_exp
        done_tasks = [t for t in tasks_f
                      if (t.get("status") or "").lower() in ("done", "завершена", "выполнена")]

        return (
            f"📈 <b>Отчёт за {label}</b>\n\n"
            f"💬 Новых сделок: <b>{len(deals_f)}</b>\n"
            f"✅ Закрытых сделок: <b>{len(won)}</b>\n"
            f"💰 Выручка: <b>{revenue:,.0f} ₽</b>\n"
            f"💸 Расходы: <b>{total_exp:,.0f} ₽</b>\n"
            f"📊 Прибыль: <b>{profit:,.0f} ₽</b>\n"
            f"📋 Задач выполнено: <b>{len(done_tasks)}</b> из <b>{len(tasks_f)}</b>"
        )
    except Exception as e:
        logger.warning(f"get_report error: {e}")
        return f"❌ Не удалось сформировать отчёт: {e}"


async def _tool_get_services() -> str:
    """Получить прайс-лист услуг из CRM."""
    try:
        services = await _crm_get("/services")
        if not services:
            return "📋 Прайс-лист пуст."
        lines = ["📋 <b>Прайс-лист услуг:</b>"]
        for s in services:
            name  = s.get("name") or "—"
            price = float(s.get("price") or 0)
            unit  = s.get("unit") or "ед"
            lines.append(f"• <b>{name}</b> — {price:,.0f} ₽/{unit}")
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"get_services error: {e}")
        return f"❌ Не удалось загрузить прайс: {e}"


async def _tool_create_task_direct(
    data: Dict,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Создать задачу напрямую из tool data — без второго вызова AI."""
    msg = update.effective_message
    title    = (data.get("title") or "").strip()
    if not title:
        await msg.reply_text("❌ Не указан заголовок задачи.")
        return

    due_date    = data.get("due_date")
    priority    = data.get("priority") or "Обычный"
    description = data.get("description")
    contact_name = data.get("contact_name")

    context.user_data["pending_task"] = {
        "title": title,
        "description": description,
        "due_date": due_date,
        "priority": priority,
        "contact_name": contact_name,
    }
    due_str = due_date or "не указана"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Создать задачу", callback_data="task_confirm")],
        [InlineKeyboardButton("❌ Отмена",          callback_data="task_cancel")],
    ])
    await msg.reply_text(
        f"📋 <b>Задача:</b>\n\n"
        f"📌 <b>{title}</b>\n"
        f"📅 Срок: {due_str}\n"
        f"🔴 Приоритет: {priority}\n"
        f"👤 Контакт: {contact_name or '—'}\n"
        f"📝 {description or ''}",
        reply_markup=kb,
        parse_mode="HTML",
    )


async def _tool_create_expense_direct(
    data: Dict,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Создать расход напрямую из tool data — без второго вызова AI."""
    msg = update.effective_message
    amount = data.get("amount")
    if not amount:
        await msg.reply_text("❌ Не указана сумма расхода.")
        return

    name     = data.get("name") or "Расход"
    exp_date = data.get("date") or date.today().isoformat()
    cat_str  = data.get("category") or "Прочее"

    try:
        categories = await _get_expense_categories()
        cat_id   = _match_category_id(cat_str, categories)
        cat_name = next((c["name"] for c in categories if c["id"] == cat_id), cat_str)
    except Exception:
        cat_id   = None
        cat_name = cat_str

    context.user_data["pending_expense"] = {
        "name":     name,
        "amount":   float(amount),
        "date":     exp_date,
        "category": cat_name,
        "cat_id":   cat_id,
    }
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Сохранить",         callback_data="exp_confirm")],
        [InlineKeyboardButton("✏️ Изменить название", callback_data="exp_edit_name")],
        [InlineKeyboardButton("❌ Отмена",             callback_data="exp_cancel")],
    ])
    await msg.reply_text(
        f"💸 <b>Расход:</b>\n\n"
        f"📌 {name}\n"
        f"💰 <b>{float(amount):,.0f} ₽</b>\n"
        f"📅 {exp_date}\n"
        f"🗂 {cat_name}",
        reply_markup=kb,
        parse_mode="HTML",
    )


async def _tool_create_contact(
    data: Dict,
    update: Update,
) -> str:
    """Создать контакт напрямую через API."""
    try:
        name = (data.get("name") or "").strip()
        if not name:
            return "❌ Имя контакта обязательно."

        payload = {
            "name": name,
            "phone": data.get("phone"),
            "email": data.get("email"),
            "notes": data.get("notes"),
        }
        created = await _crm_post("/contacts", payload)
        cid = created.get("id", "?") if isinstance(created, dict) else "?"
        return f"✅ Контакт <b>{name}</b> добавлен (#{cid})"
    except Exception as e:
        logger.warning(f"create_contact error: {e}")
        return f"❌ Ошибка при создании контакта: {e}"


async def _tool_create_note(text: str, chat_id: int) -> str:
    """Сохранить заметку прямо в память AI (не засоряет задачи)."""
    try:
        await save_memory(
            chat_id=chat_id,
            role="fact",
            content=text[:1000],
            memory_type="fact",
            importance=2,
            metadata={"source": "note"},
        )
        return "📝 Запомнил."
    except Exception as e:
        logger.warning(f"create_note error: {e}")
        return f"❌ Ошибка при сохранении заметки: {e}"


async def _ai_action(action: str, data: Dict) -> str:
    """
    Вызвать /api/service/ai/action — универсальный executor на backend.
    Используется для change_status и add_comment (требуют доступа к БД).
    """
    try:
        result = await _crm_post("/service/ai/action", {"action": action, "data": data, "source": "bot"})
        return result
    except Exception as e:
        logger.warning(f"ai_action '{action}' error: {e}")
        raise


async def _tool_change_status(data: Dict, update: Update) -> str:
    """Сменить стадию сделки."""
    deal_id    = data.get("deal_id")
    stage_name = data.get("stage_name") or data.get("stage") or ""

    if not deal_id:
        return "❌ Укажи номер сделки (deal_id)."
    if not stage_name:
        return "❌ Укажи название стадии (stage_name)."

    try:
        result = await _ai_action("change_status", {
            "deal_id": deal_id,
            "stage_name": stage_name,
        })
        from_s = result.get("from", "—")
        to_s   = result.get("to", stage_name)
        return f"✅ Сделка #{deal_id}: стадия изменена\n<b>{from_s}</b> → <b>{to_s}</b>"
    except Exception as e:
        return f"❌ Ошибка смены стадии: {e}"


async def _tool_add_comment(data: Dict, update: Update) -> str:
    """Добавить комментарий к сделке."""
    deal_id = data.get("deal_id")
    text    = (data.get("text") or "").strip()

    if not deal_id:
        return "❌ Укажи номер сделки (deal_id)."
    if not text:
        return "❌ Текст комментария не может быть пустым."

    try:
        result = await _ai_action("add_comment", {
            "deal_id": deal_id,
            "text": text,
        })
        return f"💬 Комментарий к сделке #{deal_id} добавлен."
    except Exception as e:
        return f"❌ Ошибка добавления комментария: {e}"


async def _execute_tool(
    action: str,
    data: Dict,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    original_text: str,
) -> bool:
    """
    Универсальный router для tool system.
    Возвращает True если tool был выполнен (даже частично),
    False если action неизвестен (нужно передать в старый pipeline).
    """
    msg = update.effective_message

    # ── GET-инструменты (просто отвечаем) ─────────────────────────────────────
    if action == "get_tasks":
        result = await _tool_get_tasks(data)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "get_deals":
        result = await _tool_get_deals(data)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "get_expenses":
        result = await _tool_get_expenses(data)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "get_stats":
        result = await _tool_get_stats()
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "get_contacts":
        query = data.get("query") or ""
        result = await _tool_get_contacts(query)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "get_report":
        period = data.get("period") or "month"
        result = await _tool_get_report(period)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "get_services":
        result = await _tool_get_services()
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "answer":
        # AI просто отвечает текстом — reply уже показан до вызова этой функции
        return True

    # ── CREATE-инструменты (часть через существующие handlers) ────────────────
    if action == "create_contact":
        result = await _tool_create_contact(data, update)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "create_note":
        text_note = data.get("text") or original_text
        chat_id = update.effective_message.chat_id
        result = await _tool_create_note(text_note, chat_id)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    # ── CREATE-инструменты (переводим в существующие handlers) ────────────────
    if action == "create_task":
        if data.get("title"):
            # Прямое исполнение из tool data — без второго вызова AI
            await _tool_create_task_direct(data, update, context)
        else:
            await _handle_task(update, context, original_text)
        return True

    if action == "create_deal":
        await _handle_deal_from_chat(update, context, original_text)
        return True

    if action == "create_expense":
        if data.get("amount"):
            # Прямое исполнение из tool data — без второго вызова AI
            await _tool_create_expense_direct(data, update, context)
        else:
            await _handle_quick_expense_text(update, context, original_text)
        return True

    if action == "change_status":
        result = await _tool_change_status(data, update)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    if action == "add_comment":
        result = await _tool_add_comment(data, update)
        await msg.reply_text(result, parse_mode="HTML")
        return True

    # Неизвестный action — вернём False, старый pipeline разберётся
    return False


# ════════════════════════════════════════
#  🧠  MEMORY SYSTEM
# ════════════════════════════════════════

MEMORY_FACT_PROMPT = """
Ты — CRM-ассистент. Проанализируй сообщение пользователя.
Если в нём есть важный факт (имя клиента, адрес, предпочтение, договорённость) — извлеки его.
Верни СТРОГО JSON без markdown:
{
  "has_fact": true или false,
  "fact": "краткое изложение факта на русском (1-2 предложения)" или null,
  "importance": 1 или 2 или 3
}
importance: 1=обычное, 2=важно (клиент/сделка), 3=критично (деньги/договор)
Примеры:
"Клиент Иванов живёт в Гатчине, участок 12 соток" → {"has_fact":true,"fact":"Клиент Иванов: Гатчина, 12 соток","importance":2}
"Привет как дела" → {"has_fact":false,"fact":null,"importance":1}
"Договорились на 15 000р за покос" → {"has_fact":true,"fact":"Договорённость: 15 000₽ за покос","importance":3}
""".strip()


async def save_memory(chat_id: int, role: str, content: str,
                      memory_type: str = "message", importance: int = 1,
                      metadata: dict = None, ttl_days: int = None) -> None:
    """Сохранить запись в память через CRM API. Не блокирует основной поток."""
    try:
        payload = {
            "chat_id": str(chat_id),
            "role": role,
            "content": content[:2000],
            "memory_type": memory_type,
            "importance": importance,
            "metadata": metadata or {},
        }
        if ttl_days:
            payload["ttl_days"] = ttl_days
        await _crm_post("/service/ai/memory", payload)
    except Exception as e:
        logger.debug(f"save_memory error (non-critical): {e}")


async def search_memory(chat_id: int, query: str = "") -> List[Dict]:
    """Получить релевантные воспоминания из CRM API."""
    try:
        params = f"?chat_id={chat_id}"
        if query:
            import urllib.parse
            params += f"&q={urllib.parse.quote(query[:100])}"
        result = await _crm_get(f"/service/ai/memory{params}")
        return result.get("memories") or [] if isinstance(result, dict) else []
    except Exception as e:
        logger.debug(f"search_memory error (non-critical): {e}")
        return []


async def _extract_and_save_fact(chat_id: int, user_text: str) -> None:
    """Фоновая задача: извлечь факт из сообщения и сохранить в память."""
    try:
        raw = await _ask_ai_text(MEMORY_FACT_PROMPT, user_text)
        data = _parse_json_safe(raw)
        if data and data.get("has_fact") and data.get("fact"):
            await save_memory(
                chat_id=chat_id,
                role="fact",
                content=data["fact"],
                memory_type="fact",
                importance=int(data.get("importance") or 1),
            )
    except Exception as e:
        logger.debug(f"_extract_and_save_fact error: {e}")


def build_memory_context(memories: List[Dict]) -> str:
    """Форматировать воспоминания в текстовый блок для промпта."""
    if not memories:
        return ""

    facts = [m for m in memories if m.get("memory_type") in ("fact", "preference")]
    msgs  = [m for m in memories if m.get("memory_type") == "message"]

    parts = []
    if facts:
        parts.append("=== ПАМЯТЬ (факты о пользователе) ===")
        for f in facts[:10]:
            imp = "⭐" if f.get("importance", 1) >= 2 else ""
            parts.append(f"  {imp} {f['content']}")

    if msgs:
        parts.append("=== ИСТОРИЯ ДИАЛОГА (последние сообщения) ===")
        for m in msgs[-6:]:  # последние 6 сообщений
            role_label = "Пользователь" if m.get("role") == "user" else "Ассистент"
            parts.append(f"  [{role_label}]: {m['content'][:300]}")

    return "\n".join(parts)


async def _route_via_tools(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
) -> bool:
    """
    Попытаться обработать сообщение через tool system.
    Возвращает True если обработано, False если нужен fallback.
    """
    chat_id = update.effective_message.chat_id

    thinking = await update.effective_message.reply_text("🤔 Анализирую…")
    try:
        # ── Загружаем память ──────────────────────────────────────────────────
        memories = await search_memory(chat_id, query=text)
        memory_ctx = build_memory_context(memories)

        # Если есть память — добавляем её к промпту
        base_prompt = _get_tool_prompt()
        prompt_with_memory = base_prompt
        if memory_ctx:
            prompt_with_memory = base_prompt + "\n\n" + memory_ctx

        # ── Запрос к AI ───────────────────────────────────────────────────────
        raw = await _ask_ai_text(prompt_with_memory, text)
        tool_data = _parse_json_safe(raw)

        if not tool_data:
            logger.warning(f"Tool system: не смог распарсить ответ AI: {raw[:200]}")
            await thinking.delete()
            return False

        # Поддержка мультиэкшенов: {"actions": [...]} или стандартный {"action": ...}
        if tool_data.get("actions"):
            steps = tool_data["actions"]
        elif tool_data.get("action"):
            steps = [{"action": tool_data["action"], "data": tool_data.get("data") or {}, "reply": tool_data.get("reply") or ""}]
        else:
            logger.warning(f"Tool system: нет action в ответе: {raw[:200]}")
            await thinking.delete()
            return False

        await thinking.delete()

        # Берём первый шаг для обратной совместимости
        action = steps[0].get("action", "")
        data   = steps[0].get("data") or {}
        reply  = steps[0].get("reply") or tool_data.get("reply") or ""

        # ── Сохраняем сообщение пользователя в память ─────────────────────────
        asyncio.create_task(save_memory(
            chat_id=chat_id, role="user",
            content=text[:500], memory_type="message", importance=1,
        ))

        # ── Фоновое извлечение фактов из сообщения ────────────────────────────
        asyncio.create_task(_extract_and_save_fact(chat_id, text))

        # ── Показываем reply от AI ─────────────────────────────────────────────
        GET_ACTIONS = ("get_tasks", "get_deals", "get_expenses", "get_stats",
                       "get_report", "get_contacts", "get_services")
        if action == "answer" and reply:
            # Для ответов на вопросы показываем полный текст с иконкой
            await update.effective_message.reply_text(f"💡 {reply}")
            asyncio.create_task(save_memory(
                chat_id=chat_id, role="assistant",
                content=reply[:500], memory_type="message", importance=1,
            ))
        elif reply and action not in GET_ACTIONS:
            # Для action-команд показываем статус без 💡
            await update.effective_message.reply_text(reply)

        # Выполняем все шаги (мультиэкшен)
        result = False
        for i, step in enumerate(steps):
            act  = step.get("action", "")
            dat  = step.get("data") or {}
            # Для доп. шагов показываем их reply если есть
            if i > 0 and step.get("reply"):
                await update.effective_message.reply_text(step["reply"])
            ok = await _execute_tool(act, dat, update, context, text)
            if ok:
                result = True
        return result

    except Exception as e:
        logger.exception(f"Tool routing error: {e}")
        try:
            await thinking.delete()
        except Exception:
            pass
        return False


# ════════════════════════════════════════
#  🔍  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ════════════════════════════════════════

async def _notify_group_new_deal(
    deal_id,
    title: str,
    contact_name: str,
    services: list,
    address: str = "",
    phone: str = "",
    deal_date: str = "",
):
    """Отправить уведомление о новой сделке в группу через основного бота."""
    if not MAIN_BOT_TOKEN or not GROUP_CHAT_ID:
        return

    lines = [f"<b>Сделка №{deal_id} — {title}</b>"]

    if deal_date:
        lines.append(deal_date)

    lines.append(f"Клиент: {contact_name}")
    if phone:
        lines.append(f"Телефон: {phone}")
    if address:
        lines.append(f"Адрес: {address}")

    if services:
        lines.append("")
        lines.append("Услуги:")
        total = 0.0
        for s in services:
            qty = s.get("quantity", 1)
            price = float(s.get("price") or 0)
            line_total = price * qty
            total += line_total
            price_str = f" — {int(line_total):,} ₽".replace(",", "\u00a0") if price else ""
            lines.append(f"  · {s['name']} × {qty}{price_str}")
        if total > 0:
            lines.append("")
            lines.append(f"Итого: {int(total):,} ₽".replace(",", "\u00a0"))

    text = "\n".join(lines)
    url = f"https://api.telegram.org/bot{MAIN_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url, json={"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        logger.warning(f"Не удалось отправить уведомление в группу: {e}")

async def _get_first_stage_id() -> Optional[int]:
    try:
        stages = await _crm_get("/stages")
        for s in stages:
            if not s.get("is_final"):
                return s["id"]
        return stages[0]["id"] if stages else None
    except Exception:
        return None


def _find_best_service(keyword: str, crm_services: List[Dict]) -> Optional[Dict]:
    """Найти лучшее совпадение услуги по ключевому слову."""
    keyword = keyword.lower().strip()
    best = None
    best_score = 0
    for svc in crm_services:
        svc_name = (svc.get("name") or "").lower()
        if keyword in svc_name or svc_name in keyword:
            score = len(set(keyword.split()) & set(svc_name.split())) + 1
            if score > best_score:
                best_score = score
                best = svc
        else:
            kw_words = set(keyword.split())
            svc_words = set(svc_name.split())
            common = kw_words & svc_words
            if common and len(common) > best_score:
                best_score = len(common)
                best = svc
    return best


# Бизнес-правила: если в сделке есть услуга-триггер (ключ),
# автоматически добавляем связанную услугу (значение), если её ещё нет.
# Площадь берётся от услуги-триггера.
SERVICE_AUTO_ADD_RULES = [
    # вывоз → добавить сбор с той же площадью
    ("вывоз", "сбор"),
]

# Услуги-дополнения которые должны иметь то же количество что и покос
SERVICE_QUANTITY_INHERIT = ["вывоз", "сбор"]


async def _match_services(extracted: List[Dict]) -> List[Dict]:
    """Матчим услуги из переписки с реальными услугами из CRM.
    Применяем бизнес-правила (вывоз → сбор и т.д.)"""
    if not extracted:
        return []
    try:
        crm_services = await _crm_get("/services")
    except Exception:
        return []

    result = []
    for item in extracted:
        keyword = (item.get("name") or "").lower().strip()
        quantity = float(item.get("quantity") or 1)
        if not keyword:
            continue
        best = _find_best_service(keyword, crm_services)
        if best:
            result.append({"service_id": best["id"], "name": best["name"], "quantity": quantity, "price": float(best.get("price") or 0)})

    # Применяем бизнес-правила
    existing_names = [s["name"].lower() for s in result]
    to_add = []
    for trigger_kw, auto_kw in SERVICE_AUTO_ADD_RULES:
        # Ищем услугу-триггер в уже добавленных
        trigger_item = None
        for s in result:
            if trigger_kw in s["name"].lower():
                trigger_item = s
                break
        if not trigger_item:
            continue
        # Проверяем, нет ли уже автоуслуги
        already_present = any(auto_kw in name for name in existing_names)
        if already_present:
            continue
        # Ищем автоуслугу в CRM и добавляем с той же площадью
        auto_svc = _find_best_service(auto_kw, crm_services)
        if auto_svc:
            to_add.append({
                "service_id": auto_svc["id"],
                "name": auto_svc["name"],
                "quantity": trigger_item["quantity"],
                "price": float(auto_svc.get("price") or 0),
                "auto_added": True,
            })

    result.extend(to_add)

    # Наследуем количество покоса для услуг-дополнений
    mow_item = next(
        (s for s in result if any(kw in s["name"].lower() for kw in ("покос", "стрижка", "кошение"))),
        None
    )
    if mow_item:
        mow_qty = mow_item["quantity"]
        for s in result:
            if any(kw in s["name"].lower() for kw in SERVICE_QUANTITY_INHERIT):
                s["quantity"] = mow_qty

    return result

async def _find_or_create_contact(name: str, phone: Optional[str] = None) -> Optional[int]:
    """Найти контакт по имени/телефону или создать новый."""
    try:
        contacts = await _crm_get("/contacts")
        # Поиск по телефону
        if phone:
            for c in contacts:
                if (c.get("phone") or "").replace(" ", "") == phone.replace(" ", ""):
                    return c["id"]
        # Поиск по имени (нечёткий)
        if name:
            norm = name.lower().strip()
            for c in contacts:
                if (c.get("name") or "").lower().strip() == norm:
                    return c["id"]
        # Создать новый
        payload = {"name": name}
        if phone:
            payload["phone"] = phone
        created = await _crm_post("/contacts", payload)
        return created.get("id")
    except Exception as e:
        logger.warning(f"_find_or_create_contact error: {e}")
        return None

# Статусы сделок, которые считаются "активными" для прикрепления фото
ACTIVE_STAGE_NAMES = {"согласовать", "в работе", "ожидание", "новая", "новый"}

async def _get_active_deals() -> List[Dict]:
    """Вернуть список активных сделок (согласовать / в работе / ожидание)."""
    try:
        deals = await _crm_get("/deals")
        active = [
            d for d in deals
            if not d.get("stage_is_final")
            and (d.get("stage_name") or "").lower().strip() in ACTIVE_STAGE_NAMES
        ]
        if not active:
            # Фолбэк: все незакрытые
            active = [d for d in deals if not d.get("stage_is_final")]
        return sorted(active, key=lambda d: d.get("updated_at") or d.get("created_at") or "", reverse=True)[:8]
    except Exception as e:
        logger.warning(f"_get_active_deals error: {e}")
        return []

async def _get_expense_categories() -> List[Dict]:
    try:
        return await _crm_get("/expense-categories")
    except Exception:
        return []

def _match_category_id(ai_cat: str, categories: List[Dict]) -> Optional[int]:
    """Сопоставить AI-категорию с реальными категориями расходов."""
    mapping = {
        "топливо":       ["топлив", "бензин", "солярк", "fuel"],
        "расходники":    ["расходник", "запчаст", "масло", "фильтр", "нож", "леска"],
        "оборудование":  ["оборудован", "техник", "инструмент", "купл"],
        "транспорт":     ["транспорт", "доставк", "перевоз"],
        "прочее":        [],
    }
    ai_lower = (ai_cat or "").lower()
    for key, triggers in mapping.items():
        if key in ai_lower or any(t in ai_lower for t in triggers):
            # Ищем среди реальных категорий
            for cat in categories:
                cat_name = (cat.get("name") or "").lower()
                if key in cat_name or any(t in cat_name for t in triggers):
                    return cat["id"]
    # Вернуть первую «прочее» или просто первую
    for cat in categories:
        if "проч" in (cat.get("name") or "").lower():
            return cat["id"]
    return categories[0]["id"] if categories else None


INTENT_PROMPT = """
Ты — ассистент CRM-системы газонокосилочной компании.
Определи намерение пользователя. Верни СТРОГО JSON без markdown:
{
  "intent": "task | deal | expense | contact | note | analytics | report | file | unknown",
  "confidence": 0.0..1.0
}

intent = task      — сделать/напомнить/позвонить/задача/встреча/запланировать
intent = deal      — клиент/заявка/покос/работа на участке/адрес/переписка с клиентом
intent = expense   — потратил/купил/расход/заправка/чек/оплатил/стоит X рублей
intent = contact   — добавить контакт/клиента/найти клиента/новый клиент/телефон клиента
intent = note      — заметка/запомни/важно/пометка (не задача, просто текст)
intent = analytics — статистика/прибыль/аналитика/сколько заработали/как дела с деньгами
intent = report    — отчёт/покажи сводку/итоги/что за день/за неделю/за месяц
intent = file      — файл/документ/фото отчёт/прикреплённое
intent = unknown   — не понятно

Примеры:
"Позвони Иванову завтра"              → {"intent":"task","confidence":0.95}
"Клиент из Ропши хочет покос"         → {"intent":"deal","confidence":0.97}
"Заправил на 2500р"                   → {"intent":"expense","confidence":0.98}
"Добавь клиента Петров +79001234567"  → {"intent":"contact","confidence":0.96}
"Запомни: забрать ключи у Сидорова"   → {"intent":"note","confidence":0.9}
"Какая прибыль за месяц?"             → {"intent":"analytics","confidence":0.95}
"Покажи отчёт за неделю"              → {"intent":"report","confidence":0.93}
"Купил леску 500р"                    → {"intent":"expense","confidence":0.95}
""".strip()

EXPENSE_TEXT_PROMPT = """
Извлеки расход из текста. Верни СТРОГО JSON без markdown:
{
  "name": "название расхода",
  "amount": число или null,
  "date": "YYYY-MM-DD или null",
  "category": "Топливо | Расходники | Оборудование | Транспорт | Прочее"
}
Примеры:
"заправил на 2500" → {"name":"Заправка","amount":2500,"date":null,"category":"Топливо"}
"купил леску 500р" → {"name":"Леска","amount":500,"date":null,"category":"Расходники"}
""".strip()


async def _detect_intent(text: str) -> dict:
    """AI определяет что хочет пользователь. Возвращает {intent, confidence}."""
    try:
        raw = await _ask_ai_text(INTENT_PROMPT, text)
        data = _parse_json_safe(raw)
        if data and data.get("intent"):
            return data
    except Exception as e:
        logger.warning(f"Intent detection error: {e}")
    return {"intent": "unknown", "confidence": 0.0}


async def _handle_quick_expense_text(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Создать расход из текста (без фото). AI извлекает название и сумму."""
    try:
        raw = await _ask_ai_text(EXPENSE_TEXT_PROMPT, text)
        data = _parse_json_safe(raw)

        if not data or not data.get("amount"):
            context.user_data["pending_expense"] = {
                "name": text[:100], "amount": 0.0,
                "date": date.today().isoformat(), "category": "Прочее",
            }
            context.user_data["awaiting"] = "expense_name"
            await update.effective_message.reply_text(
                f"💸 Расход: <b>{text[:100]}</b>\n"
                "Не смог распознать сумму — введи название и сумму через запятую\n"
                "Например: <i>Бензин, 2500</i>",
                parse_mode="HTML",
            )
            return

        categories = await _get_expense_categories()
        cat_id = _match_category_id(data.get("category", "Прочее"), categories)
        cat_name = next((c["name"] for c in categories if c["id"] == cat_id), data.get("category", "Прочее"))

        context.user_data["pending_expense"] = {
            "name":     data.get("name") or text[:100],
            "amount":   float(data["amount"]),
            "date":     data.get("date") or date.today().isoformat(),
            "category": cat_name,
        }
        exp = context.user_data["pending_expense"]
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Сохранить",           callback_data="exp_confirm")],
            [InlineKeyboardButton("✏️ Изменить название",   callback_data="exp_edit_name")],
            [InlineKeyboardButton("❌ Отмена",              callback_data="exp_cancel")],
        ])
        await update.effective_message.reply_text(
            f"💸 <b>Расход:</b>\n\n"
            f"📌 {exp['name']}\n"
            f"💰 <b>{exp['amount']} ₽</b>\n"
            f"📅 {exp['date']}\n"
            f"🗂 {cat_name}",
            reply_markup=kb, parse_mode="HTML",
        )
    except Exception as e:
        logger.exception("Quick expense text error")
        await update.effective_message.reply_text(f"❌ Ошибка: {e}")

# ════════════════════════════════════════
#  /start — справка
# ════════════════════════════════════════

async def cmd_checkup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск proactive проверки."""
    if not _is_allowed(update):
        return await _deny(update)
    msg = await update.effective_message.reply_text("🔍 Анализирую CRM…")
    try:
        summary = await _build_proactive_summary()
        if not summary:
            await msg.edit_text("❌ Не удалось получить данные CRM.")
            return
        raw = await _ask_ai_text(PROACTIVE_PROMPT, f"Сводка CRM:\n{summary}")
        if raw.strip() == "NO_ISSUES":
            await msg.edit_text("✅ Всё в порядке, проблем не обнаружено.")
        else:
            await msg.edit_text(f"📋 <b>Анализ CRM:</b>\n\n{raw}", parse_mode="HTML")
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {e}")


async def cmd_forget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Очистить память ассистента для текущего чата."""
    if not _is_allowed(update):
        return await _deny(update)
    chat_id = update.effective_message.chat_id
    try:
        await _crm_post(f"/service/ai/memory?chat_id={chat_id}", {})
    except Exception:
        pass
    # DELETE через отдельный вызов
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.delete(
                f"{API_BASE_URL}/service/ai/memory",
                headers=API_HEADERS,
                params={"chat_id": str(chat_id)},
            )
    except Exception as e:
        logger.warning(f"cmd_forget error: {e}")
    await update.message.reply_text(
        "🧹 Память очищена. Начинаем с чистого листа.",
        parse_mode="HTML",
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update):
        return await _deny(update)
    await update.message.reply_text(
        "👋 <b>GreenCRM Ассистент</b>\n\n"
        "Что я умею:\n\n"
        "🧾 <b>Фото чека</b> → распознаю и заношу расход\n"
        "💬 <b>Переписка с клиентом</b> → создаю сделку\n"
        "✅ <b>«Задача: текст»</b> → создаю задачу\n"
        "📸 <b>Фото с подписью «до» или «после»</b> → прикрепляю к сделке\n"
        "🎤 <b>Голосовое</b> → расшифрую и обработаю\n\n"
        "Просто отправь нужное — сам разберусь.",
        parse_mode="HTML",
    )

# ════════════════════════════════════════
#  🧾  ЧЕКИ → РАСХОДЫ
# ════════════════════════════════════════

async def _handle_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE, photo_bytes: bytes):
    """Распознать чек и занести расход."""
    msg = await update.effective_message.reply_text("🔍 Читаю чек…")
    try:
        import base64
        b64 = base64.b64encode(photo_bytes).decode()
        raw = await _ask_ai_vision(b64, RECEIPT_PROMPT)
        data = _parse_json_safe(raw)

        if not data or "error" in data:
            err = (data or {}).get("error", raw[:200])
            await msg.edit_text(f"❌ Не смог разобрать чек: {err}")
            return

        name   = data.get("name") or "Расход (чек)"
        amount = data.get("amount")
        dt     = data.get("date") or date.today().isoformat()
        ai_cat = data.get("category", "Прочее")

        if not amount:
            await msg.edit_text("❌ Не удалось распознать сумму на чеке.")
            return

        categories = await _get_expense_categories()
        cat_id = _match_category_id(ai_cat, categories)
        cat_name = next((c["name"] for c in categories if c["id"] == cat_id), ai_cat)

        # Показываем пользователю что нашли, предлагаем подтвердить
        context.user_data["pending_expense"] = {
            "name": name, "amount": float(amount),
            "date": dt, "category": cat_name,
        }
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Сохранить", callback_data="exp_confirm")],
            [InlineKeyboardButton("✏️ Изменить название", callback_data="exp_edit_name")],
            [InlineKeyboardButton("❌ Отмена", callback_data="exp_cancel")],
        ])
        await msg.edit_text(
            f"📋 <b>Распознан расход:</b>\n\n"
            f"📌 Название: <b>{name}</b>\n"
            f"💰 Сумма: <b>{amount} ₽</b>\n"
            f"📅 Дата: <b>{dt}</b>\n"
            f"🗂 Категория: <b>{cat_name}</b>",
            reply_markup=kb,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.exception("Receipt handling error")
        await msg.edit_text(f"❌ Ошибка: {e}")


async def cb_expense(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "exp_cancel":
        context.user_data.pop("pending_expense", None)
        await query.edit_message_text("Отменено.")
        return

    if action == "exp_confirm":
        exp = context.user_data.pop("pending_expense", None)
        if not exp:
            await query.edit_message_text("Нет данных для сохранения.")
            return
        try:
            await _crm_post("/expenses", exp)
            await query.edit_message_text(
                f"✅ Расход <b>{exp['name']}</b> на <b>{exp['amount']} ₽</b> сохранён.",
                parse_mode="HTML",
            )
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка сохранения: {e}")
        return

    if action == "exp_edit_name":
        context.user_data["awaiting"] = "expense_name"
        await query.edit_message_text("Введите новое название расхода:")
        return

# ════════════════════════════════════════
#  💬  ПЕРЕПИСКА → СДЕЛКА
# ════════════════════════════════════════

async def _handle_deal_from_chat(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    msg = await update.effective_message.reply_text("🔄 Анализирую переписку…")
    try:
        raw = await _ask_ai_text(DEAL_FROM_CHAT_PROMPT, text)
        data = _parse_json_safe(raw)

        if not data:
            await msg.edit_text("❌ Не удалось разобрать переписку. Попробуйте отформатировать чище.")
            return

        contact_name = data.get("contact_name") or "Неизвестный клиент"
        phone  = data.get("phone")
        title  = data.get("title") or f"Заявка от {contact_name}"
        notes  = data.get("notes") or ""
        addr   = data.get("address") or ""

        # Матчим услуги
        extracted_services = data.get("services") or []
        matched_services = await _match_services(extracted_services)

        context.user_data["pending_deal"] = {
            "contact_name": contact_name,
            "phone": phone,
            "title": title,
            "notes": notes,
            "address": addr,
            "matched_services": matched_services,
        }
        phone_str = phone or "не указан"
        services_str = ""
        if matched_services:
            lines = []
            for s in matched_services:
                marker = "🔁" if s.get("auto_added") else "🔧"
                lines.append(f"  {marker} {s['name']} × {s['quantity']}")
            services_str = "\nУслуги:\n" + "\n".join(lines)
        elif extracted_services:
            services_str = "\n⚠️ Услуги не распознаны: " + ", ".join(
                s.get("name", "") for s in extracted_services
            )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Создать сделку", callback_data="deal_confirm")],
            [InlineKeyboardButton("❌ Отмена", callback_data="deal_cancel")],
        ])
        await msg.edit_text(
            f"📋 <b>Найдена заявка:</b>\n\n"
            f"👤 Клиент: <b>{contact_name}</b>\n"
            f"📱 Телефон: <b>{phone_str}</b>\n"
            f"📍 Адрес: <b>{addr or '—'}</b>\n"
            f"📌 Сделка: <b>{title}</b>\n"
            f"📝 Детали: {notes[:200] or '—'}"
            f"{services_str}",
            reply_markup=kb,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.exception("Deal from chat error")
        await msg.edit_text(f"❌ Ошибка: {e}")


async def cb_deal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "deal_cancel":
        context.user_data.pop("pending_deal", None)
        await query.edit_message_text("Отменено.")
        return

    if action == "deal_confirm":
        deal = context.user_data.pop("pending_deal", None)
        if not deal:
            await query.edit_message_text("Нет данных для сохранения.")
            return
        try:
            stage_id = await _get_first_stage_id()
            if not stage_id:
                await query.edit_message_text("❌ Не удалось получить стадии сделок.")
                return

            contact_id = await _find_or_create_contact(deal["contact_name"], deal.get("phone"))

            payload = {
                "title": deal["title"],
                "stage_id": stage_id,
                "contact_id": contact_id,
                "address": deal.get("address") or None,
                "notes": deal.get("notes") or None,
                "services": [
                    {"service_id": s["service_id"], "quantity": s["quantity"]}
                    for s in (deal.get("matched_services") or [])
                ],
            }
            if not contact_id:
                payload["new_contact_name"] = deal["contact_name"]
                del payload["contact_id"]

            created = await _crm_post("/deals", payload)
            deal_id = created.get("id", "?")
            context.user_data["last_deal_id"] = created.get("id")
            services_added = deal.get("matched_services") or []
            svc_text = ""
            if services_added:
                svc_text = "\n🔧 " + ", ".join(f"{s['name']} ×{s['quantity']}" for s in services_added)
            await query.edit_message_text(
                f"✅ Сделка <b>№{deal_id} «{deal['title']}»</b> создана!\n"
                f"Клиент: {deal['contact_name']}{svc_text}",
                parse_mode="HTML",
            )
            # Уведомляем группу через основного бота
            # Форматируем дату выезда если есть
            _dd = deal.get("date") or deal.get("work_date") or ""
            _dt = deal.get("time") or deal.get("work_time") or ""
            _date_str = ""
            if _dd:
                try:
                    from datetime import datetime as _dtt
                    _d = _dtt.fromisoformat(_dd[:10])
                    _date_str = _d.strftime("%d.%m.%Y")
                    if _dt:
                        _date_str += f" {_dt[:5]}"
                except Exception:
                    _date_str = _dd
            await _notify_group_new_deal(
                deal_id=deal_id,
                title=deal["title"],
                contact_name=deal["contact_name"],
                services=services_added,
                address=deal.get("address") or "",
                phone=deal.get("phone") or "",
                deal_date=_date_str,
            )
        except Exception as e:
            logger.exception("Deal create error")
            await query.edit_message_text(f"❌ Ошибка создания сделки: {e}")

# ════════════════════════════════════════
#  ✅  ЗАДАЧИ
# ════════════════════════════════════════

async def _handle_task(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    msg = await update.effective_message.reply_text("🔄 Разбираю задачу…")
    try:
        raw = await _ask_ai_text(TASK_PROMPT, text)
        data = _parse_json_safe(raw)

        if not data:
            # Простой fallback — создаём задачу напрямую из текста
            data = {"title": text[:200], "description": None, "due_date": None, "priority": "Обычный"}

        context.user_data["pending_task"] = data
        due = data.get("due_date") or "не указана"
        contact = data.get("contact_name") or "не указан"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Создать задачу", callback_data="task_confirm")],
            [InlineKeyboardButton("❌ Отмена", callback_data="task_cancel")],
        ])
        await msg.edit_text(
            f"📋 <b>Задача:</b>\n\n"
            f"📌 <b>{data['title']}</b>\n"
            f"📅 Срок: {due}\n"
            f"🔴 Приоритет: {data.get('priority', 'Обычный')}\n"
            f"👤 Контакт: {contact}\n"
            f"📝 {data.get('description') or ''}",
            reply_markup=kb,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.exception("Task error")
        await msg.edit_text(f"❌ Ошибка: {e}")


async def cb_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "task_cancel":
        context.user_data.pop("pending_task", None)
        await query.edit_message_text("Отменено.")
        return

    if action == "task_confirm":
        task = context.user_data.pop("pending_task", None)
        if not task:
            await query.edit_message_text("Нет данных.")
            return
        try:
            contact_id = None
            if task.get("contact_name"):
                contact_id = await _find_or_create_contact(task["contact_name"])

            payload = {
                "title": task["title"],
                "description": task.get("description"),
                "due_date": task.get("due_date"),
                "priority": task.get("priority") or "Обычный",
                "contact_id": contact_id,
            }
            created = await _crm_post("/tasks", payload)
            await query.edit_message_text(
                f"✅ Задача <b>«{task['title']}»</b> создана!",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.exception("Task create error")
            await query.edit_message_text(f"❌ Ошибка: {e}")

# ════════════════════════════════════════
#  📸  ФОТО ДО/ПОСЛЕ → СДЕЛКА
# ════════════════════════════════════════

async def _handle_deal_photo(update: Update, context: ContextTypes.DEFAULT_TYPE,
                              photo_bytes: bytes, caption: str):
    """Показать список активных сделок и дать выбрать, к которой прикрепить фото."""
    caption_lower = (caption or "").lower().strip()

    if "до" in caption_lower or "before" in caption_lower:
        kind = "before"
        kind_label = "ДО"
    elif "после" in caption_lower or "after" in caption_lower:
        kind = "after"
        kind_label = "ПОСЛЕ"
    else:
        kind = "general"
        kind_label = "общее"

    msg = await update.effective_message.reply_text("🔍 Ищу активные сделки…")

    # Сохраняем фото и kind в контексте
    context.user_data["pending_photo"] = {
        "bytes": photo_bytes,
        "kind": kind,
        "kind_label": kind_label,
        "deal_id": None,
    }

    active_deals = await _get_active_deals()

    if not active_deals:
        await msg.edit_text(
            "❌ Нет активных сделок (статусы: Согласовать / В работе / Ожидание). "
            "Введите номер сделки вручную или создайте сделку сначала.",
        )
        context.user_data["awaiting"] = "photo_deal_id"
        return

    # Строим клавиатуру из списка сделок
    buttons = []
    for d in active_deals:
        deal_id = d.get("id")
        title = (d.get("title") or f"Сделка #{deal_id}")[:35]
        stage = (d.get("stage_name") or "").strip()
        label = f"#{deal_id} {title} [{stage}]"
        buttons.append([InlineKeyboardButton(label, callback_data=f"photo_pick_{deal_id}")])

    buttons.append([InlineKeyboardButton("🔢 Ввести номер вручную", callback_data="photo_other")])
    buttons.append([InlineKeyboardButton("❌ Отмена", callback_data="photo_cancel")])

    await msg.edit_text(
        f"📸 Фото <b>{kind_label}</b>. Выберите сделку:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML",
    )


async def cb_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "photo_cancel":
        context.user_data.pop("pending_photo", None)
        await query.edit_message_text("Отменено.")
        return

    if action == "photo_other":
        context.user_data["awaiting"] = "photo_deal_id"
        await query.edit_message_text("Введите номер сделки (только цифры):")
        return

    if action == "photo_confirm":
        pending = context.user_data.pop("pending_photo", None)
        if not pending:
            await query.edit_message_text("Нет данных.")
            return
        await _upload_photo_to_deal(query, pending["bytes"], pending["deal_id"],
                                    pending["kind"], pending["kind_label"])
        return

    if action.startswith("photo_pick_"):
        deal_id_str = action.split("photo_pick_", 1)[1]
        if not deal_id_str.isdigit():
            await query.edit_message_text("Ошибка: неверный ID сделки.")
            return
        deal_id = int(deal_id_str)
        pending = context.user_data.pop("pending_photo", None)
        if not pending:
            await query.edit_message_text("Нет данных.")
            return
        context.user_data["last_deal_id"] = deal_id
        await _upload_photo_to_deal(query, pending["bytes"], deal_id,
                                    pending["kind"], pending["kind_label"])


async def _upload_photo_to_deal(target, photo_bytes: bytes, deal_id: int,
                                 kind: str, kind_label: str):
    try:
        filename = f"photo_{kind}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        await _crm_upload_file(
            "/files",
            photo_bytes,
            filename,
            extra_fields={"deal_id": str(deal_id), "file_kind": kind},
        )
        text = f"✅ Фото <b>{kind_label}</b> прикреплено к сделке #{deal_id}."
        if hasattr(target, "edit_message_text"):
            await target.edit_message_text(text, parse_mode="HTML")
        else:
            await target.reply_text(text, parse_mode="HTML")
    except Exception as e:
        logger.exception("Photo upload error")
        text = f"❌ Ошибка загрузки фото: {e}"
        if hasattr(target, "edit_message_text"):
            await target.edit_message_text(text)
        else:
            await target.reply_text(text)

# ════════════════════════════════════════
#  📥  ГЛАВНЫЙ РОУТЕР СООБЩЕНИЙ
# ════════════════════════════════════════

# ════════════════════════════════════════
#  📄  DOCUMENT HANDLER — PDF / Excel / Word / TXT
# ════════════════════════════════════════

DOC_ANALYZE_PROMPT = """
Ты — AI-ассистент CRM газонокосилочного бизнеса GrassCRM.
Тебе передан текст из документа. Проанализируй его и сделай следующее:

1. Кратко (2-3 предложения) опиши что это за документ.
2. Если это договор, смета, счёт или заявка — извлеки ключевые данные:
   клиент, адрес, услуги, суммы, даты.
3. Предложи конкретные действия в CRM (создать сделку, задачу, контакт).

Отвечай строго на русском, структурированно.
""".strip()

DOC_EXPENSE_PROMPT = """
Ты — AI-ассистент CRM. Из текста документа извлеки данные о расходе.
Верни СТРОГО JSON без markdown:
{
  "name": "название расхода",
  "amount": число (только цифры),
  "category": "категория из списка: Топливо / Оборудование / Расходники / Зарплата / Реклама / Прочее",
  "date": "YYYY-MM-DD или null",
  "confidence": 0.0-1.0
}
Если это не документ о расходе — верни {"confidence": 0.0}
""".strip()


def _extract_text_from_file(file_bytes: bytes, filename: str) -> str:
    """Извлечь текст из файла по расширению."""
    ext = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''
    text = ''
    try:
        if ext == 'txt' or ext == 'csv':
            try:
                text = file_bytes.decode('utf-8')
            except UnicodeDecodeError:
                text = file_bytes.decode('cp1251', errors='replace')

        elif ext == 'pdf':
            import pdfplumber
            parts = []
            with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                for page in pdf.pages[:10]:
                    page_text = page.extract_text()
                    if page_text:
                        parts.append(page_text)
            text = chr(10).join(parts)

        elif ext in ('xlsx', 'xls'):
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
            parts = []
            for sheet in wb.worksheets[:3]:
                rows = []
                for row in sheet.iter_rows(max_row=200, values_only=True):
                    cells = [str(c) for c in row if c is not None]
                    if cells:
                        rows.append(chr(9).join(cells))
                if rows:
                    parts.append("[Лист: " + sheet.title + "]" + chr(10) + chr(10).join(rows))
            text = (chr(10) * 2).join(parts)

        elif ext in ('docx', 'doc'):
            import docx as _docx
            doc = _docx.Document(io.BytesIO(file_bytes))
            text = chr(10).join(p.text for p in doc.paragraphs if p.text.strip())

    except Exception as e:
        logger.warning(f"_extract_text_from_file({ext}): {e}")
        text = ''

    return text.strip()[:8000]


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик документов: PDF, Excel, Word, TXT, CSV."""
    if not _is_allowed(update):
        return await _deny(update)

    message       = update.effective_message
    doc           = message.document
    caption       = (message.caption or '').strip()
    caption_lower = caption.lower()
    filename      = doc.file_name or 'document'
    ext           = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''

    SUPPORTED = {'pdf', 'xlsx', 'xls', 'docx', 'doc', 'txt', 'csv'}
    if ext not in SUPPORTED:
        await message.reply_text(
            "Файл " + filename + " получен.\n"
            "Поддерживаемые форматы: PDF, Excel, Word, TXT, CSV.",
        )
        return

    msg = await message.reply_text("Читаю " + filename + "…")

    try:
        tg_file    = await doc.get_file()
        file_data  = await tg_file.download_as_bytearray()
        file_bytes = bytes(file_data)

        text = _extract_text_from_file(file_bytes, filename)

        if not text:
            await msg.edit_text(
                "Не удалось извлечь текст из " + filename + ".\n"
                "Возможно файл повреждён или защищён паролем."
            )
            return

        await msg.edit_text("Анализирую " + filename + "…")

        is_expense = any(k in caption_lower for k in ('чек', 'расход', 'трат', 'счёт', 'invoice'))

        if is_expense:
            raw  = await _ask_ai_text(DOC_EXPENSE_PROMPT, text[:3000])
            data = _parse_json_safe(raw)
            if data and float(data.get('confidence') or 0) >= 0.6 and float(data.get('amount') or 0) > 0:
                from datetime import date as _d
                payload = {
                    'name':     data.get('name') or filename,
                    'amount':   float(data.get('amount') or 0),
                    'date':     data.get('date') or _d.today().isoformat(),
                    'category': data.get('category') or 'Прочее',
                }
                await _crm_post('/expenses', payload)
                await msg.edit_text(
                    "Расход занесён: " + payload['name'] +
                    " — " + str(int(payload['amount'])) + "руб."
                )
                return

        raw = await _ask_ai_text(DOC_ANALYZE_PROMPT, "Документ: " + filename + chr(10) + chr(10) + text[:5000])

        chat_id = message.chat_id
        asyncio.create_task(save_memory(
            chat_id=chat_id,
            role='fact',
            content="Документ " + filename + ": " + raw[:400],
            memory_type='fact',
            importance=2,
            metadata={'source': 'document', 'filename': filename},
        ))

        context.user_data['pending_doc_text']     = text[:3000]
        context.user_data['pending_doc_filename'] = filename
        context.user_data['pending_doc_analysis'] = raw

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Создать сделку из документа", callback_data="doc_deal")],
            [InlineKeyboardButton("Создать задачу из документа", callback_data="doc_task")],
            [InlineKeyboardButton("Только сохранить в память",   callback_data="doc_mem")],
        ])

        await msg.edit_text(
            filename + chr(10) + chr(10) + raw + chr(10) + chr(10) + "Что сделать?",
            reply_markup=kb,
        )

    except Exception as e:
        logger.exception(f"handle_document error: {e}")
        await msg.edit_text("Ошибка при обработке файла: " + str(e))


async def cb_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback для кнопок после анализа документа."""
    query    = update.callback_query
    await query.answer()
    action   = query.data  # doc_deal / doc_task / doc_mem
    text     = context.user_data.get('pending_doc_text', '')
    filename = context.user_data.get('pending_doc_filename', 'документ')
    analysis = context.user_data.get('pending_doc_analysis', '')

    if action == 'doc_mem':
        await query.edit_message_text(
            f"💾 Документ <b>{filename}</b> сохранён в память ассистента.",
            parse_mode='HTML'
        )

    elif action == 'doc_deal':
        # Передаём анализ как текст для создания сделки
        context.user_data['pending_doc_text'] = ''
        await query.edit_message_text(
            f"📋 Создаю сделку из <b>{filename}</b>…",
            parse_mode='HTML'
        )
        await _handle_deal_from_chat(update, context, analysis)

    elif action == 'doc_task':
        context.user_data['pending_doc_text'] = ''
        await query.edit_message_text(
            f"✅ Создаю задачу из <b>{filename}</b>…",
            parse_mode='HTML'
        )
        await _handle_task(update, context, analysis)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update):
        return await _deny(update)

    message = update.effective_message
    caption = (message.caption or "").strip()
    caption_lower = caption.lower()

    # Скачиваем фото
    photo = message.photo[-1]  # наибольшее разрешение
    tg_file = await photo.get_file()
    photo_bytes = await tg_file.download_as_bytearray()
    photo_bytes = bytes(photo_bytes)

    # Роутинг по подписи
    # "чек" — расход
    if "чек" in caption_lower or "расход" in caption_lower or "трат" in caption_lower:
        await _handle_receipt(update, context, photo_bytes)
    # до/после — к сделке
    elif any(k in caption_lower for k in ("до", "после", "before", "after")):
        await _handle_deal_photo(update, context, photo_bytes, caption)
    else:
        # Неясно — спрашиваем
        context.user_data["pending_photo_bytes"] = photo_bytes
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🧾 Это чек (занести расход)", callback_data="route_receipt")],
            [InlineKeyboardButton("📸 Фото ДО к сделке",        callback_data="route_before")],
            [InlineKeyboardButton("📸 Фото ПОСЛЕ к сделке",     callback_data="route_after")],
            [InlineKeyboardButton("❌ Отмена",                   callback_data="route_cancel")],
        ])
        await message.reply_text("Что сделать с этим фото?", reply_markup=kb)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update):
        return await _deny(update)

    message = update.effective_message
    text = (message.text or "").strip()
    if not text:
        return

    awaiting = context.user_data.get("awaiting")

    # ── Ожидаем ввод имени расхода
    if awaiting == "expense_name":
        context.user_data.pop("awaiting", None)
        exp = context.user_data.get("pending_expense")
        if exp:
            exp["name"] = text
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Сохранить", callback_data="exp_confirm")],
            [InlineKeyboardButton("❌ Отмена",    callback_data="exp_cancel")],
        ])
        await message.reply_text(
            f"Новое название: <b>{text}</b>. Сохранить?",
            reply_markup=kb, parse_mode="HTML",
        )
        return

    # ── Ожидаем номер сделки для фото
    if awaiting == "photo_deal_id":
        context.user_data.pop("awaiting", None)
        pending = context.user_data.get("pending_photo")
        if pending and text.isdigit():
            pending["deal_id"] = int(text)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Загрузить", callback_data="photo_confirm")],
                [InlineKeyboardButton("❌ Отмена",    callback_data="photo_cancel")],
            ])
            await message.reply_text(
                f"Загрузить фото <b>{pending['kind_label']}</b> в сделку <b>#{text}</b>?",
                reply_markup=kb, parse_mode="HTML",
            )
        else:
            await message.reply_text("Неверный формат. Введите только цифры.")
        return

    # ── Явные префиксы (быстро, без AI)
    text_lower = text.lower().strip()
    if text_lower.startswith("задача:"):
        await _handle_task(update, context, text[7:].strip() or text)
        return
    if text_lower.startswith("сделка:"):
        await _handle_deal_from_chat(update, context, text[7:].strip() or text)
        return
    if text_lower.startswith("расход:"):
        await _handle_quick_expense_text(update, context, text[7:].strip() or text)
        return

    # ── Tool system (AI agent) — основной путь ──────────────────────────────
    handled = await _route_via_tools(update, context, text)
    if handled:
        return

    # ── Fallback: кнопки выбора (tool system не справился) ──────────────────
    context.user_data["pending_text"] = text
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Задача",  callback_data="route_task")],
        [InlineKeyboardButton("💬 Сделка",  callback_data="route_deal")],
        [InlineKeyboardButton("💸 Расход",  callback_data="route_expense")],
        [InlineKeyboardButton("❌ Ничего",  callback_data="route_cancel")],
    ])
    await message.reply_text(
        f"Не смог обработать автоматически. Что сделать?\n\n<i>{text[:200]}</i>",
        reply_markup=kb, parse_mode="HTML",
    )


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed(update):
        return await _deny(update)

    msg = await update.effective_message.reply_text("🎤 Расшифровываю голосовое…")
    try:
        voice = update.effective_message.voice
        tg_file = await voice.get_file()
        voice_bytes = bytes(await tg_file.download_as_bytearray())
        text = await _transcribe_voice(voice_bytes)

        if not text:
            await msg.edit_text("❌ Не удалось распознать речь.")
            return

        await msg.edit_text(f"📝 Расшифровка:\n<i>{text}</i>", parse_mode="HTML")

        # Tool system — основной путь
        handled = await _route_via_tools(update, context, text)
        if handled:
            return

        # Fallback: кнопки выбора
        context.user_data["pending_text"] = text
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Задача",  callback_data="route_task")],
            [InlineKeyboardButton("💬 Сделка",  callback_data="route_deal")],
            [InlineKeyboardButton("💸 Расход",  callback_data="route_expense")],
            [InlineKeyboardButton("❌ Ничего",  callback_data="route_cancel")],
        ])
        await update.effective_message.reply_text(
            "Не смог обработать автоматически. Что сделать?",
            reply_markup=kb,
        )
    except Exception as e:
        logger.exception("Voice error")
        await msg.edit_text(f"❌ Ошибка: {e}")

# ════════════════════════════════════════
#  🔀  ROUTING CALLBACKS
# ════════════════════════════════════════

async def cb_route(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "route_cancel":
        context.user_data.pop("pending_photo_bytes", None)
        context.user_data.pop("pending_text", None)
        context.user_data.pop("voice_transcription", None)
        await query.edit_message_text("Ок, отменено.")
        return

    if action == "route_receipt":
        photo_bytes = context.user_data.pop("pending_photo_bytes", None)
        if photo_bytes:
            await query.edit_message_text("🔍 Читаю чек…")
            await _handle_receipt(update, context, photo_bytes)
        return

    if action in ("route_before", "route_after"):
        photo_bytes = context.user_data.pop("pending_photo_bytes", None)
        if photo_bytes:
            caption = "до" if action == "route_before" else "после"
            await query.edit_message_text(f"📤 Обрабатываю фото ({caption})…")
            await _handle_deal_photo(update, context, photo_bytes, caption)
        return

    if action == "route_task":
        text = context.user_data.pop("pending_text", None) or context.user_data.pop("voice_transcription", None)
        if text:
            await query.edit_message_text("🔄 Разбираю задачу…")
            await _handle_task(update, context, text)
        return

    if action == "route_deal":
        text = context.user_data.pop("pending_text", None) or context.user_data.pop("voice_transcription", None)
        if text:
            await query.edit_message_text("🔄 Анализирую переписку…")
            await _handle_deal_from_chat(update, context, text)
        return

    if action == "route_expense":
        text = context.user_data.pop("pending_text", None) or context.user_data.pop("voice_transcription", None)
        if text:
            await query.edit_message_text("💸 Разбираю расход…")
            await _handle_quick_expense_text(update, context, text)
        return

# ════════════════════════════════════════
#  🚀  ЗАПУСК
# ════════════════════════════════════════

# ════════════════════════════════════════
#  🔔  PROACTIVE AI — сам пишет когда видит проблемы
# ════════════════════════════════════════

PROACTIVE_PROMPT = """
Ты — AI-ассистент CRM газонокосилочного бизнеса GrassCRM.
Тебе передана сводка по бизнесу. Найди проблемы и напиши короткое сообщение владельцу.

Проверь:
1. Просроченные задачи (due_date < сегодня)
2. Сделки без движения (нет задач, нет комментариев)
3. Нет задач на завтра
4. Расходы превышают выручку за месяц

Если проблем нет — ответь только: NO_ISSUES

Если есть проблемы — напиши коротко (3-5 строк), конкретно, без воды.
Начни с эмодзи. Не повторяй цифры которые и так очевидны.
Пример: "⚠️ 3 задачи просрочены — Авито, Связаться, Скидка. Разберись сегодня."
""".strip()


async def _send_to_owner(text: str) -> None:
    """Отправить сообщение владельцу через основного бота."""
    if not MAIN_BOT_TOKEN or not GROUP_CHAT_ID:
        logger.warning("Proactive: MAIN_BOT_TOKEN или GROUP_CHAT_ID не заданы")
        return
    try:
        url = f"https://api.telegram.org/bot{MAIN_BOT_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(url, json={
                "chat_id": GROUP_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
            })
    except Exception as e:
        logger.warning(f"Proactive send error: {e}")


async def _build_proactive_summary() -> str:
    """Собрать сводку для proactive анализа."""
    try:
        from datetime import date as _date, timedelta
        today = _date.today()
        tomorrow = today + timedelta(days=1)
        month_prefix = today.strftime("%Y-%m")

        tasks    = await _crm_get("/tasks") or []
        deals    = await _crm_get("/deals") or []
        expenses = await _crm_get(f"/expenses?year={today.year}") or []

        # Просроченные задачи
        overdue = [
            t for t in tasks
            if t.get("due_date") and str(t["due_date"])[:10] < today.isoformat()
            and (t.get("status") or "").lower() not in ("завершена", "выполнена", "done")
        ]

        # Задачи на завтра
        tomorrow_tasks = [
            t for t in tasks
            if str(t.get("due_date") or "")[:10] == tomorrow.isoformat()
        ]

        # Активные сделки без изменений (нет work_date в ближайшие 7 дней)
        active_deals = [d for d in deals if not d.get("is_closed")]

        # Финансы за месяц
        month_exp = sum(
            float(e.get("amount") or 0) for e in expenses
            if str(e.get("date") or "")[:7] == month_prefix
        )
        won_deals = [d for d in deals if d.get("is_closed") and d.get("total")]
        # Приближённо — выручка закрытых в этом месяце
        month_rev = sum(
            float(d.get("total") or 0) for d in won_deals
            # нет поля месяца закрытия в API — берём все
        )

        lines = [
            f"Дата: {today.isoformat()}",
            f"Просроченных задач: {len(overdue)}",
            f"Названия просроченных: {', '.join(t.get('title','?') for t in overdue[:5])}",
            f"Задач на завтра: {len(tomorrow_tasks)}",
            f"Активных сделок: {len(active_deals)}",
            f"Расходы за месяц: {month_exp:,.0f}₽",
            f"Выручка (закрытые сделки): {month_rev:,.0f}₽",
        ]
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"_build_proactive_summary error: {e}")
        return ""


async def proactive_morning_check(context) -> None:
    """Утренняя проверка — 9:00 по Москве."""
    logger.info("Proactive: утренняя проверка...")
    try:
        summary = await _build_proactive_summary()
        if not summary:
            return

        prompt = f"Утренняя сводка CRM:\n{summary}"
        raw = await _ask_ai_text(PROACTIVE_PROMPT, prompt)

        if raw.strip() == "NO_ISSUES":
            logger.info("Proactive: проблем нет, молчим.")
            return

        await _send_to_owner(f"🌅 <b>Доброе утро!</b>\n\n{raw}")
        logger.info("Proactive: утреннее сообщение отправлено")
    except Exception as e:
        logger.warning(f"proactive_morning_check error: {e}")


async def proactive_evening_check(context) -> None:
    """Вечерняя проверка — 19:00 по Москве."""
    logger.info("Proactive: вечерняя проверка...")
    try:
        summary = await _build_proactive_summary()
        if not summary:
            return

        prompt = f"Вечерняя сводка CRM:\n{summary}"
        raw = await _ask_ai_text(PROACTIVE_PROMPT, prompt)

        if raw.strip() == "NO_ISSUES":
            logger.info("Proactive: проблем нет, молчим.")
            return

        await _send_to_owner(f"🌆 <b>Вечерняя сводка</b>\n\n{raw}")
        logger.info("Proactive: вечернее сообщение отправлено")
    except Exception as e:
        logger.warning(f"proactive_evening_check error: {e}")


def main():
    app = Application.builder().token(ASSISTANT_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   cmd_start))
    app.add_handler(CommandHandler("forget",   cmd_forget))
    app.add_handler(CommandHandler("checkup", cmd_checkup))

    # Фото
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(cb_document, pattern="^doc_"))

    # Голосовые
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))

    # Текст
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Callback-кнопки
    app.add_handler(CallbackQueryHandler(cb_expense, pattern="^exp_"))
    app.add_handler(CallbackQueryHandler(cb_deal,    pattern="^deal_"))
    app.add_handler(CallbackQueryHandler(cb_task,    pattern="^task_"))
    app.add_handler(CallbackQueryHandler(cb_photo,   pattern="^photo_"))
    app.add_handler(CallbackQueryHandler(cb_route,   pattern="^route_"))

    # ── Proactive AI — расписание проверок ───────────────────────────────────
    job_queue = app.job_queue
    if job_queue:
        import pytz
        moscow = pytz.timezone("Europe/Moscow")
        from datetime import time as _time

        # Утренняя проверка — 9:00 МСК
        job_queue.run_daily(
            proactive_morning_check,
            time=_time(hour=9, minute=0, tzinfo=moscow),
            name="proactive_morning",
        )
        # Вечерняя проверка — 19:00 МСК
        job_queue.run_daily(
            proactive_evening_check,
            time=_time(hour=19, minute=0, tzinfo=moscow),
            name="proactive_evening",
        )
        logger.info("Proactive AI: расписание установлено (09:00 и 19:00 МСК)")
    else:
        logger.warning("Proactive AI: job_queue недоступен")

    logger.info("GreenCRM Assistant Bot запускается…")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
