# -*- coding: utf-8 -*-
"""
GrassCRM Telegram Bot — bot.py v1.3
- Позволяет создавать сделки через интерактивный диалог с категориями услуг.
- Автоматически отправляет ежедневные отчеты.
- Корректно удаляет за собой сообщения, оставляя только итоговый результат.
"""

import asyncio
import html
import logging
import os
import sys
import re
import tempfile
from datetime import date, timedelta, time
import pytz

from dotenv import load_dotenv
load_dotenv()

import requests
import httpx
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, joinedload, contains_eager

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from main import Deal, Stage, Contact, Task, DailyPhrase, Service, Expense, ExpenseCategory, DATABASE_URL
except ImportError as e:
    print(f"Critical Error: Cannot import models from main.py: {e}", file=sys.stderr)
    sys.exit(1)

# ════════════════════════════════════════
#  ⚙️  НАСТРОЙКИ
# ════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
OWNER_ID = 29635426  # ID владельца для личных уведомлений
ASSISTANT_BOT_TOKEN = os.getenv("TELEGRAM_ASSISTANT_BOT_TOKEN")
API_BASE_URL = "http://127.0.0.1:8000/api"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

if not INTERNAL_API_KEY:
    logger.critical("Переменная окружения INTERNAL_API_KEY не установлена!")
    sys.exit(1)
API_HEADERS = {"X-Internal-API-Key": INTERNAL_API_KEY}

# Состояния для диалога создания сделки
(GET_TITLE, GET_CLIENT, PICK_CONTACT, CHOOSE_CATEGORY, CHOOSE_SERVICE, GET_QUANTITY, ADD_MORE) = range(7)

# Состояния для диалога расхода
(EXP_CHOOSE_CATEGORY, EXP_GET_NAME, EXP_GET_AMOUNT) = range(7, 10)

SERVICE_CATEGORIES = {
    "🌿 Покос травы": list(range(1, 7)),
    "🧹 Уборка и вывоз": list(range(7, 11)),
    "🌱 Газон и почва": list(range(11, 18)),
    "🧪 Обработки": list(range(18, 21)),
    "🌳 Деревья и кустарники": list(range(21, 25)),
    "🌸 Посадка и уход за клумбами": list(range(25, 34)),
    "🍀 Удобрение и питание газона": list(range(34, 38)),
}

# ════════════════════════════════════════
#  🗑  УПРАВЛЕНИЕ СООБЩЕНИЯМИ (ИСПРАВЛЕНО)
# ════════════════════════════════════════

def add_message_to_cleanup(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    if 'messages_to_delete' not in context.user_data:
        context.user_data['messages_to_delete'] = []
    context.user_data['messages_to_delete'].append(message_id)

async def cleanup_temp_messages(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Удаляет все ВРЕМЕННЫЕ сообщения, собранные в ходе диалога."""
    message_ids = context.user_data.get('messages_to_delete', [])
    logger.info(f"Очистка {len(message_ids)} временных сообщений...")
    for message_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except BadRequest as e:
            if "Message to delete not found" not in e.message:
                logger.warning(f"Не удалось удалить сообщение {message_id}: {e}")
    context.user_data['messages_to_delete'] = []

# ════════════════════════════════════════
#  💬  ДИАЛОГ СОЗДАНИЯ СДЕЛКИ (ИСПРАВЛЕН)
# ════════════════════════════════════════

async def new_deal_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    last_id = context.user_data.get('last_created_deal_id')
    context.user_data.clear()
    if last_id:
        context.user_data['last_created_deal_id'] = last_id
    context.user_data['deal_data'] = {'services': []}
    add_message_to_cleanup(context, update.message.message_id)
    
    sent_message = await update.message.reply_text(
        "Начинаем создание новой сделки...\n\n"
        "<b>Шаг 1/3:</b> Введите название сделки.",
        parse_mode='HTML'
    )
    # Это сообщение станет основным, его не удаляем, а запоминаем
    context.user_data['main_dialog_message_id'] = sent_message.message_id
    return GET_TITLE

async def get_deal_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    deal_title = update.message.text.strip()
    add_message_to_cleanup(context, update.message.message_id) # Удаляем сообщение пользователя

    if not deal_title:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['main_dialog_message_id'],
            text="Название сделки не может быть пустым. Попробуйте снова."
        )
        return GET_TITLE

    context.user_data['deal_data']['title'] = deal_title
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data['main_dialog_message_id'],
        text=f"Название сделки: <b>{html.escape(deal_title)}</b>.\n\n"
             "<b>Шаг 2/3:</b> Теперь введите имя контакта.",
        parse_mode='HTML'
    )
    return GET_CLIENT

async def get_client_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    client_name = update.message.text.strip()
    add_message_to_cleanup(context, update.message.message_id)

    if not client_name:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['main_dialog_message_id'],
            text="Имя контакта не может быть пустым. Попробуйте снова."
        )
        return GET_CLIENT

    deal_data = context.user_data['deal_data']
    deal_data['client_name'] = client_name

    normalized_name = " ".join(client_name.lower().split())
    found_exact_contact = None
    suggested_contacts = []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(f"{API_BASE_URL}/contacts", headers=API_HEADERS)
            response.raise_for_status()
            contacts = response.json()
        for contact in contacts:
            contact_name = (contact.get('name') or '').strip()
            normalized_contact_name = " ".join(contact_name.lower().split())
            if normalized_contact_name == normalized_name:
                found_exact_contact = contact
                break
            if normalized_name and normalized_name in normalized_contact_name:
                suggested_contacts.append(contact)
    except Exception as e:
        logger.warning(f"Не удалось проверить контакт в CRM: {e}")

    if found_exact_contact:
        deal_data['contact_id'] = found_exact_contact.get('id')
        deal_data['contact_name'] = found_exact_contact.get('name') or client_name
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['main_dialog_message_id'],
            text=f"Контакт найден: <b>{html.escape(deal_data['contact_name'])}</b>.\n\n<b>Шаг 3/3:</b> Выберите категорию услуг.",
            parse_mode='HTML'
        )
        return await show_category_keyboard(update, context)

    if suggested_contacts:
        context.user_data['suggested_contacts'] = {str(c['id']): c for c in suggested_contacts[:5]}
        context.user_data['pending_contact_name'] = client_name
        keyboard = [
            [InlineKeyboardButton(f"👤 {c['name']}", callback_data=f"pick_contact_{c['id']}")]
            for c in suggested_contacts[:5]
        ]
        keyboard.append([InlineKeyboardButton(f"➕ Создать новый: {client_name}", callback_data="pick_contact_new")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_deal")])
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['main_dialog_message_id'],
            text="Нашел похожие контакты. Выберите существующий или создайте новый:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return PICK_CONTACT

    deal_data['new_contact_name'] = client_name
    deal_data['contact_name'] = client_name
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data['main_dialog_message_id'],
        text=f"Создам новый контакт: <b>{html.escape(client_name)}</b>.\n\n<b>Шаг 3/3:</b> Выберите категорию услуг.",
        parse_mode='HTML'
    )
    return await show_category_keyboard(update, context)


async def pick_contact_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    deal_data = context.user_data.get('deal_data', {})

    if query.data == "pick_contact_new":
        name = context.user_data.get('pending_contact_name') or deal_data.get('client_name') or ''
        deal_data['new_contact_name'] = name
        deal_data['contact_name'] = name
        deal_data.pop('contact_id', None)
        await query.edit_message_text(
            text=f"Создам новый контакт: <b>{html.escape(name)}</b>.\n\n<b>Шаг 3/3:</b> Выберите категорию услуг.",
            parse_mode='HTML'
        )
        return await show_category_keyboard(update, context)

    contact_id = query.data.split("pick_contact_", 1)[1]
    contact = context.user_data.get('suggested_contacts', {}).get(contact_id)
    if not contact:
        await query.edit_message_text("Не удалось найти контакт, введите имя снова.")
        return GET_CLIENT

    deal_data['contact_id'] = contact.get('id')
    deal_data['contact_name'] = contact.get('name')
    deal_data.pop('new_contact_name', None)
    await query.edit_message_text(
        text=f"Выбран контакт: <b>{html.escape(deal_data['contact_name'])}</b>.\n\n<b>Шаг 3/3:</b> Выберите категорию услуг.",
        parse_mode='HTML'
    )
    return await show_category_keyboard(update, context)


async def fetch_services_if_needed(context: ContextTypes.DEFAULT_TYPE) -> bool:
    if 'services_list' not in context.user_data:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(f"{API_BASE_URL}/services", headers=API_HEADERS)
                response.raise_for_status()
                services = response.json()
            context.user_data['services_list'] = {s['id']: s for s in services}
            return True
        except Exception as e:
            logger.error(f"Ошибка при запросе услуг из API: {e}")
            return False
    return True

async def show_category_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await fetch_services_if_needed(context):
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, 
            message_id=context.user_data['main_dialog_message_id'],
            text="Не удалось загрузить список услуг. Попробуйте отменить (/cancel) и начать заново."
        ) 
        return CHOOSE_CATEGORY

    keyboard = [[InlineKeyboardButton(name, callback_data=f"cat_{name}")] for name in SERVICE_CATEGORIES.keys()]
    # ДОБАВЛЕНА КНОПКА ОТМЕНЫ
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_deal")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = f"<b>Шаг 3/3:</b> Выберите категорию услуг:"

    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data['main_dialog_message_id'],
        text=message_text, 
        reply_markup=reply_markup, 
        parse_mode='HTML'
    )
    return CHOOSE_CATEGORY

async def choose_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    category_name = query.data.split("cat_", 1)[1]
    context.user_data['current_category'] = category_name
    return await show_services_keyboard(update, context)

async def show_services_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    category_name = context.user_data['current_category']
    all_services = context.user_data.get('services_list', {})
    service_ids = SERVICE_CATEGORIES.get(category_name, [])
    services_to_show = [s for s_id, s in all_services.items() if s_id in service_ids]

    if not services_to_show:
         await query.answer("В этой категории пока нет услуг.", show_alert=True)
         return CHOOSE_CATEGORY

    keyboard = [[InlineKeyboardButton(f"{s['name']} ({s['price']} ₽)", callback_data=f"service_{s['id']}")] for s in services_to_show]
    keyboard.append([InlineKeyboardButton("🔙 Назад к категориям", callback_data="back_to_cat")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = f"<b>Категория: {html.escape(category_name)}</b>\n\nВыберите услугу:"
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CHOOSE_SERVICE

async def back_to_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await show_category_keyboard(update, context)

async def choose_service_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    service_id = int(query.data.split('_')[1])
    context.user_data['current_service_id'] = service_id
    service_name = context.user_data.get('services_list', {}).get(service_id, {}).get('name', 'Unknown')
    context.user_data['current_service_name'] = service_name
    
    await query.edit_message_text(
        text=f"Выбрана услуга: <b>{html.escape(service_name)}</b>.\n\nВведите количество (например: 1, 5, 1.5).",
        parse_mode='HTML',
        reply_markup=None
    )
    return GET_QUANTITY

async def get_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    add_message_to_cleanup(context, update.message.message_id)
    main_msg_id = context.user_data['main_dialog_message_id']
    
    try:
        quantity = float(update.message.text.strip().replace(',', '.'))
        if quantity <= 0: raise ValueError
    except ValueError:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=main_msg_id,
            text="Ошибка. Введите положительное число (например, 1 или 2.5)."
        )
        return GET_QUANTITY

    deal_data = context.user_data['deal_data']
    service_id = context.user_data['current_service_id']
    price = context.user_data.get('services_list', {}).get(service_id, {}).get('price', 0)

    deal_data['services'].append({
        'service_id': service_id, 'quantity': quantity,
        'name': context.user_data['current_service_name'], 'price': price
    })

    keyboard = [
        [InlineKeyboardButton("➕ Добавить еще услугу", callback_data='add_more')],
        [InlineKeyboardButton("✅ Завершить и создать сделку", callback_data='finish')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=main_msg_id,
        text=f"Услуга <b>'{html.escape(context.user_data['current_service_name'])}'</b> x{quantity} добавлена.\n\nДобавить еще или завершить?",
        reply_markup=reply_markup, parse_mode='HTML'
    )
    return ADD_MORE

async def add_more_or_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if query.data == 'add_more':
        return await show_category_keyboard(update, context)
    elif query.data == 'finish':
        return await create_deal_in_api(update, context)

async def wrong_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    add_message_to_cleanup(context, update.message.message_id)
    temp_msg = await update.message.reply_text("Пожалуйста, используйте кнопки для выбора, а не вводите текст.")
    await asyncio.sleep(3)
    await temp_msg.delete()

async def create_deal_in_api(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data['main_dialog_message_id'],
        text="⏳ Создаю сделку в CRM...",
        reply_markup=None
    )
    await cleanup_temp_messages(context, update.effective_chat.id)

    deal_data = context.user_data.get('deal_data')
    if not deal_data or not deal_data.get('services'):
        await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=context.user_data['main_dialog_message_id'], text="Нет данных для создания сделки. /newdeal для старта.")
        return ConversationHandler.END

    # Получаем первую стадию
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{API_BASE_URL}/stages", headers=API_HEADERS)
            r.raise_for_status()
            stage_id = r.json()[0]['id']
    except Exception as e:
        logger.error(f"Не удалось получить ID начального этапа: {e}")
        await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=context.user_data['main_dialog_message_id'], text="Ошибка: не удалось определить начальный этап для сделки.")
        return ConversationHandler.END

    # Определяем имя менеджера: сначала ищем CRM-пользователя по Telegram ID
    tg_uid = str(update.effective_user.id)
    manager_name = update.effective_user.full_name
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{API_BASE_URL}/users/by-telegram/{tg_uid}", headers=API_HEADERS)
            if r.status_code == 200:
                crm_user = r.json()
                manager_name = crm_user.get("name") or manager_name
    except Exception as e:
        logger.warning(f"Не удалось получить CRM-пользователя по Telegram ID: {e}")

    payload = {
        "title": deal_data['title'],
        "contact_id": deal_data.get('contact_id'),
        "new_contact_name": deal_data.get('new_contact_name'),
        "stage_id": stage_id,
        "manager": manager_name,
        "services": [{'service_id': s['service_id'], 'quantity': s['quantity']} for s in deal_data['services']],
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(f"{API_BASE_URL}/deals", json=payload, headers=API_HEADERS)
        
        if response.status_code not in (200, 201):
            error_text = ""
            try:
                error_text = response.json().get('detail', response.text[:200])
            except Exception:
                error_text = response.text[:200]
            logger.error(f"API вернул {response.status_code}: {error_text}")
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=context.user_data['main_dialog_message_id'],
                text=f"❌ <b>Не удалось создать сделку.</b>\nОшибка {response.status_code}: {html.escape(str(error_text))}",
                parse_mode='HTML'
            )
            context.user_data.clear()
            return ConversationHandler.END

        try:
            created = response.json() if response.text else {}
            created_id = created.get('id')
            if created_id:
                context.user_data['last_created_deal_id'] = created_id
        except Exception:
            pass

        total_cost = sum(s.get('price', 0) * s.get('quantity', 0) for s in deal_data['services'])
        total_cost_str = f"{int(total_cost):,} ₽".replace(",", " ")
        services_str = "\n".join([f"  · {html.escape(s['name'])} × {s['quantity']}" for s in deal_data['services']])

        final_text = (
            f"✅ <b>Сделка создана!</b>\n\n"
            f"👤 <b>Клиент:</b> {html.escape(deal_data['client_name'])}\n"
            f"📋 <b>Название:</b> {html.escape(deal_data['title'])}\n\n"
            f"<b>Услуги:</b>\n{services_str}\n\n"
            f"💰 <b>Итого: {total_cost_str}</b>\n"
            f"👷 <b>Менеджер:</b> {html.escape(manager_name)}"
        )
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['main_dialog_message_id'],
            text=final_text,
            parse_mode='HTML'
        )

        # Уведомление в группу
        if TG_CHAT_ID and created_id:
            try:
                _lines = [f"<b>Сделка №{created_id} — {html.escape(deal_data['title'])}</b>"]
                _lines.append(f"Клиент: {html.escape(deal_data['client_name'])}")
                if deal_data.get('services'):
                    _lines.append("")
                    _lines.append("Услуги:")
                    for s in deal_data['services']:
                        _qty = s.get('quantity', 1)
                        _price = float(s.get('price') or 0)
                        _line_total = _price * _qty
                        _price_str = f" — {int(_line_total):,} ₽".replace(",", " ") if _price else ""
                        _lines.append(f"  · {html.escape(s['name'])} × {_qty}{_price_str}")
                if total_cost > 0:
                    _lines.append("")
                    _lines.append(f"Итого: {int(total_cost):,} ₽".replace(",", " "))
                _group_msg = "\n".join(_lines)
                await context.bot.send_message(
                    chat_id=TG_CHAT_ID,
                    text=_group_msg,
                    parse_mode='HTML'
                )
            except Exception as _e:
                logger.warning(f"Не удалось отправить уведомление в группу: {_e}")

    except httpx.RequestError as e:
        logger.error(f"Сетевая ошибка при создании сделки: {e}")
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['main_dialog_message_id'],
            text=f"❌ <b>Сетевая ошибка.</b>\n{html.escape(str(e))}",
            parse_mode='HTML'
        )
    finally:
        last_id = context.user_data.get('last_created_deal_id')
        context.user_data.clear()
        if last_id:
            context.user_data['last_created_deal_id'] = last_id
        return ConversationHandler.END

async def cancel_deal_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await cleanup_temp_messages(context, query.message.chat_id)
    await context.bot.edit_message_text(
        chat_id=query.message.chat_id, 
        message_id=context.user_data['main_dialog_message_id'], 
        text="Действие отменено.",
        reply_markup=None
    )
    last_id = context.user_data.get('last_created_deal_id')
    context.user_data.clear()
    if last_id:
        context.user_data['last_created_deal_id'] = last_id
    return ConversationHandler.END

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await cleanup_temp_messages(context, update.effective_chat.id)
    if 'main_dialog_message_id' in context.user_data:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, 
            message_id=context.user_data['main_dialog_message_id'], 
            text="Действие отменено.",
            reply_markup=None
        )
    else:
        await update.message.reply_text("Действие отменено.")
    last_id = context.user_data.get('last_created_deal_id')
    context.user_data.clear()
    if last_id:
        context.user_data['last_created_deal_id'] = last_id
    return ConversationHandler.END


# ════════════════════════════════════════
#  📅  РАСПИСАНИЕ НА СЕГОДНЯ / ЗАВТРА
# ════════════════════════════════════════

async def schedule_for_day(target_date: date) -> str:
    engine_r = create_engine(DATABASE_URL)
    Session_r = sessionmaker(bind=engine_r)
    from sqlalchemy import func as sa_func
    from datetime import datetime as _dt

    with Session_r() as session:
        deals = (
            session.query(Deal)
            .options(joinedload(Deal.contact), joinedload(Deal.stage))
            .filter(
                Deal.deal_date != None,
                sa_func.date(Deal.deal_date) == target_date,
            )
            .order_by(Deal.deal_date)
            .all()
        )

    engine_r.dispose()

    label = "сегодня" if target_date == date.today() else "завтра"
    day_str = target_date.strftime("%d.%m.%Y")

    if not deals:
        return f"📅 Выездов на {label} ({day_str}) нет."

    lines = [f"📅 <b>Выезды на {label}, {day_str}:</b>\n"]
    for i, d in enumerate(deals, 1):
        client = d.contact.name if d.contact else "Без клиента"
        stage = d.stage.name if d.stage else "—"
        time_str = d.deal_date.strftime("%H:%M") if d.deal_date else "—"
        total = f"{int(d.total or 0):,}".replace(",", " ")
        lines.append(f"<b>{i}. {time_str}</b> — {html.escape(d.title)}")
        lines.append(f"   👤 {html.escape(client)}  |  {stage}  |  {total} руб.")
        if d.address:
            lines.append(f"   📍 {html.escape(d.address)}")
        lines.append("")

    return "\n".join(lines).strip()


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await schedule_for_day(date.today())
    await update.message.reply_text(msg, parse_mode='HTML')


async def tomorrow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await schedule_for_day(date.today() + timedelta(days=1))
    await update.message.reply_text(msg, parse_mode='HTML')


# ════════════════════════════════════════
#  💸  ДОБАВЛЕНИЕ РАСХОДА
# ════════════════════════════════════════

EXPENSE_CATEGORIES = ["Техника", "Топливо", "Расходники", "Реклама", "Запчасти", "Прочее"]


async def newexpense_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    context.user_data['expense_data'] = {}
    add_message_to_cleanup(context, update.message.message_id)

    keyboard = [[InlineKeyboardButton(cat, callback_data=f"expcat_{cat}")] for cat in EXPENSE_CATEGORIES]
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="expcancel")])
    sent = await update.message.reply_text(
        "<b>Новый расход</b>\n\n<b>Шаг 1/3:</b> Выберите категорию:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='HTML'
    )
    context.user_data['exp_main_msg_id'] = sent.message_id
    return EXP_CHOOSE_CATEGORY


async def exp_choose_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    category = query.data.split("expcat_", 1)[1]
    context.user_data['expense_data']['category'] = category

    await query.edit_message_text(
        f"Категория: <b>{html.escape(category)}</b>\n\n"
        "<b>Шаг 2/3:</b> Введите название расхода\n"
        "<i>(например: Бензин АИ-95, Леска для триммера)</i>",
        parse_mode='HTML'
    )
    return EXP_GET_NAME


async def exp_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    add_message_to_cleanup(context, update.message.message_id)

    if not text:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['exp_main_msg_id'],
            text="Название не может быть пустым. Введите ещё раз:"
        )
        return EXP_GET_NAME

    context.user_data['expense_data']['name'] = text
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id,
        message_id=context.user_data['exp_main_msg_id'],
        text=f"Название: <b>{html.escape(text)}</b>\n\n"
             "<b>Шаг 3/3:</b> Введите сумму в рублях (например: 500 или 1250.50):",
        parse_mode='HTML'
    )
    return EXP_GET_AMOUNT


async def exp_get_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip().replace(',', '.')
    add_message_to_cleanup(context, update.message.message_id)

    try:
        amount = float(text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['exp_main_msg_id'],
            text="Неверная сумма. Введите положительное число (например: 500 или 1250.50):"
        )
        return EXP_GET_AMOUNT

    expense = context.user_data['expense_data']
    expense['amount'] = amount

    payload = {
        "name": expense['name'],
        "amount": amount,
        "date": date.today().isoformat(),
        "category": expense['category'],
    }

    try:
        response = requests.post(f"{API_BASE_URL}/expenses", json=payload, timeout=10, headers=API_HEADERS)
        response.raise_for_status()
        amount_str = f"{amount:,.2f}".replace(",", " ").rstrip('0').rstrip('.')
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['exp_main_msg_id'],
            text=f"✅ <b>Расход записан!</b>\n\n"
                 f"📂 <b>Категория:</b> {html.escape(expense['category'])}\n"
                 f"📝 <b>Название:</b> {html.escape(expense['name'])}\n"
                 f"💸 <b>Сумма:</b> {amount_str} руб.\n"
                 f"📅 <b>Дата:</b> {date.today().strftime('%d.%m.%Y')}",
            parse_mode='HTML'
        )
    except requests.RequestException as e:
        error_details = str(e)
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_details = e.response.json().get('detail', e.response.text)
            except Exception:
                error_details = e.response.text
        logger.error(f"Ошибка API при создании расхода: {error_details}")
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=context.user_data['exp_main_msg_id'],
            text=f"❌ <b>Не удалось записать расход.</b>\nОшибка: {html.escape(str(error_details))}",
            parse_mode='HTML'
        )

    last_id = context.user_data.get('last_created_deal_id')
    context.user_data.clear()
    if last_id:
        context.user_data['last_created_deal_id'] = last_id
    return ConversationHandler.END


async def exp_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Отменено.", reply_markup=None)
    last_id = context.user_data.get('last_created_deal_id')
    context.user_data.clear()
    if last_id:
        context.user_data['last_created_deal_id'] = last_id
    return ConversationHandler.END


# ════════════════════════════════════════
#  📊  ОТЧЁТ
# ════════════════════════════════════════

from sqlalchemy import text as sa_text

async def build_report_string() -> str:
    engine_r = create_engine(DATABASE_URL)
    Session_r = sessionmaker(bind=engine_r)
    today = date.today()
    lines = []

    with Session_r() as session:
        # ── Сделки в работе ──
        active_deals = (
            session.query(Deal)
            .join(Stage)
            .filter(Stage.is_final == False)
            .order_by(Stage.order, Deal.created_at.desc())
            .all()
        )

        lines.append(f"🌿 GrassCRM — отчёт за {today.strftime('%d.%m.%Y')}")
        lines.append("")

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

        # ── Задачи ──
        today_tasks = (
            session.query(Task)
            .filter(Task.is_done == False, Task.due_date == today)
            .all()
        )
        overdue_tasks = (
            session.query(Task)
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

        # ── Цитата из daily_phrases ──
        try:
            row = session.execute(
                sa_text("SELECT phrase FROM daily_phrases ORDER BY RANDOM() LIMIT 1")
            ).fetchone()
            if row:
                # Заменяем литеральный \n на реальный перенос строки
                phrase = row[0].replace("\\n", "\n")
                lines.append("· · · · · · · · · ·")
                lines.append(phrase)
        except Exception as e:
            logger.warning(f"Не удалось получить цитату: {e}")

    engine_r.dispose()
    return "\n".join(lines)


async def my_deals_today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    manager_name = update.effective_user.full_name
    tg_uid = str(update.effective_user.id)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{API_BASE_URL}/users/by-telegram/{tg_uid}", headers=API_HEADERS)
            if r.status_code == 200:
                crm_user = r.json()
                manager_name = crm_user.get("name") or manager_name
    except Exception:
        pass

    engine_r = create_engine(DATABASE_URL)
    Session_r = sessionmaker(bind=engine_r)
    from sqlalchemy import func as sa_func
    with Session_r() as session:
        deals = (
            session.query(Deal)
            .options(joinedload(Deal.contact), joinedload(Deal.stage))
            .filter(sa_func.date(Deal.deal_date) == date.today(), Deal.manager == manager_name)
            .order_by(Deal.deal_date)
            .all()
        )
    engine_r.dispose()

    if not deals:
        await update.message.reply_text("На сегодня у вас нет сделок.")
        return

    lines = [f"📋 Ваши сделки на сегодня ({len(deals)}):"]
    for d in deals:
        t = d.deal_date.strftime('%H:%M') if d.deal_date else '--:--'
        client = d.contact.name if d.contact else '—'
        lines.append(f"• {t} — {d.title or 'Без названия'} ({client})")
    await update.message.reply_text("\n".join(lines))


async def edit_last_deal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Использование:\n"
            "/editlastdeal show\n"
            "/editlastdeal add <service_id> <qty>\n"
            "/editlastdeal remove <service_id>"
        )
        return

    deal_id = context.user_data.get('last_created_deal_id')
    if not deal_id:
        await update.message.reply_text("Не нашел последнюю созданную сделку в этой сессии. Сначала создайте новую.")
        return

    action = context.args[0].lower()
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(f"{API_BASE_URL}/deals/{deal_id}", headers=API_HEADERS)
        if r.status_code != 200:
            await update.message.reply_text("Не удалось загрузить сделку.")
            return
        deal = r.json()

        services = []
        for item in deal.get('services', []):
            svc = item.get('service') or {}
            sid = svc.get('id')
            if sid and sid > 0:
                services.append({'service_id': sid, 'quantity': item.get('quantity', 1)})

        if action == 'show':
            if not deal.get('services'):
                await update.message.reply_text(f"Сделка #{deal_id}: услуг пока нет")
                return
            lines=[f"Сделка #{deal_id}: услуги"]
            for item in deal.get('services', []):
                svc=item.get('service') or {}
                lines.append(f"• id={svc.get('id')} {svc.get('name')} × {item.get('quantity')}")
            await update.message.reply_text("\n".join(lines))
            return

        if action == 'add' and len(context.args) >= 3:
            try:
                sid = int(context.args[1]); qty=float(context.args[2])
                if qty <= 0: raise ValueError
            except Exception:
                await update.message.reply_text("Неверный формат. Пример: /editlastdeal add 12 1.5")
                return
            found = False
            for s in services:
                if s['service_id'] == sid:
                    s['quantity'] += qty
                    found = True
                    break
            if not found:
                services.append({'service_id': sid, 'quantity': qty})
        elif action == 'remove' and len(context.args) >= 2:
            try:
                sid = int(context.args[1])
            except Exception:
                await update.message.reply_text("Неверный формат. Пример: /editlastdeal remove 12")
                return
            services = [s for s in services if s['service_id'] != sid]
        else:
            await update.message.reply_text("Неизвестная команда. Используйте show/add/remove")
            return

        patch_payload = {'services': services}
        upd = await client.patch(f"{API_BASE_URL}/deals/{deal_id}", json=patch_payload, headers=API_HEADERS)
        if upd.status_code != 200:
            await update.message.reply_text(f"Не удалось обновить сделку: {upd.status_code}")
            return

    await update.message.reply_text(f"✅ Сделка #{deal_id} обновлена.")




def _extract_deal_id_from_text(text: str):
    if not text:
        return None
    m = re.search(r'(?:#|№|deal\s*)(\d+)', text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _transcribe_with_openai(file_path: str) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY не установлен")
    url = f"{OPENAI_BASE_URL.rstrip('/')}/audio/transcriptions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    with open(file_path, 'rb') as f:
        files = {"file": (os.path.basename(file_path), f, "audio/ogg")}
        data = {"model": "whisper-1", "response_format": "text", "language": "ru"}
        r = requests.post(url, headers=headers, files=files, data=data, timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"Whisper API {r.status_code}: {r.text[:200]}")
        return r.text.strip()


def _transcribe_with_local_whisper(file_path: str) -> str:
    try:
        import whisper  # type: ignore
    except Exception as e:
        raise RuntimeError(f"Локальный whisper недоступен: {e}")
    model = whisper.load_model("base")
    result = model.transcribe(file_path, language="ru")
    text = (result or {}).get("text", "").strip()
    if not text:
        raise RuntimeError("Пустая расшифровка")
    return text


def transcribe_voice_file(file_path: str) -> str:
    errors = []
    try:
        return _transcribe_with_openai(file_path)
    except Exception as e:
        errors.append(f"openai: {e}")
    try:
        return _transcribe_with_local_whisper(file_path)
    except Exception as e:
        errors.append(f"local: {e}")
    raise RuntimeError("; ".join(errors))


async def _save_voice_note_to_deal(deal_id: int, text: str):
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"{API_BASE_URL}/deals/{deal_id}/comments",
            json={"text": text},
            headers=API_HEADERS,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Ошибка API {r.status_code}: {r.text[:200]}")


async def voicenote_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    deal_id = None
    if context.args:
        try:
            deal_id = int(context.args[0])
        except Exception:
            pass

    if deal_id is None and update.message and update.message.reply_to_message:
        reply_text = (update.message.reply_to_message.text or update.message.reply_to_message.caption or "")
        deal_id = _extract_deal_id_from_text(reply_text)

    if deal_id is None:
        await update.message.reply_text(
            "Укажите ID сделки: /voicenote <deal_id>\n"
            "Или ответьте этой командой на сообщение, где есть № сделки."
        )
        return

    context.user_data['voicenote_deal_id'] = deal_id
    context.user_data['voicenote_waiting_voice'] = True
    context.user_data.pop('voicenote_pending_text', None)
    context.user_data.pop('voicenote_editing', None)
    await update.message.reply_text(
        f"🎤 Отправьте голосовое сообщение для сделки #{deal_id}.\n"
        "После расшифровки вы сможете сохранить/исправить/отменить."
    )


async def voicenote_voice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('voicenote_waiting_voice'):
        return
    deal_id = context.user_data.get('voicenote_deal_id')
    if not deal_id:
        await update.message.reply_text("Сначала укажите сделку командой /voicenote <deal_id>")
        return

    msg = await update.message.reply_text("⏳ Расшифровываю голосовое...")
    tmp_path = None
    try:
        voice = update.message.voice
        tg_file = await context.bot.get_file(voice.file_id)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.ogg') as tmp:
            tmp_path = tmp.name
        await tg_file.download_to_drive(custom_path=tmp_path)

        text = await asyncio.to_thread(transcribe_voice_file, tmp_path)
        if not text.strip():
            raise RuntimeError("Не удалось получить текст из голосового сообщения")

        context.user_data['voicenote_pending_text'] = text.strip()
        context.user_data['voicenote_waiting_voice'] = False

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Сохранить как есть", callback_data="vn_save")],
            [InlineKeyboardButton("✏️ Исправить текст", callback_data="vn_edit")],
            [InlineKeyboardButton("❌ Отменить", callback_data="vn_cancel")],
        ])
        await msg.edit_text(
            f"📝 Расшифровка для сделки #{deal_id}:\n\n{text.strip()}",
            reply_markup=kb,
        )
    except Exception as e:
        logger.error(f"Voice note error: {e}")
        await msg.edit_text(f"❌ Не удалось обработать голосовое: {e}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


async def voicenote_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data
    deal_id = context.user_data.get('voicenote_deal_id')
    text = context.user_data.get('voicenote_pending_text', '').strip()

    if action == 'vn_cancel':
        context.user_data.pop('voicenote_pending_text', None)
        context.user_data.pop('voicenote_waiting_voice', None)
        context.user_data.pop('voicenote_editing', None)
        await query.edit_message_text("Отменено. Комментарий не сохранен.")
        return

    if not deal_id or not text:
        await query.edit_message_text("Нет данных для сохранения. Отправьте /voicenote заново.")
        return

    if action == 'vn_edit':
        context.user_data['voicenote_editing'] = True
        await query.edit_message_text(
            f"Введите исправленный текст для сделки #{deal_id} одним сообщением."
        )
        return

    if action == 'vn_save':
        try:
            await _save_voice_note_to_deal(deal_id, text)
            context.user_data.pop('voicenote_pending_text', None)
            context.user_data.pop('voicenote_editing', None)
            await query.edit_message_text(f"✅ Комментарий добавлен в сделку #{deal_id}.")
        except Exception as e:
            await query.edit_message_text(f"❌ Не удалось сохранить: {e}")


async def voicenote_text_edit_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('voicenote_editing'):
        return

    deal_id = context.user_data.get('voicenote_deal_id')
    text = (update.message.text or '').strip()
    if not deal_id:
        await update.message.reply_text("Не найден ID сделки. Запустите /voicenote заново.")
        return
    if not text:
        await update.message.reply_text("Текст пустой, отправьте исправленный комментарий ещё раз.")
        return

    try:
        await _save_voice_note_to_deal(deal_id, text)
        context.user_data.pop('voicenote_pending_text', None)
        context.user_data.pop('voicenote_editing', None)
        await update.message.reply_text(f"✅ Исправленный комментарий добавлен в сделку #{deal_id}.")
    except Exception as e:
        await update.message.reply_text(f"❌ Не удалось сохранить: {e}")

async def send_report_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Отправляю ежедневный отчёт...")
    try:
        msg = await build_report_string()
        await context.bot.send_message(chat_id=TG_CHAT_ID, text=msg)
        logger.info("Отчёт отправлен.")
    except Exception as e:
        logger.error(f"Ошибка при отправке отчёта: {e}")


async def send_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Формирую отчёт...")
    try:
        msg = await build_report_string()
        await context.bot.send_message(chat_id=TG_CHAT_ID, text=msg)
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text(f"Ошибка: {e}")


# ════════════════════════════════════════
#  🚀  ЗАПУСК БОТА
# ════════════════════════════════════════


# ════════════════════════════════════════
#  🖥  STARTUP И СТАТУС СЕРВИСОВ
# ════════════════════════════════════════

async def _post_init(application) -> None:
    """Отправляет уведомление владельцу при старте бота."""
    try:
        await application.bot.send_message(
            chat_id=OWNER_ID,
            text=(
                "Сервер запущен\n\n"
                "Службы:\n"
                "  · Telegram бот — ✅\n"
                "  · CRM API — проверяю..."
            )
        )
        # Проверяем API
        api_ok = False
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(f"{API_BASE_URL}/stages", headers=API_HEADERS)
                api_ok = r.status_code == 200
        except Exception:
            pass

        status = "✅" if api_ok else "❌"
        await application.bot.send_message(
            chat_id=OWNER_ID,
            text=(
                "GrassCRM запущен\n\n"
                f"  · Telegram бот — ✅\n"
                f"  · CRM API — {status}"
            )
        )
    except Exception as e:
        logger.warning(f"Не удалось отправить startup-уведомление: {e}")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /status — только для владельца. Показывает состояние сервисов."""
    if update.effective_user.id != OWNER_ID:
        return

    msg = await update.message.reply_text("Проверяю сервисы...")

    lines = ["Статус сервисов\n"]

    # CRM API
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{API_BASE_URL}/stages", headers=API_HEADERS)
        lines.append(f"CRM API — {'✅' if r.status_code == 200 else '❌ ' + str(r.status_code)}")
    except Exception as e:
        lines.append(f"CRM API — ❌ {e}")

    # Сайт crmpokos.ru
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get("https://crmpokos.ru", follow_redirects=True)
        lines.append(f"crmpokos.ru — {'✅' if r.status_code < 400 else '❌ ' + str(r.status_code)}")
    except Exception as e:
        lines.append(f"crmpokos.ru — ❌ {e}")

    # Сайт покос-ропша.рф
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get("https://xn----8sbgjpqjjbr1b.xn--p1ai", follow_redirects=True)
        lines.append(f"покос-ропша.рф — {'✅' if r.status_code < 400 else '❌ ' + str(r.status_code)}")
    except Exception as e:
        lines.append(f"покос-ропша.рф — ❌ {e}")

    # Основной бот (себя проверяем через getMe)
    try:
        me = await context.bot.get_me()
        lines.append(f"Бот @{me.username} — ✅")
    except Exception as e:
        lines.append(f"Основной бот — ❌ {e}")

    # Ассистент-бот (проверяем через getMe с его токеном)
    if ASSISTANT_BOT_TOKEN:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(
                    f"https://api.telegram.org/bot{ASSISTANT_BOT_TOKEN}/getMe"
                )
                data = r.json()
            if data.get("ok"):
                uname = data["result"].get("username", "assistant")
                lines.append(f"Бот @{uname} — ✅")
            else:
                lines.append(f"Ассистент-бот — ❌ {data.get('description', 'ошибка')}")
        except Exception as e:
            lines.append(f"Ассистент-бот — ❌ {e}")

    result = lines[0] + "\n\n" + "\n".join(f"  · {l}" for l in lines[1:])
    await msg.edit_text(result)


def main() -> None:
    if not TG_TOKEN:
        logger.critical("Переменная окружения TELEGRAM_BOT_TOKEN не установлена!")
        sys.exit(1)

    application = Application.builder().token(TG_TOKEN).post_init(_post_init).build()

    if TG_CHAT_ID:
        job_queue = application.job_queue
        report_time = time(hour=18, minute=0, tzinfo=pytz.timezone('Europe/Moscow'))
        job_queue.run_daily(send_report_job, time=report_time)
    else:
        logger.warning("TELEGRAM_CHAT_ID не установлен: авто-отчет отключен, ручные команды доступны.")

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("newdeal", new_deal_start),
            MessageHandler(filters.Regex(r"(?i)^\s*новая\s+сделка\s*$"), new_deal_start),
        ],
        states={
            GET_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_deal_title)],
            GET_CLIENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_client_name)],
            PICK_CONTACT: [
                CallbackQueryHandler(cancel_deal_callback, pattern="^cancel_deal$"),
                CallbackQueryHandler(pick_contact_callback, pattern="^pick_contact_"),
            ],
            CHOOSE_CATEGORY: [
                CallbackQueryHandler(cancel_deal_callback, pattern="^cancel_deal$"),
                CallbackQueryHandler(choose_category_callback, pattern="^cat_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, wrong_input_handler)
            ],
            CHOOSE_SERVICE: [
                CallbackQueryHandler(back_to_category_callback, pattern="^back_to_cat$"),
                CallbackQueryHandler(choose_service_callback, pattern="^service_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, wrong_input_handler)
            ],
            GET_QUANTITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_quantity)],
            ADD_MORE: [
                CallbackQueryHandler(add_more_or_finish_callback, pattern="^(add_more|finish)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, wrong_input_handler)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_command)],
        conversation_timeout=900
    )

    application.add_handler(conv_handler)

    expense_handler = ConversationHandler(
        entry_points=[CommandHandler("newexpense", newexpense_start)],
        states={
            EXP_CHOOSE_CATEGORY: [
                CallbackQueryHandler(exp_cancel_callback, pattern="^expcancel$"),
                CallbackQueryHandler(exp_choose_category_callback, pattern="^expcat_"),
            ],
            EXP_GET_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, exp_get_name)],
            EXP_GET_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, exp_get_amount)],
        },
        fallbacks=[CommandHandler("cancel", cancel_command)],
        conversation_timeout=300
    )
    application.add_handler(expense_handler)

    application.add_handler(CommandHandler("start", lambda u, c: u.message.reply_text(
        "Привет! Я CRM-бот 🌿\n\n"
        "/newdeal — создать сделку\n"
        "/newexpense — записать расход\n"
        "/today — выезды на сегодня\n"
        "/tomorrow — выезды на завтра\n"
        "/sendreport — отправить отчёт\n"
        "/mydeals — мои сделки на сегодня\n"
        "/editlastdeal — редактировать последнюю сделку\n"
        "/voicenote — голосовая заметка к сделке"
    )))
    application.add_handler(CommandHandler("today", today_command))
    application.add_handler(CommandHandler("tomorrow", tomorrow_command))
    application.add_handler(CommandHandler("sendreport", send_report_command))
    application.add_handler(CommandHandler("mydeals", my_deals_today_command))
    application.add_handler(CommandHandler("editlastdeal", edit_last_deal_command))
    application.add_handler(CommandHandler("voicenote", voicenote_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CallbackQueryHandler(voicenote_callback, pattern="^vn_(save|edit|cancel)$"))
    application.add_handler(MessageHandler(filters.VOICE, voicenote_voice_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, voicenote_text_edit_handler))

    logger.info("Бот запускается...")
    application.run_polling()

if __name__ == "__main__":
    main()
