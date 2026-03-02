"""
Telegram-бот для утренних уведомлений GreenCRM
Запуск вручную:  python scripts/tg_notify.py
Автозапуск:      настраивается через Планировщик задач Windows (см. README)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import urllib.request
import urllib.parse
import json
from datetime import date, timedelta
from models.database import (
    get_engine, get_session_factory,
    Deal, Stage, Maintenance, Equipment
)

# ════════════════════════════════════════
#  ⚙️  НАСТРОЙКИ — заполните свои данные
# ════════════════════════════════════════

TG_TOKEN  = "8620281491:AAFhrxrs5TzMCAl5NCEStaADv9MOX_4PsbE"   # от @BotFather
TG_CHAT   = "-4993820220"                 # ваш chat_id

# ════════════════════════════════════════


def send_message(text: str):
    """Отправить сообщение в Telegram"""
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id":    TG_CHAT,
        "text":       text,
        "parse_mode": "HTML",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data)
        urllib.request.urlopen(req, timeout=10)
        print("✅ Сообщение отправлено!")
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")


def build_morning_report() -> str:
    """Собирает утренний отчёт из базы данных"""
    engine  = get_engine()
    Session = get_session_factory(engine)

    today    = date.today()
    tomorrow = today + timedelta(days=1)
    in_week  = today + timedelta(days=7)

    with Session() as session:

        # ── Сделки "Запланировано" и "В работе" ──
        active_stages = session.query(Stage).filter(
            Stage.name.in_(["Запланировано", "В работе", "Ожидание"])
        ).all()
        active_ids = [s.id for s in active_stages]

        active_deals = session.query(Deal).filter(
            Deal.stage_id.in_(active_ids)
        ).order_by(Deal.created_at).all() if active_ids else []

        # ── Техника которая требует ТО в ближайшую неделю ──
        equip_attention = session.query(Equipment).filter(
            Equipment.next_maintenance <= in_week,
            Equipment.next_maintenance >= today,
            Equipment.status == "active"
        ).all()

        # ── Техника в ремонте ──
        in_repair = session.query(Equipment).filter(
            Equipment.status == "repair"
        ).all()

    # ── Формируем текст сообщения ──────────────────────────

    lines = []
    lines.append(f"🌿 <b>GreenCRM — Доброе утро!</b>")
    lines.append(f"📅 {today.strftime('%d.%m.%Y, %A').replace('Monday','Понедельник').replace('Tuesday','Вторник').replace('Wednesday','Среда').replace('Thursday','Четверг').replace('Friday','Пятница').replace('Saturday','Суббота').replace('Sunday','Воскресенье')}")
    lines.append("")

    # Активные сделки
    if active_deals:
        lines.append(f"📋 <b>Активных сделок: {len(active_deals)}</b>")
        for deal in active_deals[:10]:  # максимум 10 чтобы не спамить
            total = sum(ds.quantity * ds.price_at_moment for ds in deal.deal_services)
            stage_icon = {"Запланировано": "🗓", "В работе": "⚡", "Ожидание": "⏳"}.get(
                deal.stage.name if deal.stage else "", "📌"
            )
            total_str = f"{int(total):,}₽".replace(",", " ") if total else "—"
            lines.append(f"  {stage_icon} <b>{deal.client}</b> — {total_str}")
            lines.append(f"      {deal.title[:50]}")
        if len(active_deals) > 10:
            lines.append(f"  ... и ещё {len(active_deals)-10} сделок")
    else:
        lines.append("📋 <b>Активных сделок нет</b>")
        lines.append("  Самое время создать новые! 💪")

    lines.append("")

    # Техника требует внимания
    if equip_attention:
        lines.append(f"🔧 <b>ТО на этой неделе:</b>")
        for eq in equip_attention:
            days_left = (eq.next_maintenance - today).days
            if days_left == 0:
                when = "сегодня!"
            elif days_left == 1:
                when = "завтра"
            else:
                when = f"через {days_left} дн."
            lines.append(f"  ⚠️ {eq.name} — {when}")

    if in_repair:
        lines.append(f"🛠 <b>В ремонте:</b>")
        for eq in in_repair:
            lines.append(f"  🔴 {eq.name} ({eq.model})")

    if not equip_attention and not in_repair:
        lines.append("🔧 <b>Техника:</b> всё в порядке ✅")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🌿 Хорошего рабочего дня!")

    return "\n".join(lines)


if __name__ == "__main__":
    print("🌿 GreenCRM — отправка утреннего отчёта...")

    if TG_TOKEN == "ВСТАВЬТЕ_ВАШ_ТОКЕН_СЮДА":
        print("❌ Сначала вставьте токен от BotFather в переменную TG_TOKEN!")
        print("   Откройте файл scripts/tg_notify.py и найдите строку TG_TOKEN")
        sys.exit(1)

    report = build_morning_report()
    print("\n" + report + "\n")
    send_message(report)
