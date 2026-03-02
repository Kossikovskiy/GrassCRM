"""
Создание пользователей в базе данных.
Запуск: python scripts/create_users.py

Запускайте каждый раз когда нужно добавить/изменить пользователя.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime
from models.database import get_engine, get_session_factory, init_db, Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime

# Импортируем или создаём модель User
try:
    from models.user import User
except ImportError:
    # Если файл не скопировали — создаём здесь
    class User(Base):
        __tablename__ = "users"
        id            = Column(Integer, primary_key=True)
        username      = Column(String(50), unique=True, nullable=False)
        password_hash = Column(String(200), nullable=False)
        full_name     = Column(String(100), default="")
        role          = Column(String(20), default="user")
        is_active     = Column(Boolean, default=True)
        created_at    = Column(DateTime, default=datetime.utcnow)
        last_login    = Column(DateTime, nullable=True)

from api.auth_module import hash_password

# ════════════════════════════════════════
#  НАСТРОЙТЕ ПОЛЬЗОВАТЕЛЕЙ ЗДЕСЬ
# ════════════════════════════════════════

USERS_TO_CREATE = [
    {
        "username":  "kossikovskiy",
        "password":  "950fg2206b54kos27",      # ← смените на свой пароль!
        "full_name": "Администратор",
        "role":      "admin",
    },
    {
        "username":  "manager",
        "password":  "Fgfgfgbi7",       # ← смените на свой пароль!
        "full_name": "Менеджер",
        "role":      "manager",
    },
    {
        "username":  "User",
        "password":  "Q85x3DF6565Rd",        # ← смените на свой пароль!
        "full_name": "Сотрудник",
        "role":      "user",
    },
]

# ════════════════════════════════════════

def main():
    engine = get_engine()

    # Создаём таблицу users если не существует
    Base.metadata.create_all(engine)

    Session = get_session_factory(engine)

    with Session() as session:
        for u in USERS_TO_CREATE:
            existing = session.query(User).filter_by(username=u["username"]).first()

            if existing:
                # Обновляем пароль и данные существующего
                existing.password_hash = hash_password(u["password"])
                existing.full_name     = u["full_name"]
                existing.role          = u["role"]
                existing.is_active     = True
                print(f"🔄 Обновлён:   {u['username']} ({u['role']})")
            else:
                # Создаём нового
                user = User(
                    username      = u["username"],
                    password_hash = hash_password(u["password"]),
                    full_name     = u["full_name"],
                    role          = u["role"],
                )
                session.add(user)
                print(f"✅ Создан:     {u['username']} ({u['role']})")

        session.commit()

    print()
    print("🔐 Пароли зашифрованы и сохранены в базе данных.")
    print("   Никто не сможет их прочитать — даже вы.")
    print()
    print("Для входа используйте логины выше.")


if __name__ == "__main__":
    main()
