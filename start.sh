#!/bin/bash
# Рабочая директория и переменные окружения (.env)
# теперь устанавливаются в файле /etc/systemd/system/crm.service

# Активируем виртуальное окружение
source /var/www/crm/venv/bin/activate

# Просто запускаем приложение
exec /var/www/crm/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000
