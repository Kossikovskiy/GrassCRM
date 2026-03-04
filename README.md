CRM-система на FastAPI

Это бэкенд CRM-системы, разработанный на Python с использованием фреймворка FastAPI. Приложение развернуто на Linux-сервере в качестве системной службы (systemd) и использует PostgreSQL в качестве базы данных.

## Технологический стек

- **Бэкенд:** Python 3, FastAPI
- **Веб-сервер:** Uvicorn
- **База данных:** PostgreSQL
- **Окружение:** systemd, Venv
- **Дополнительно:** Интеграция с Telegram-ботом (`bot.py`)

## Структура проекта

```
/
├── .env                  # Файл с переменными окружения (БД, токены)
├── main.py               # Основное приложение FastAPI
├── bot.py                # Логика для Telegram-бота
├── requirements.txt      # Зависимости Python
├── start.sh              # Скрипт для запуска uvicorn в production
└── ...
```

## Развертывание (Production на Linux)

Это руководство описывает, как было настроено приложение для работы на сервере.

### 1. Расположение файлов

Проект расположен в директории `/var/www/crm/GCRM-2/`. Виртуальное окружение находится в `/var/www/crm/venv/`.

### 2. Скрипт запуска (`start.sh`)

Для корректного запуска приложения из-под `systemd` используется bash-скрипт, который активирует виртуальное окружение.

**`start.sh`:**
```bash
#!/bin/bash
source /var/www/crm/venv/bin/activate
/var/www/crm/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000
```
*Скрипту необходимо дать права на исполнение (`chmod +x start.sh`).*

### 3. Служба `systemd`

Приложением управляет `systemd`. Конфигурационный файл службы находится в `/etc/systemd/system/crm.service`.

**`crm.service`:**
```ini
[Unit]
Description=Grass CRM FastAPI
After=network.target

[Service]
User=root
Group=www-data
WorkingDirectory=/var/www/crm/GCRM-2
ExecStart=/var/www/crm/GCRM-2/start.sh
Restart=always

[Install]
WantedBy=multi-user.target
```

### 4. Управление службой

Основные команды для управления сервисом:
- `systemctl daemon-reload`      # Перечитать конфигурацию после изменений в .service файле
- `systemctl enable crm.service`     # Включить автозапуск при старте системы
- `systemctl start crm.service`      # Запустить сервис
- `systemctl restart crm.service`    # Перезапустить сервис
- `systemctl status crm.service`     # Проверить статус сервиса

## Локальная разработка

### 1. Установка
```bash
# Клонировать репозиторий
git clone [URL_РЕПОЗИТОРИЯ]
cd GCRM-2

# Создать и активировать виртуальное окружение
python3 -m venv venv
source venv/bin/activate

# Установить зависимости
pip install -r requirements.txt
```

### 2. Настройка `.env`
Создайте файл `.env` в корне проекта и укажите в нем необходимые переменные, как минимум URL для подключения к базе данных.
```
DATABASE_URL="postgresql://user:password@host:port/dbname"
TELEGRAM_BOT_TOKEN="ВАШ_ТОКЕН"
```

### 3. Запуск
```bash
# Запустить приложение в режиме разработки
uvicorn main:app --reload --port 8000
```
API будет доступно по адресу `http://127.0.0.1:8000`. Автоматическая документация Swagger UI находится по адресу `http://127.0.0.1:8000/docs`.
