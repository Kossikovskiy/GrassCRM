import sys
import os

# Реальный путь к проекту
PROJECT_DIR = '/var/www/u3425316/data/www/crm.покос-ропша.рф'

# Путь к Python в venv (venv лежит прямо в папке проекта)
INTERP = os.path.join(PROJECT_DIR, 'venv', 'bin', 'python3')

if sys.executable != INTERP:
    os.execl(INTERP, INTERP, *sys.argv)

# Добавляем папку проекта в sys.path
sys.path.insert(0, PROJECT_DIR)

# Импортируем приложение
from api.main import application

import logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
