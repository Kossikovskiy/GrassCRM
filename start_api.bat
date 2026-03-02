@echo off
cd /d "\CRM_Green"
uvicorn api.main:app --reload --port 8000
pause