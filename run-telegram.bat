@echo off
setlocal
cd /d "%~dp0"

if not exist "%~dp0.env" (
  echo Chua co .env - copy tu .env.example va dien token/chat ID.
  exit /b 1
)

python "%~dp0telegram_bot.py" %*
