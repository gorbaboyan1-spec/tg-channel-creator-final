#!/bin/bash
# Двойной клик по этому файлу запустит всё автоматически

cd "$(dirname "$0")"

echo "=================================================="
echo "  TG Channel Creator — Автозапуск"
echo "=================================================="
echo ""

# Проверяем python3
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 не найден."
    echo ""
    echo "Установи Homebrew командой:"
    echo '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
    echo ""
    echo "Потом: brew install python"
    echo ""
    read -p "Нажми Enter для выхода..."
    exit 1
fi

echo "✓ Python3 найден: $(python3 --version)"
echo ""

# Проверяем pip3
if ! command -v pip3 &> /dev/null; then
    echo "⚠️  pip3 не найден, пробую через python3 -m pip..."
    PIP="python3 -m pip"
else
    PIP="pip3"
fi

# Устанавливаем зависимости если нужно
echo "📦 Проверяю зависимости..."
$PIP install -q flask flask-cors telethon python-telegram-bot google-generativeai 2>&1 | grep -E "(Successfully|already|ERROR)"

echo ""
echo "✓ Зависимости установлены"
echo ""
# Завершаем старые процессы если есть
pkill -f "python3 web_api.py" 2>/dev/null; sleep 0.5

echo "🚀 Запускаю сервер..."
echo "📌 Открой браузер: http://localhost:5000"
echo ""
echo "Для остановки нажми Ctrl+C"
echo "=================================================="
echo ""

python3 -u web_api.py
