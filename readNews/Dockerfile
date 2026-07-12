FROM python:3.11-slim

# Устанавливаем uv напрямую из официального Docker-образа
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Копируем файл зависимостей
COPY requirements.txt .

# Системное окружение Python (без venv внутри контейнера)
# --system указывает uv ставить пакеты прямо в систему, как обычный pip
RUN uv pip install --no-cache-dir --system -r requirements.txt

COPY . .

EXPOSE 8210

# Сервер запускается в фоне, ждем 20 секунд завершения тяжелых миграций базы, стартуем скрипт
CMD ["sh", "-c", "uv run prefect server start --host 0.0.0.0 --port 8210 & sleep 20 && uv run python readNews.py"]
