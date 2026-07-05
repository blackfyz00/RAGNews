FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8210

# Сервер запускается в фоне, ждем 20 секунд завершения тяжелых миграций базы, стартуем скрипт
CMD ["sh", "-c", "prefect server start --host 0.0.0.0 --port 8210 & sleep 20 && python readNews.py"]




