FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

# без сохранения кэша (для уменьшения размера образа)
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
