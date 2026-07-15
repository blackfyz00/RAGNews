FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY requirements.txt .

RUN uv pip install --no-cache-dir --system -r requirements.txt

COPY . .

EXPOSE 8210

CMD ["sh", "-c", "uv run prefect server start --host 0.0.0.0 --port 8210 & sleep 20 && uv run python readNews.py"]
