FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY requirements.txt .

# Идеально: быстро собирает и не раздувает образ
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r requirements.txt

COPY . .

# ИСПРАВЛЕНО: синтаксис для корректного запуска
CMD ["uv", "run", "python", "main.py"]
