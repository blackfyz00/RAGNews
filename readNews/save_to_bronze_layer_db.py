import os
import json
from datetime import datetime
from pathlib import Path
from prefect import task, get_run_logger
import asyncpg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH)

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)) 
}

@task(retries=2, retry_delay_seconds=15, name="Сохранение в Бронзовый слой БД")
async def save_to_bronze_layer_db(data: list[dict]) -> list[dict]:
    """
    Принимает список новостей (payload) и сохраняет их в таблицу bronze.
    Возвращает список только успешно добавленных (новых) новостей.
    """
    logger = get_run_logger()
    
    if not data:
        logger.info("ℹ️ Нет данных для сохранения в базу данных.")
        return []

    saved_news = []
    
    # Подключаемся, используя настройки из env
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        logger.info(f"💾 Начинаю импорт {len(data)} новостей в PostgreSQL...")
        
        for item in data:
            url = item.get("url", "")
            url = url.strip() if url else ""
            
            if not url:
                continue # Пропускаем записи без URL, их невозможно проверить на уникальность
            
            # Проверяем уникальность
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM bronze WHERE url = $1)", url
            )
            
            if exists:
                continue
                
            full_content = item.get("content", "") or ""
            
            # Извлекаем title корректно (ИСПРАВЛЕНО: безопасная обработка строк)
            if full_content and "\n" in full_content:
                title = full_content.split("\n")[0].strip()
            else:
                title = full_content[:100].strip() if full_content else "Без названия"
            
            # Парсинг даты для PostgreSQL типа DATE (ИСПРАВЛЕНО: защита от IndexError)
            raw_date = item.get("timestamp", "")
            parsed_date = datetime.now().date()
            
            if raw_date and isinstance(raw_date, str):
                try:
                    date_parts = raw_date.split()
                    if date_parts:
                        parsed_date = datetime.strptime(date_parts[0], "%Y-%m-%d").date()
                except Exception:
                    pass # Если формат не подошел, останется datetime.now().date()

            # Запись в базу
            await conn.execute(
                """
                INSERT INTO bronze (source_name, url, title, content, date)
                VALUES ($1, $2, $3, $4, $5)
                """,
                item.get("source"),
                url,
                title,
                full_content,
                parsed_date
            )
            
            saved_news.append(item)
            
        logger.info(f"✅ Успешно сохранено новых новостей в БД: {len(saved_news)}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при работе с базой данных: {e}")
        raise e
    finally:
        await conn.close()
        
    return saved_news
