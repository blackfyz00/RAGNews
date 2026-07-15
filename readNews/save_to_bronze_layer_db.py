import os
from datetime import datetime
from prefect import task, get_run_logger
from dotenv import load_dotenv
from sqlalchemy import text

from ENV_PATH import ENV_PATH
from database import get_async_session_factory 

load_dotenv(ENV_PATH)

@task(retries=2, retry_delay_seconds=15, name="Сохранение в Бронзовый слой БД (SQLAlchemy)")
async def save_to_bronze_layer_db(data: list[dict]) -> list[dict]:
    """
    Принимает список новостей (payload) и сохраняет их в таблицу bronze через SQLAlchemy.
    """
    logger = get_run_logger()
    
    if not data:
        logger.info("ℹ️ Нет данных для сохранения в базу данных.")
        return []

    saved_news = []
    logger.info(f"💾 Начинаю импорт {len(data)} новостей в PostgreSQL через SQLAlchemy...")
    
    
    async_session_factory = get_async_session_factory()
    
    
    async with async_session_factory() as session:
        try:
            
            async with session.begin():
                for item in data:
                    url = item.get("url", "")
                    url = url.strip() if url else ""
                    
                    if not url:
                        continue
                    
                    check_query = text("SELECT EXISTS(SELECT 1 FROM bronze WHERE url = :url)")
                    result = await session.execute(check_query, {"url": url})
                    exists = result.scalar()
                    
                    if exists:
                        continue
                        
                    full_content = item.get("content", "") or ""
                    
                    if full_content and "\n" in full_content:
                        title = full_content.split("\n")[0].strip()
                    else:
                        title = full_content[:100].strip() if full_content else "Без названия"
                    
                    raw_date = item.get("timestamp", "")
                    parsed_date = datetime.now().date()
                    
                    if raw_date and isinstance(raw_date, str):
                        try:
                            date_parts = raw_date.split()
                            if date_parts:
                                parsed_date = datetime.strptime(date_parts[0], "%Y-%m-%d").date()
                        except Exception:
                            pass

                    insert_query = text("""
                        INSERT INTO bronze (source_name, url, title, content, date)
                        VALUES (:source_name, :url, :title, :content, :date)
                    """)
                    
                    await session.execute(
                        insert_query,
                        {
                            "source_name": item.get("source"),
                            "url": url,
                            "title": title,
                            "content": full_content,
                            "date": parsed_date
                        }
                    )
                    saved_news.append(item)
                    
            logger.info(f"✅ Успешно сохранено новых новостей в БД: {len(saved_news)}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при работе с базой данных: {e}")
            raise e
            
    
    await async_session_factory.kw['bind'].dispose()
    
    return saved_news
