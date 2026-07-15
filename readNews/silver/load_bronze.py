from typing import List, Dict
from pathlib import Path
from sqlalchemy import text

root_path = Path(__file__).resolve().parent.parent
from database import get_async_session_factory

async def load_news_from_db() -> List[Dict]:
    """
    Загружает новости из таблицы bronze, которые еще не обработаны в silver.
    Использует пул подключений SQLAlchemy для стабильности в Docker.
    """
    
    async_session_factory = get_async_session_factory()
    
    
    async with async_session_factory() as session:
        try:
            
            query = text("""
                SELECT
                    id,
                    source_name,
                    url,
                    title,
                    content,
                    date
                FROM bronze b
                WHERE NOT EXISTS (
                    SELECT 1 FROM silver s
                    WHERE s.url = b.url
                    OR s.id = b.id
                )
                ORDER BY date DESC
            """)

            result = await session.execute(query)
            
            rows = result.mappings().all()

            if not rows:
                return []

            news = []
            for row in rows:
                
                title = row["title"] or ""
                content = row["content"] or ""
                full_text = f"{title}\n{content}".strip()
                
                news.append({
                    "id": row["id"],
                    "text": full_text,
                    "content": content,
                    "source": row["source_name"],  
                    "timestamp": str(row["date"]) if row["date"] else "", 
                    "url": row["url"],
                    "title": title,
                    "normalized_text": "",
                    "embedding": []
                })

            return news

        except Exception as e:
            print(f"Ошибка при загрузке из БД: {e}")
            raise
            
        finally:
            
            await async_session_factory.kw['bind'].dispose()
