import sys
from pathlib import Path
from sqlalchemy import text

root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from database import get_async_session_factory

async def save_silver_to_db(silver_news: list[dict]):
    """
    Сохраняет обработанные новости в таблицу silver через SQLAlchemy пул.
    """
    if not silver_news:
        return

    async_session_factory = get_async_session_factory()
    
    async with async_session_factory() as session:
        try:
            async with session.begin():
                for item in silver_news:
                    embeddings_list = item.get("embeddings", [])
                    
                    
                    if embeddings_list and isinstance(embeddings_list, list):
                        embeddings_str = '[' + ', '.join(str(x) for x in embeddings_list) + ']'
                    else:
                        embeddings_str = None

                    query = text("""
                        INSERT INTO silver (
                            id,
                            source_name,
                            url,
                            date,
                            normalized_title,
                            normalized_content,
                            links,
                            hash,
                            embeddings
                        ) VALUES (
                            :id, 
                            :source_name, 
                            :url, 
                            :date, 
                            :normalized_title, 
                            :normalized_content, 
                            :links, 
                            :hash, 
                            CAST(:embeddings AS vector)
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            normalized_title = EXCLUDED.normalized_title,
                            normalized_content = EXCLUDED.normalized_content,
                            links = EXCLUDED.links,
                            hash = EXCLUDED.hash,
                            embeddings = EXCLUDED.embeddings;
                    """)

                    await session.execute(
                        query,
                        {
                            "id": item.get("id"),
                            "source_name": item.get("source_name"),
                            "url": item.get("url"),
                            "date": item.get("date"),
                            "normalized_title": item.get("normalized_title"),
                            "normalized_content": item.get("normalized_content"),
                            "links": item.get("links", []),
                            "hash": item.get("hash", ""),
                            "embeddings": embeddings_str  
                        }
                    )

            print(f"Сохранено {len(silver_news)} новостей в таблицу silver")

        except Exception as e:
            print(f"Ошибка при сохранении в БД (Silver слой): {e}")
            raise
            
        finally:
            await async_session_factory.kw['bind'].dispose()
