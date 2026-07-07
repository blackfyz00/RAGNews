import asyncpg
from readNews.DB_CONFIG import DB_CONFIG

async def save_silver_to_db(silver_news: list[dict]):
    """
    Сохраняет обработанные новости в таблицу silver.
    """
    conn = None
    try:
        conn = await asyncpg.connect(**DB_CONFIG)

        for item in silver_news:
            embeddings_list = item.get("embeddings", [])
            embeddings_str = '[' + ', '.join(str(x) for x in embeddings_list) + ']'

            query = """
                INSERT INTO silver (
                    source_name,
                    url,
                    date,
                    normalized_title,
                    normalized_content,
                    links,
                    hash,
                    embeddings
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector)
                ON CONFLICT (id) DO UPDATE SET
                    normalized_title = EXCLUDED.normalized_title,
                    normalized_content = EXCLUDED.normalized_content,
                    links = EXCLUDED.links,
                    hash = EXCLUDED.hash,
                    embeddings = EXCLUDED.embeddings
            """

            await conn.execute(
                query,
                item.get("source_name"),
                item.get("url"),
                item.get("date"),
                item.get("normalized_title"),
                item.get("normalized_content"),
                item.get("links", []),
                item.get("hash", ""),
                embeddings_str
            )

        print(f"Сохранено {len(silver_news)} новостей в таблицу silver")

    except Exception as e:
        print(f"Ошибка при сохранении в БД: {e}")
        raise
    finally:
        if conn:
            await conn.close()
