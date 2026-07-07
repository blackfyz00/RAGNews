import asyncpg
from typing import List, Dict
from readNews.DB_CONFIG import DB_CONFIG

async def load_news_from_db() -> List[Dict]:
    """
    Загружает новости из таблицы bronze, которые еще не обработаны в silver.
    """
    conn = None
    try:
        conn = await asyncpg.connect(**DB_CONFIG)

        query = """
            SELECT
                id,
                source_name as source,
                url,
                title,
                content,
                date as timestamp
            FROM bronze b
            WHERE NOT EXISTS (
                SELECT 1 FROM silver s
                WHERE s.url = b.url
                OR s.id = b.id
            )
            ORDER BY date DESC
        """

        rows = await conn.fetch(query)

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
                "source": row["source"],
                "timestamp": str(row["timestamp"]) if row["timestamp"] else "",
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
        if conn:
            await conn.close()
