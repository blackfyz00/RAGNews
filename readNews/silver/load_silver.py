import asyncpg
import json
import numpy as np
from typing import List, Dict
from pathlib import Path

root_path = Path(__file__).resolve().parent.parent
from DB_CONFIG import DB_CONFIG

async def load_silver_data() -> List[Dict]:
    """
    Загружает существующие новости из silver для проверки на дубликаты.
    Возвращает список словарей с полями: id, url, title, embedding
    """
    silver_data = []
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        rows = await conn.fetch(
            "SELECT id, url, normalized_title as title, embeddings FROM silver WHERE embeddings IS NOT NULL"
        )
        for row in rows:
            if row["embeddings"]:
                emb_str = row["embeddings"].replace("'", '"')
                emb_list = json.loads(emb_str)
                silver_data.append({
                    "id": row["id"],
                    "url": row["url"],
                    "title": row["title"],
                    "embedding": np.array(emb_list)
                })
        return silver_data
    finally:
        await conn.close()
