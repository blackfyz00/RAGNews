from prefect import flow
import asyncio
import sys
from pathlib import Path
from utils import *
from normalize import *
from embeddings import *
from deduplicate import *
from datetime import datetime

root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from load_bronze import load_news_from_db
from save_to_silver_layer_db import save_silver_to_db


from silver.load_silver import load_silver_data

@flow(name="Новостной конвейер: Серебряный Слой")
async def silver_pipeline():
    """
    Основная функция обработки новостей (Silver слой)
    """
    print("\nЗАПУСК SILVER LAYER\n")
    
    news = await load_news_from_db()
    
    if not news:
        print("Нет новых новостей для обработки")
        return

    for item in news:
        if not item.get("id"):
            item["id"] = generate_news_id()

    news = normalize_news_list(news)

    provider = GigaChatEmbeddingProvider()
    news = provider.add_embeddings(news)
    
    silver_data = await load_silver_data()

    finder = DuplicateFinder(threshold=0.92, silver_data=silver_data)
    finder.print_top_pairs(news)  # Вывод дубликатов
    news = finder.merge_duplicates(news)
    
    duplicates_to_update = getattr(finder, 'duplicates_to_update', [])
    links_to_add = {}
    for dup in duplicates_to_update:
        silver_id = dup.get("silver_id")
        new_url = dup.get("new_url")
        if silver_id and new_url:
            if silver_id not in links_to_add:
                links_to_add[silver_id] = []
            links_to_add[silver_id].append(new_url)

    silver_news = []
    for item in news:
        current_links = item.get("links", [item.get("url", "")])

        silver_id = item.get("id")
        if silver_id and silver_id in links_to_add:
            for url in links_to_add[silver_id]:
                if url not in current_links:
                    current_links.append(url)
            print(f"   ✅ Добавлены ссылки в links для id={silver_id}: {links_to_add[silver_id]}")
        
        silver_news.append({
            "id": item.get("id"),
            "source_name": item.get("source"),
            "url": item.get("url"),
            "date": datetime.strptime(item.get("timestamp", "").split()[0], "%Y-%m-%d").date() 
                    if item.get("timestamp") else None,
            "normalized_title": item.get("title", ""),
            "normalized_content": item.get("normalized_text", ""),
            "links": current_links,
            "hash": item.get("hash", ""),
            "embeddings": item.get("embedding", [])
        })

    await save_silver_to_db(silver_news)

    print("SILVER LAYER ЗАВЕРШЕН!")
    print(f"Обработано новостей: {len(silver_news)}")
