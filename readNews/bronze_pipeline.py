from prefect import flow
from fetch_news_from_sites import fetch_news_from_sites
from fetch_news_from_telegram import fetch_news_from_telegram
from save_to_bronze_layer_db import save_to_bronze_layer_db

@flow(name="Новостной конвейер: Бронзовый Слой")
async def bronze_pipeline():
    print("🚀 Запуск процесса сбора новостей в Бронзовый слой...")
    print("="*50)
    
    # 1. Парсинг новостей с сайтов и Telegram
    site_news = fetch_news_from_sites(file_path="sites.txt")
    tg_news = fetch_news_from_telegram(file_path="tgch.txt")
    
    # Объединяем массивы payload в один общий бронзовый пул
    raw_bronze_pool = site_news + tg_news
    
    saved_bronze_data = await save_to_bronze_layer_db(
        data=raw_bronze_pool
    )