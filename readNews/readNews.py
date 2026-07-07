import os
import asyncio
from prefect import flow
from dotenv import load_dotenv
from bronze_pipeline import bronze_pipeline
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

from silver.generate_silver_layer import silver_pipeline
from gold_layer.silver_to_gold import gold_pipeline

load_dotenv('telegram_creds.env')
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')

# ---- ГЛАВНЫЙ ПОТОК (FLOW) ----

@flow(name="Главный Новостной Конвейер: Medallion")
async def main_pipeline():
    await bronze_pipeline()
    await silver_pipeline()
    await gold_pipeline()

if __name__ == "__main__":
    import os
    # Говорим скрипту смотреть на локальный сервер
    os.environ["PREFECT_API_URL"] = "http://127.0.0"
    
    # serve() регистрирует флоу и превращает скрипт в постоянного воркера
    main_pipeline.serve(
        name="medallion-cron-deployment",
        cron="*/90 * * * *"  # Будет сам запускаться каждые 15 минут
    )
