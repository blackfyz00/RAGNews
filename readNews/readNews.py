import os
import asyncio
from prefect import flow
from dotenv import load_dotenv
from bronze_pipeline import bronze_pipeline

load_dotenv('telegram_creds.env')
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')

# ---- ГЛАВНЫЙ ПОТОК (FLOW) ----

@flow(name="Главный Новостной Конвейер: Medallion")
async def main_pipeline():
    bronze_data = await bronze_pipeline()
    # silver_data = await silver_pipeline(bronze_data)
    # await gold_pipeline(silver_data)

if __name__ == "__main__":
    import os
    # Говорим скрипту смотреть на локальный сервер
    os.environ["PREFECT_API_URL"] = "http://127.0.0"
    
    # serve() регистрирует флоу и превращает скрипт в постоянного воркера
    main_pipeline.serve(
        name="medallion-cron-deployment",
        cron="*/90 * * * *"  # Будет сам запускаться каждые 15 минут
    )
