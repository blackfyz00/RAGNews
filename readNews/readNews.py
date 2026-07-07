import os
import asyncio  
from prefect import flow, aserve  # ИСПОЛЬЗУЕМ АСИНХРОННЫЙ aserve
from dotenv import load_dotenv

from bronze_pipeline import bronze_pipeline
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

async def create_deployments():
    """
    Асинхронно формирует конфигурацию для двух раздельных деплойментов в Prefect 3.x.
    """
    # 1. Готовим деплоймент для полного конвейера (Раз в 90 минут)
    full_monolith_deployment = await main_pipeline.to_deployment(
        name="medallion-cron-deployment",
        cron="*/90 * * * *",
        tags=["full_pipeline"]
    )

    # 2. Готовим отдельный деплоймент для Gold-слоя (Раз в 15 минут)
    gold_layer_deployment = await gold_pipeline.to_deployment(
        name="gold-layer-fast-cron",
        tags=["llm", "gold"]
    )

    # Запускаем асинхронный воркер, который слушает обе очереди задач
    print("🚀 Воркер успешно инициализирован. Регистрация деплойментов в Prefect 3.x...")
    await aserve(full_monolith_deployment, gold_layer_deployment)

if __name__ == "__main__":
    # Настройка полного и корректного адреса локального API сервера Prefect
    os.environ["PREFECT_API_URL"] = "http://127.0.0"
    
    # Запускаем асинхронную регистрацию деплойментов
    asyncio.run(create_deployments())
