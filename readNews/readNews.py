import os
import asyncio  
from prefect import flow, aserve  
from dotenv import load_dotenv

from bronze_pipeline import bronze_pipeline
from silver.generate_silver_layer import silver_pipeline
from gold_layer.silver_to_gold import gold_pipeline

load_dotenv('telegram_creds.env')
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')



@flow(name="Главный Новостной Конвейер: Medallion")
async def main_pipeline():
    await bronze_pipeline()
    await silver_pipeline()
    await gold_pipeline()

async def create_deployments():
    """
    Асинхронно формирует конфигурацию для двух раздельных деплойментов в Prefect 3.x.
    """
    
    full_monolith_deployment = await main_pipeline.to_deployment(
        name="medallion-cron-deployment",
        cron="*/90 * * * *",
        tags=["full_pipeline"]
    )

    
    gold_layer_deployment = await gold_pipeline.to_deployment(
        name="gold-layer-fast-cron",
        tags=["llm", "gold"]
    )

    
    print("🚀 Воркер успешно инициализирован. Регистрация деплойментов в Prefect 3.x...")
    await aserve(full_monolith_deployment, gold_layer_deployment)

if __name__ == "__main__":
    
    os.environ["PREFECT_API_URL"] = "http://127.0.0"
    
    
    asyncio.run(create_deployments())
