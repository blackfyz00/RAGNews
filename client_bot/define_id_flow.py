import httpx
from typing import Optional
import logging

logger = logging.getLogger("ragnews-bot")

async def get_deployment_id_by_name(deployment_name: str, api_url: str) -> Optional[str]:
    """Динамически получает ID деплоймента по его имени через официальный фильтр Prefect API."""
    # Убеждаемся, что в конце URL нет лишнего слэша
    api_url = api_url.rstrip("/")
    
    # Prefect API требует POST-запрос на /deployments/filter для точного поиска
    url = f"{api_url}/deployments/filter"
    
    # Тело запроса со строгим фильтром по имени деплоймента
    payload = {
        "deployments": {
            "operator": "and_",
            "name": {
                "any_": [deployment_name]
            }
        }
    }
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            logger.info(f"Запрос ID деплоймента '{deployment_name}' через {url}")
            response = await client.post(url, json=payload)
            response.raise_for_status()
            
            deployments = response.json()
            
            if not deployments:
                logger.error(f"Deployment '{deployment_name}' не найден в системе Prefect (вернулся пустой список).")
                return None
            
            # Элемент найден — извлекаем id
            deployment_id = deployments[0].get("id")
            if deployment_id:
                logger.info(f"Успешно найден деплоймент '{deployment_name}' с ID: {deployment_id}")
                return deployment_id
                
            logger.error(f"Деплоймент '{deployment_name}' найден, но в ответе API отсутствует поле 'id'.")
            return None
            
    except httpx.ConnectError:
        logger.error(f"Не удалось подключиться к Prefect API по адресу: {api_url}")
        return None
    except httpx.HTTPStatusError as e:
        logger.error(f"Ошибка Prefect API: Код ответа {e.response.status_code}. Тест: {e.response.text}")
        return None
    except Exception as e:
        logger.exception(f"Непредвиденная ошибка при получении deployment_id: {e}")
        return None
