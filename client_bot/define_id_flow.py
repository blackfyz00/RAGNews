# define_id_flow.py
import httpx
from typing import Optional
import logging

logger = logging.getLogger("ragnews-bot")

async def get_deployment_id_by_name(deployment_name: str, api_url: str) -> Optional[str]:
    """Динамически получает ID деплоймента по его имени через Prefect API"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{api_url}/deployments")
            response.raise_for_status()
            deployments = response.json()
            
            for deployment in deployments:
                if (deployment.get("name") == deployment_name or 
                    deployment.get("deployment_name") == deployment_name):
                    deployment_id = deployment.get("id") or deployment.get("deployment_id")
                    if deployment_id:
                        logger.info(f"Found deployment '{deployment_name}' with ID: {deployment_id}")
                        return deployment_id
            
            logger.error(f"Deployment '{deployment_name}' not found")
            return None
            
    except httpx.ConnectError:
        logger.error(f"Cannot connect to Prefect API at {api_url}")
        return None
    except Exception as e:
        logger.exception(f"Error getting deployment ID: {e}")
        return None