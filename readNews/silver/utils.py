import json
import logging
import uuid
from pathlib import Path
from typing import List, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger("news_pipeline")

def load_json(file_path: str | Path) -> List[Dict[str, Any]]:
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"{file_path} не найден.")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        logger.error("Invalid JSON: %s", file_path)
        return []

    logger.info("Загружено %d новостей", len(data))
    return data

def save_json(data: List[Dict[str, Any]], file_path: str | Path) -> None:
    file_path = Path(file_path)
    tmp_path = file_path.with_suffix(".tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    tmp_path.replace(file_path)

    logger.info("Сохранено %d новостей в %s", len(data), file_path)

def generate_news_id() -> str:
    return str(uuid.uuid4())