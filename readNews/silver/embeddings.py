import os
import asyncio
import logging
from typing import List, Dict, Any
from dotenv import load_dotenv
from gigachat import GigaChat
from gigachat.exceptions import RateLimitError, GigaChatException

load_dotenv()

logger = logging.getLogger(__name__)


class GigaChatEmbeddingProvider:
    def __init__(self):
        self.client = GigaChat(
            credentials=os.getenv("GIGACHAT_CREDENTIALS"),
            scope=os.getenv("GIGACHAT_SCOPE"),
            verify_ssl_certs=False,
            timeout=60,                    
        )

    def prepare_embedding_text(self, news: Dict[str, Any], max_chars: int = 1000) -> str:
        title = news.get("title", "") or ""
        body = (
            news.get("normalized_text")
            or news.get("text")
            or news.get("content")
            or ""
        )
        text = f"TITLE: {title}\nCONTENT: {body}".strip()

        if len(text) > max_chars:
            try:
                text = text[:max_chars].rsplit(" ", 1)[0]
            except Exception:
                text = text[:max_chars]
        return text

    def get_embedding(self, text: str) -> List[float]:
        """Получение эмбеддинга с обработкой Rate Limit"""
        for attempt in range(7):  
            try:
                response = self.client.embeddings(texts=[text])
                return response.data[0].embedding

            except RateLimitError:
                wait_seconds = (2 ** attempt) * 7  
                logger.warning(
                    f"RateLimitError при получении эмбеддинга. "
                    f"Попытка {attempt + 1}/7. Ждём {wait_seconds} сек."
                )
                asyncio.sleep(wait_seconds)
                continue

            except GigaChatException as e:
                logger.error(f"GigaChat error: {e}")
                if attempt == 6:
                    raise
                asyncio.sleep(5)

            except Exception as e:
                logger.exception(f"Неожиданная ошибка при эмбеддинге: {e}")
                if attempt == 6:
                    raise
                asyncio.sleep(3)

        raise RuntimeError("Не удалось получить embedding после всех попыток")

    def add_embeddings(self, news_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        total = len(news_list)
        for i, news in enumerate(news_list, start=1):
            print(f"Embedding {i}/{total}")
            
            text = self.prepare_embedding_text(news)
            if not text.strip():
                text = "Пустая новость"

            embedding = self.get_embedding(text)
            news["embedding"] = embedding

            
            if i % 4 == 0:
                asyncio.sleep(1.8)
            else:
                asyncio.sleep(0.7)   

        return news_list