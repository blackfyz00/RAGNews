import os
from typing import List, Dict, Any
from dotenv import load_dotenv
from gigachat import GigaChat

load_dotenv()

class GigaChatEmbeddingProvider:
    def __init__(self):
        # В докере переменные уже в системе, os.getenv сработает отлично
        self.client = GigaChat(
            credentials=os.getenv("GIGACHAT_CREDENTIALS"),
            scope=os.getenv("GIGACHAT_SCOPE"),
            verify_ssl_certs=False,
        )

    def prepare_embedding_text(self, news: Dict[str, Any], max_chars: int = 1500) -> str:
        title = news.get("title", "") or ""
        
        # ИСПРАВЛЕНО: Защита на случай, если очищенный текст лежит в другом ключе
        body = news.get("normalized_text") or news.get("text") or news.get("content") or ""

        text = f"TITLE: {title}\nCONTENT: {body}".strip()

        if len(text) > max_chars:
            # Безопасное усечение по пробелу, чтобы не ломать слово на полуслове
            try:
                text = text[:max_chars].rsplit(" ", 1)[0]
            except Exception:
                text = text[:max_chars]

        return text

    def get_embedding(self, text: str) -> List[float]:
        # Отправляем в API уже гарантированно короткий текст
        response = self.client.embeddings(
            texts=[text]
        )
        return response.data[0].embedding

    def add_embeddings(self, news_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        total = len(news_list)

        for i, news in enumerate(news_list, start=1):
            print(f"Embedding {i}/{total}")

            text = self.prepare_embedding_text(news)
            
            if not text:
                text = "Пустая новость"

            embedding = self.get_embedding(text)
            news["embedding"] = embedding

        return news_list
