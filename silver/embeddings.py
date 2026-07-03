import os
from typing import List, Dict, Any
from dotenv import load_dotenv
from gigachat import GigaChat

load_dotenv()

# Генерация эмбеддингов, использует embedding_text
class GigaChatEmbeddingProvider:
    def __init__(self):
        self.client = GigaChat(
            credentials=os.getenv("GIGACHAT_CREDENTIALS"),
            scope=os.getenv("GIGACHAT_SCOPE"),
            verify_ssl_certs=False,
        )

    def prepare_embedding_text(self, news: Dict[str, Any], max_chars: int = 1800) -> str:
        title = news.get("title", "")
        body = news.get("normalized_text", "")

        text = f"TITLE: {title}\nCONTENT: {body}".strip()

        if len(text) > max_chars:
            text = text[:max_chars].rsplit(" ", 1)[0]

        return text

    # Получает embedding одного текста
    def get_embedding(self, text: str) -> List[float]:
        response = self.client.embeddings(
            texts=[text]
        )

        return response.data[0].embedding

    # Добавляет embeddings к списку новостей
    def add_embeddings(self, news_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        total = len(news_list)

        for i, news in enumerate(news_list, start=1):
            print(f"Embedding {i}/{total}")

            text = self.prepare_embedding_text(news)
            embedding = self.get_embedding(text)

            news["embedding"] = embedding

        return news_list