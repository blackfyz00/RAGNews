import os
from typing import List, Dict, Any

from dotenv import load_dotenv
from gigachat import GigaChat

load_dotenv()


class GigaChatEmbeddingProvider:
    """
    Генерация эмбеддингов через GigaChat Embeddings.
    
    Использует embedding_text, подготовленный в silver-layer.
    """

    def __init__(self):
        self.client = GigaChat(
            credentials=os.getenv("GIGACHAT_CREDENTIALS"),
            scope=os.getenv("GIGACHAT_SCOPE"),
            verify_ssl_certs=False,
        )

    # -------------------------
    # TEXT PREPARATION
    # -------------------------
    def prepare_embedding_text(self, news: Dict[str, Any], max_chars: int = 1800) -> str:
        """
        Берёт готовый embedding_text из silver-layer.
        Это единый source of truth.
        """

        text = news.get("embedding_text") or ""

        if not text:
            # fallback (на случай старых данных)
            title = news.get("text", "").split("\n")[0]
            body = news.get("normalized_text", "")
            text = f"TITLE: {title}\nCONTENT: {body}"

        text = text.strip()

        # безопасное обрезание без разрыва слов
        if len(text) > max_chars:
            text = text[:max_chars].rsplit(" ", 1)[0]

        return text

    # -------------------------
    # EMBEDDING CALL
    # -------------------------
    def get_embedding(self, text: str) -> List[float]:
        """
        Получает embedding одного текста через GigaChat.
        """

        response = self.client.embeddings(
            texts=[text]
        )

        return response.data[0].embedding

    # -------------------------
    # BATCH PROCESSING
    # -------------------------
    def add_embeddings(self, news_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Добавляет embeddings к списку новостей.
        """

        total = len(news_list)

        for i, news in enumerate(news_list, start=1):
            print(f"Embedding {i}/{total}")

            text = self.prepare_embedding_text(news)
            embedding = self.get_embedding(text)

            news["embedding"] = embedding
            news["embedding_text_used"] = text  # полезно для дебага

        return news_list