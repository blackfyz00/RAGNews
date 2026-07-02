import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from typing import List


class DuplicateFinder:
    """
    Поиск семантически похожих новостей.
    """

    def __init__(self, threshold: float = 0.90):
        self.threshold = threshold

    def similarity_matrix(self, news_list: List[dict]) -> np.ndarray:
        embeddings = np.array(
            [np.array(n["embedding"], dtype=np.float32) for n in news_list]
        )

        return cosine_similarity(embeddings)

    def print_top_pairs(self, news_list: List[dict], top_k: int = 10):

        sim = self.similarity_matrix(news_list)

        pairs = []
        n = len(news_list)

        for i in range(n):
            for j in range(i + 1, n):
                pairs.append((sim[i][j], i, j))

        pairs.sort(reverse=True)

        print("\nСамые похожие новости:\n")

        for similarity, i, j in pairs[:top_k]:
            print("=" * 80)
            print(f"Similarity: {similarity:.4f}\n")
            print(news_list[i]["normalized_title"])
            print()
            print(news_list[j]["normalized_title"])
            print()

    def mark_duplicates(self, news_list: List[dict]) -> List[dict]:

        sim = self.similarity_matrix(news_list)
        n = len(news_list)

        for news in news_list:
            news["duplicates"] = []

        for i in range(n):
            for j in range(i + 1, n):

                similarity = float(sim[i][j])

                if similarity >= self.threshold:

                    news_list[i]["duplicates"].append({
                        "id": news_list[j]["id"],
                        "similarity": round(similarity, 4)
                    })

                    news_list[j]["duplicates"].append({
                        "id": news_list[i]["id"],
                        "similarity": round(similarity, 4)
                    })

        return news_list