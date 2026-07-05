import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from typing import List

class DuplicateFinder:
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

        for i in range(len(news_list)):
            for j in range(i + 1, len(news_list)):
                pairs.append((sim[i][j], i, j))

        pairs.sort(reverse=True)

        print("\nСамые похожие новости:\n")

        for similarity, i, j in pairs[:top_k]:
            print("=" * 80)
            print(f"Similarity: {similarity:.4f}\n")
            print(news_list[i]["title"])
            print()
            print(news_list[j]["title"])
            print()

    # объединяем семантически одинаковые новости, оставляем первую из них, ссылки всех новостей храним в поле links
    def merge_duplicates(self, news_list: List[dict]) -> List[dict]:
        sim = self.similarity_matrix(news_list)
        visited = set()
        result = []

        for i in range(len(news_list)):
            if i in visited:
                continue

            group = [i]
            visited.add(i)

            for j in range(i + 1, len(news_list)):
                if j in visited:
                    continue

                if sim[i][j] >= self.threshold:
                    group.append(j)
                    visited.add(j)

            main_news = news_list[group[0]].copy()

            links = []
            sources = []

            for idx in group:
                url = news_list[idx]["url"]

                if url and url not in links:
                    links.append(url)

                source = news_list[idx]["source"]

                if source and source not in sources:
                    sources.append(source)

            main_news["links"] = links
            main_news["sources"] = sources

            result.append(main_news)

        print(
            f"\nУдалено дублей: {len(news_list) - len(result)}"
        )

        return result