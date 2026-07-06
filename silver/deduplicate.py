import numpy as np
import hashlib
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import normalize
from typing import List, Dict, Set, Optional
from collections import defaultdict

class DuplicateFinder:
    def __init__(self, threshold: float = 0.92, silver_data: Optional[List[dict]] = None):
        """
        Гибридный детектор дубликатов:
        1. Сначала точное совпадение по хэшу (быстро)
        2. Потом семантическое сходство (медленно, но точно)
        
        Args:
            threshold: Порог косинусного сходства для семантической дедупликации
        """
        self.threshold = threshold
        self.hash_to_indices: Dict[str, List[int]] = defaultdict(list)
        
        self.silver_embeddings = None
        self.silver_data = silver_data or []

        if self.silver_data:
            self._build_silver_matrix()

    def _build_silver_matrix(self):
        """Строит матрицу эмбеддингов из переданных данных silver"""
        embeddings = []
        for item in self.silver_data:
            emb = item.get("embedding")
            if emb is not None:
                embeddings.append(np.array(emb))
        
        if embeddings:
            matrix = np.array(embeddings)
            self.silver_embeddings = normalize(matrix, norm='l2')

    def check_against_silver(self, news_list: List[dict]) -> tuple[List[dict], List[dict]]:
        """
        Проверяет новые новости на дубликаты с существующими в silver.
        Возвращает: (уникальные_новости, список_дубликатов_для_обновления_links)
        """
        if self.silver_embeddings is None or len(self.silver_data) == 0:
            return news_list, []
        
        silver_hashes = {item.get("hash"): item for item in self.silver_data if item.get("hash")}
        
        unique_news = []
        duplicates_to_update = []
        
        for item in news_list:
            if item.get("hash") and item["hash"] in silver_hashes:
                existing = silver_hashes[item["hash"]]
                print(f"⏭️  Точный дубликат по хэшу: {item.get('title', '')[:50]}...")
                duplicates_to_update.append({
                    "silver_id": existing["id"],
                    "new_url": item.get("url")
                })
                continue

            embedding = item.get("embedding")
            if not embedding:
                unique_news.append(item)
                continue
            
            emb_norm = normalize([np.array(embedding)], norm='l2')[0]
            similarities = cosine_similarity([emb_norm], self.silver_embeddings)[0]
            max_sim = similarities.max()
            
            if max_sim >= self.threshold:
                idx = similarities.argmax()
                existing = self.silver_data[idx]
                print(f"⏭️  Семантический дубликат (сходство {max_sim:.4f}) с: {existing.get('title', '')[:50]}...")
                duplicates_to_update.append({
                    "silver_id": existing["id"],
                    "new_url": item.get("url")
                })
                continue
            
            unique_news.append(item)
        
        return unique_news, duplicates_to_update

    def compute_hash(self, item: dict) -> str:
        """
        Вычисляет хэш новости на основе url и заголовка.
        Можно расширить для учета контента.
        """

        url = item.get("url", "").strip()
        title = item.get("title", "").strip()
        key = f"{url}|{title}".lower()

        return hashlib.md5(key.encode('utf-8')).hexdigest()
    
    def find_exact_duplicates(self, news_list: List[dict]) -> List[Set[int]]:
        """
        Находит группы точных дубликатов по хэшу.
        Возвращает список множеств индексов дубликатов.
        """
        self.hash_to_indices.clear()
        for idx, item in enumerate(news_list):
            hash_val = self.compute_hash(item)
            self.hash_to_indices[hash_val].append(idx)
            item["hash"] = hash_val

        duplicate_groups = []
        for hash_val, indices in self.hash_to_indices.items():
            if len(indices) > 1:
                duplicate_groups.append(set(indices))
        
        return duplicate_groups
    
    def similarity_matrix(self, news_list: List[dict]) -> np.ndarray:
        """Вычисляет матрицу косинусного сходства с нормализацией"""
        embeddings = np.array(
            [np.array(n["embedding"], dtype=np.float32) for n in news_list]
        )

        embeddings_normalized = normalize(embeddings, norm='l2')
        sim = cosine_similarity(embeddings_normalized)
        
        return sim

    def print_top_pairs(self, news_list: List[dict], top_k: int = 10):
        sim = self.similarity_matrix(news_list)
        
        pairs = []
        for i in range(len(news_list)):
            for j in range(i + 1, len(news_list)):
                pairs.append((sim[i][j], i, j))
        
        pairs.sort(reverse=True)
        
        print(f"\nСамые похожие новости (порог: {self.threshold}):\n")
        
        for similarity, i, j in pairs[:top_k]:
            print("=" * 80)
            print(f"Similarity: {similarity:.4f}")
            if similarity >= self.threshold:
                print("БУДЕТ удалено как дубликат")
            else:
                print("НЕ БУДЕТ удалено")
            print()
            
            title_i = news_list[i].get("title") or news_list[i].get("normalized_title") or f"Новость #{i}"
            title_j = news_list[j].get("title") or news_list[j].get("normalized_title") or f"Новость #{j}"
            
            print(title_i)
            print()
            print(title_j)
            print()

    def _are_exact_duplicates(self, idx1: int, idx2: int) -> bool:
        """Проверяет, являются ли две новости точными дубликатами"""
        for indices in self.hash_to_indices.values():
            if idx1 in indices and idx2 in indices:
                return True
        return False
    
    def merge_duplicates(self, news_list: List[dict]) -> List[dict]:
        """
        Объединяет дубликаты:
        1. Сначала удаляет точные дубликаты по хэшу
        2. Потом удаляет семантические дубликаты по косинусному сходству
        """

        print("\nШАГ 1: Поиск точных дубликатов по хэшу...")
        exact_groups = self.find_exact_duplicates(news_list)

        indices_to_remove = set()
        for group in exact_groups:
            sorted_group = sorted(group)
            for idx in sorted_group[1:]:
                indices_to_remove.add(idx)

        unique_news = []
        for idx, item in enumerate(news_list):
            if idx not in indices_to_remove:
                unique_news.append(item)
        
        print(f"Точных дубликатов удалено: {len(indices_to_remove)}")
        print(f"Осталось после точной дедупликации: {len(unique_news)}")
        
        if len(unique_news) < 2:
            print("Недостаточно новостей для семантической дедупликации")
            return unique_news
        
        print("\nШАГ 2: Поиск семантических дубликатов...")

        embeddings = np.array(
            [np.array(n["embedding"], dtype=np.float32) for n in unique_news]
        )
        embeddings_normalized = normalize(embeddings, norm='l2')
        sim = cosine_similarity(embeddings_normalized)

        semantic_groups = []
        visited = set()
        
        for i in range(len(unique_news)):
            if i in visited:
                continue
            
            group = [i]
            visited.add(i)
            
            for j in range(i + 1, len(unique_news)):
                if j in visited:
                    continue
                
                if sim[i][j] >= self.threshold:
                    group.append(j)
                    visited.add(j)
            
            if len(group) > 1:
                semantic_groups.append(group)
        
        # Создаем финальный список
        final_news = []
        semantic_removed = 0
        
        for i, item in enumerate(unique_news):
            is_in_group = False
            for group in semantic_groups:
                if i in group:
                    if group[0] == i:
                        final_news.append(item)
                    else:
                        semantic_removed += 1
                    is_in_group = True
                    break
            
            if not is_in_group:
                final_news.append(item)
                
        duplicates_to_update = []
        if self.silver_data:
            print("\n🔍 ШАГ 3: Проверка с существующими в silver...")
            final_news, duplicates_to_update = self.check_against_silver(final_news)
        
        self.duplicates_to_update = duplicates_to_update
        print(f"   Семантических дубликатов удалено: {semantic_removed}")
        print(f"   Итоговое количество новостей: {len(final_news)}")
        
        return final_news
