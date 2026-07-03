from utils import *
from normalize import *
from embeddings import *
from deduplicate import *

bronze = PROJECT_ROOT / "readNews" / "test.txt"
output = PROJECT_ROOT / "silver" / "silver.json"

# Загружаем Bronze
news = load_json(bronze)

# Генерируем id
for item in news:
    item.setdefault("id", generate_news_id())

# Нормализация
news = normalize_news_list(news)

# Эмбеддинги
provider = GigaChatEmbeddingProvider()
news = provider.add_embeddings(news)

# Поиск дублей
finder = DuplicateFinder()

finder.print_top_pairs(news)

news = finder.mark_duplicates(news)

# Формируем финальный Silver
silver_news = []

for item in news:
    silver_news.append({
        "id": item["id"],
        "title": item["title"],
        "normalized_text": item["normalized_text"],
        "source": item["source"],
        "url": item["url"],
        "timestamp": item["timestamp"],
        "embedding": item["embedding"],
        "duplicates": item.get("duplicates", [])
    })

save_json(silver_news, output)