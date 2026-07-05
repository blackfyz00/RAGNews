from utils import *
from normalize import *
from embeddings import *
from deduplicate import *

bronze = PROJECT_ROOT / "readNews" / "test.txt"
output = PROJECT_ROOT / "silver" / "silver.json"

news = load_json(bronze)

for item in news:
    item.setdefault("id", generate_news_id())

news = normalize_news_list(news)

provider = GigaChatEmbeddingProvider()
news = provider.add_embeddings(news)

finder = DuplicateFinder()

finder.print_top_pairs(news)

news = finder.merge_duplicates(news) # удаляем дубли

silver_news = []

for item in news:
    silver_news.append({

        "source_name": item["source"],

        "url": item["url"],

        "date": item["timestamp"].split()[0],

        "normalized_title": item["title"],

        "normalized_content": item["normalized_text"],

        "links": item.get("links", [item["url"]]),

        "embeddings": item["embedding"]

    })

save_json(silver_news, output)