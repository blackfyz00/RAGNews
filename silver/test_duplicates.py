from utils import *
from normalize import *
from embeddings import *
from deduplicate import *

bronze = PROJECT_ROOT / "readNews" / "test.txt"

output = PROJECT_ROOT / "silver" / "silver_test.json"

news = load_json(bronze)

for item in news:

    if "id" not in item:
        item["id"] = generate_news_id()

news = normalize_news_list(news)

provider = GigaChatEmbeddingProvider()

news = provider.add_embeddings(news)

finder = DuplicateFinder()

finder.print_top_pairs(news)

news = finder.mark_duplicates(news)

save_json(news, output)