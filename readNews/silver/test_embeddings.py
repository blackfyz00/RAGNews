# from utils import *
# from normalize import *
# from embeddings import *

# bronze = PROJECT_ROOT / "readNews" / "test.txt"

# output = PROJECT_ROOT / "silver" / "embedded_test.json"

# news = load_json(bronze)

# # id
# for item in news:
#     if "id" not in item:
#         item["id"] = generate_news_id()

# # нормализация
# news = normalize_news_list(news)

# # embeddings
# provider = GigaChatEmbeddingProvider()

# news = provider.add_embeddings(news)

# save_json(news, output)

# print()

# print("Размер embedding:")

# print(len(news[0]["embedding"]))