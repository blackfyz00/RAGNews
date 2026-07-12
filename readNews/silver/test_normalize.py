# from utils import load_json, save_json, generate_news_id, PROJECT_ROOT
# from normalize import normalize_news_list

# bronze = PROJECT_ROOT / "readNews" / "test.txt"
# output = PROJECT_ROOT / "silver" / "normalized_test.json"

# # -------------------------
# # load bronze data
# # -------------------------
# news = load_json(bronze)

# # -------------------------
# # add IDs first (important for dedup later)
# # -------------------------
# for item in news:
#     if "id" not in item:
#         item["id"] = generate_news_id()

# # -------------------------
# # normalize
# # -------------------------
# news = normalize_news_list(news)

# # -------------------------
# # save silver layer
# # -------------------------
# save_json(news, output)

# print(f"✔ Silver layer saved: {len(news)} news")