# RAGNews Telegram bot

Telegram bot for searching news from the PostgreSQL/pgvector `gold` layer with GigaChat.

## Environment

Required:

- `TELEGRAM_BOT_TOKEN`
- `GIGACHAT_AUTH_KEY` or `GIGACHAT_CREDENTIALS`
- `GIGACHAT_SCOPE`

Database defaults match the project docker setup exposed to host:

- `DB_HOST=localhost`
- `DB_PORT=5438`
- `DB_NAME=it_news_rag`
- `DB_USER=postgres`
- `DB_PASSWORD=postgres`

For the news update command, set one of:

- `PREFECT_UPDATE_ENDPOINT`
- `PREFECT_DEPLOYMENT_ID`
- `PREFECT_DEPLOYMENT_NAME`, for example `flow-name/deployment-name`

Optional:

- `PREFECT_API_URL`, default `http://localhost:8210/api`
- `NEWS_SEARCH_LIMIT`, default `5`
- `GIGACHAT_MODEL`, default `GigaChat`
- `GIGACHAT_EMBEDDING_MODEL`, default `Embeddings`

## Run

```bash
pip install -r requirements.txt
python main.py
```
