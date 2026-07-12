import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

from define_id_flow import get_deployment_id_by_name

import asyncpg
import httpx
from dotenv import load_dotenv
from gigachat import GigaChat
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters


load_dotenv()

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
)
logger = logging.getLogger("ragnews-bot")

BOT_PURPOSE_TEXT = (
    "Я бот для поиска новостей в базе RAGNews. "
    "Напишите, какие новости найти, например: новости про Сбербанк. "
    "Для обновления базы используйте /update."
)

SYSTEM_PROMPT = """
Ты Telegram-бот RAGNews. Твоя единственная задача - помогать пользователю искать новости
в базе проекта RAGNews и запускать обновление новостного слоя.

Правила:
1. Не отвечай на посторонние вопросы: программирование, математика, советы, личные темы,
   творчество, перевод, пересказ произвольных текстов и любые запросы не про поиск/обновление новостей.
2. Если запрос посторонний, объясни коротко, что ты бот для поиска новостей.
3. Если пользователь просит найти новости, выдели точную поисковую фразу на русском языке.
   Примеры:
   - "найди новости про сбер" -> "сбер"
   - "что нового про Сбербанк" -> "сбербанк"
   - "последние новости сбербанк" -> "новости сбербанк"
4. Если пользователь просит обновить/запустить парсинг/обновить базу новостей, верни intent update.
5. Отвечай для классификации строго JSON без markdown:
   {"intent":"search|update|offtopic","query":"точная фраза или пустая строка","message":"короткое сообщение"}
"""


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    gigachat_credentials: str
    gigachat_scope: str | None
    db_user: str
    db_password: str
    db_name: str
    db_host: str
    db_port: int
    search_limit: int
    prefect_api_url: str
    prefect_update_endpoint: str | None
    prefect_deployment_id: str | None
    prefect_deployment_name: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        credentials = (
            os.getenv("GIGACHAT_CREDENTIALS")
            or os.getenv("GIGACHAT_AUTH_KEY")
            or ""
        ).strip()

        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
        if not credentials:
            raise RuntimeError("GIGACHAT_CREDENTIALS or GIGACHAT_AUTH_KEY is not set")

        return cls(
            telegram_bot_token=token,
            gigachat_credentials=credentials,
            gigachat_scope=os.getenv("GIGACHAT_SCOPE"),
            db_user=os.getenv("DB_USER", "postgres"),
            db_password=os.getenv("DB_PASSWORD", "postgres"),
            db_name=os.getenv("DB_NAME", "it_news_rag"),
            db_host=os.getenv("DB_HOST", "localhost"),
            db_port=int(os.getenv("DB_PORT", "5438")),
            search_limit=int(os.getenv("NEWS_SEARCH_LIMIT", "5")),
            prefect_api_url=os.getenv("PREFECT_API_URL", "http://localhost:8210/api").rstrip("/"),
            prefect_update_endpoint=os.getenv("PREFECT_UPDATE_ENDPOINT"),
            prefect_deployment_id=os.getenv("PREFECT_DEPLOYMENT_ID"),
            prefect_deployment_name=os.getenv("PREFECT_DEPLOYMENT_NAME"),
        )

    @property
    def db_config(self) -> dict[str, Any]:
        return {
            "user": self.db_user,
            "password": self.db_password,
            "database": self.db_name,
            "host": self.db_host,
            "port": self.db_port,
        }


settings = Settings.from_env()
chat_client = GigaChat(
    credentials=settings.gigachat_credentials,
    scope=settings.gigachat_scope,
    verify_ssl_certs=False,
)


def _extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text.replace("json", "", 1).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1:
        text = text[start : end + 1]

    return json.loads(text)


def _fallback_intent(user_text: str) -> dict[str, str]:
    lowered = user_text.lower()
    update_words = ("обнов", "парс", "запусти", "запустить", "собери", "пересобери")
    news_words = ("новост", "что нового", "найди", "поиск", "про ")

    if any(word in lowered for word in update_words):
        return {"intent": "update", "query": "", "message": "Запускаю обновление новостей."}

    if any(word in lowered for word in news_words):
        query = lowered
        for token in ("найди", "покажи", "последние", "свежие", "новости", "новость", "про", "о", "об"):
            query = query.replace(token, " ")
        query = " ".join(query.split()) or user_text.strip()
        return {"intent": "search", "query": query, "message": ""}

    return {"intent": "offtopic", "query": "", "message": BOT_PURPOSE_TEXT}


def classify_request(user_text: str) -> dict[str, str]:
    try:
        response = chat_client.chat(
            {
                "model": os.getenv("GIGACHAT_MODEL", "GigaChat"),
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_text},
                ],
                "temperature": 0.1,
            }
        )
        parsed = _extract_json(response.choices[0].message.content)
        intent = str(parsed.get("intent", "")).lower()
        query = str(parsed.get("query", "")).strip()
        message = str(parsed.get("message", "")).strip()

        if intent not in {"search", "update", "offtopic"}:
            return _fallback_intent(user_text)
        if intent == "search" and not query:
            return _fallback_intent(user_text)
        return {"intent": intent, "query": query, "message": message}
    except Exception:
        logger.exception("Failed to classify request with GigaChat")
        return _fallback_intent(user_text)


def get_embedding(text: str) -> list[float]:
    response = chat_client.embeddings(
        texts=[text],
        model=os.getenv("GIGACHAT_EMBEDDING_MODEL", "Embeddings"),
    )
    return response.data[0].embedding


def vector_literal(values: list[float]) -> str:
    return "[" + ",".join(str(float(value)) for value in values) + "]"


async def search_gold_news(query: str, limit: int) -> list[asyncpg.Record]:
    embedding = await asyncio.to_thread(get_embedding, query)
    query_vector = vector_literal(embedding)

    conn = await asyncpg.connect(**settings.db_config)
    try:
        return await conn.fetch(
            """
            SELECT
                id,
                ai_title,
                ai_text,
                url,
                date,
                links,
                1 - (embeddings <=> $1::vector) AS similarity
            FROM gold
            WHERE embeddings IS NOT NULL
            ORDER BY embeddings <=> $1::vector
            LIMIT $2;
            """,
            query_vector,
            limit,
        )
    finally:
        await conn.close()

def format_source_links(row: asyncpg.Record) -> str:
    links = []
    if row["url"]:
        links.append(str(row["url"]))
    if row["links"]:
        links.extend(str(link) for link in row["links"] if link)

    unique_links = list(dict.fromkeys(links))
    if not unique_links:
        return "Источник без ссылки"
    return "\n".join(f"- {link}" for link in unique_links[:3])


def normalize_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y")
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    return str(value) if value else "дата не указана"


def build_context(rows: list[asyncpg.Record]) -> str:
    chunks = []
    for index, row in enumerate(rows, start=1):
        chunks.append(
            "\n".join(
                [
                    f"Новость {index}",
                    f"Заголовок: {row['ai_title'] or 'Без заголовка'}",
                    f"Кратко: {row['ai_text'] or ''}",
                    f"Дата: {normalize_date(row['date'])}",
                    f"Ссылки:\n{format_source_links(row)}",
                ]
            )
        )
    return "\n\n".join(chunks)


def generate_answer(user_text: str, query_phrase: str, rows: list[asyncpg.Record]) -> str:
    """Формирует ответ в HTML формате для Telegram"""
    parts = []
    
    # Заголовок
    parts.append(f"<b>🔍 Результаты поиска по запросу:</b> <i>\"{query_phrase}\"</i>\n")
    
    for idx, row in enumerate(rows, start=1):
        title = row['ai_title'] or 'Без заголовка'
        text = row['ai_text'] or 'Описание отсутствует'
        date_str = normalize_date(row['date'])
        
        parts.append(f"<b>{idx}. {title}</b>\n")
        parts.append(f"{text}\n")
        parts.append(f"<b>📅 Дата:</b> {date_str}\n")
        
        links = []
        if row['url']:
            links.append(str(row['url']))
        if row['links']:
            links.extend(str(link) for link in row['links'] if link)
        unique_links = list(dict.fromkeys(links))
        
        if unique_links:
            parts.append("<b>🔗 Ссылки:</b>")
            for link in unique_links[:3]:
                parts.append(f"• <a href=\"{link}\">{link}</a>")
        else:
            parts.append("<b>🔗 Ссылки:</b> отсутствуют")
        
        parts.append("")
        parts.append("─" * 15)
        parts.append("")
    
    # Убираем лишние разделители в конце
    while parts and parts[-1] in ["", "─" * 15]:
        parts.pop()
    
    parts.append(f"\n<b>📌 Всего найдено:</b> {len(rows)} новостей")
    
    return "\n".join(parts)

async def trigger_prefect_update() -> str:
    deployment_id = None
    
    # Проверяем, что имя деплоймента указано в .env
    if not settings.prefect_deployment_name:
        raise RuntimeError("PREFECT_DEPLOYMENT_NAME not set in .env")
    
    # Получаем ID деплоймента динамически по имени
    deployment_id = await get_deployment_id_by_name(
        settings.prefect_deployment_name,
        settings.prefect_api_url
    )
    
    # Проверяем, что ID получен
    if not deployment_id:
        raise RuntimeError(f"Deployment '{settings.prefect_deployment_name}' not found in Prefect")
    
    # Запускаем flow run
    endpoint = f"{settings.prefect_api_url}/deployments/{deployment_id}/create_flow_run"
    logger.info(f"Triggering flow at: {endpoint}")
    
    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            response = await client.post(
                endpoint,
                json={
                    "parameters": {},
                    "name": f"manual_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                }
            )
            response.raise_for_status()
            payload = response.json() if response.content else {}
            
            flow_run_id = payload.get("id") or payload.get("flow_run_id")
            if flow_run_id:
                return f"✅ Запустил обновление новостей. Flow run: {flow_run_id}"
            return "✅ Запустил обновление новостей."
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            raise RuntimeError(f"Prefect API error: {e.response.status_code}")
        except httpx.ConnectError:
            logger.error("Cannot connect to Prefect API")
            raise RuntimeError("Не могу подключиться к Prefect. Проверьте, запущен ли news_pipeline.")
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            raise

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(BOT_PURPOSE_TEXT)


async def update_news(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.chat.send_action(ChatAction.TYPING)
    try:
        message = await trigger_prefect_update()
        await update.message.reply_text(
            message,
            parse_mode="Markdown"  # ← Добавьте
        )
    except Exception as exc:
        logger.exception("Failed to trigger Prefect update")
        await update.message.reply_text(
            f"❌ *Не удалось запустить обновление*\n\n`{exc}`",
            parse_mode="Markdown"  # ← Добавьте
        )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_text = (update.message.text or "").strip()
    if not user_text:
        await update.message.reply_text(BOT_PURPOSE_TEXT)
        return

    await update.message.chat.send_action(ChatAction.TYPING)
    classified = await asyncio.to_thread(classify_request, user_text)
    intent = classified["intent"]

    if intent == "offtopic":
        await update.message.reply_text(
            classified.get("message") or BOT_PURPOSE_TEXT,
            parse_mode="Markdown"  
        )
        return

    if intent == "update":
        await update_news(update, context)
        return

    query_phrase = classified["query"]
    try:
        rows = await search_gold_news(query_phrase, settings.search_limit)
    except Exception:
        logger.exception("Failed to search gold layer")
        await update.message.reply_text(
            "❌ *Не смог подключиться к gold-слою или выполнить поиск.*",
            parse_mode="Markdown"  # ← Добавьте
        )
        return

    if not rows:
        await update.message.reply_text(
            f"🔍 *По фразе* `\"{query_phrase}\"` *ничего не нашлось.*\n\n🔄 Можно запустить обновление через /update.",
            parse_mode="Markdown"  # ← Добавьте
        )
        return

    try:
        answer = await asyncio.to_thread(generate_answer, user_text, query_phrase, rows)
    except Exception:
        logger.exception("Failed to generate final answer")
        answer = build_context(rows)

    await update.message.reply_text(
        answer,
        parse_mode="HTML",  # ← Добавьте это!
        disable_web_page_preview=True
    )


def main() -> None:
    application = ApplicationBuilder().token(settings.telegram_bot_token).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("update", update_news))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.run_polling()


if __name__ == "__main__":
    main()
