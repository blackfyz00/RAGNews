import os
import sys
import asyncio
import json
import re
from pathlib import Path
from pydantic import BaseModel, Field
from prefect import flow, get_run_logger
from gigachat import GigaChat
from sqlalchemy import text

try:
    from gigachat.models import Chat, Messages
    USE_MODELS = True
except ImportError:
    USE_MODELS = False

root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from database import get_async_session_factory

GIGACHAT_CREDENTIALS = os.getenv("GIGACHAT_CREDENTIALS")
GIGACHAT_SCOPE = os.getenv("GIGACHAT_SCOPE")

BATCH_SIZE = 10            

class NewsItem(BaseModel):
    title: str = Field(description="Новый вовлекающий заголовок")
    summary: str = Field(description="Краткий пересказ")

async def process_single_news(chat_client, row, logger) -> NewsItem | None:
    """Обрабатывает одну новость с защитой от дисклеймеров"""
    
    # Берем меньше текста, чтобы снизить риск триггеров
    content_snippet = row['normalized_content'][:800]
    
    system_prompt = (
        "Ты новостной редактор. Перепиши заголовок и сделай краткое саммари. "
        "Верни ответ СТРОГО в JSON формате: {\"title\": \"...\", \"summary\": \"...\"}. "
        "Без лишнего текста."
    )

    prompt = f"Заголовок: {row['normalized_title']}\nТекст: {content_snippet}"

    if USE_MODELS:
        payload = Chat(
            model="GigaChat-Pro", 
            messages=[
                Messages(role="system", content=system_prompt),
                Messages(role="user", content=prompt)
            ]
        )
    else:
        payload = {
            "model": "GigaChat-Pro",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ]
        }

    try:
        response = await chat_client.achat(payload)
        raw_content = response.choices[0].message.content.strip()
        
        # 1. Проверка на дисклеймер
        banned_phrases = ["нейросетевой моделью", "собственным мнением", "чувствительные темы", "GigaChat"]
        if any(phrase in raw_content for phrase in banned_phrases):
            logger.warning(f"⚠️ ID {row['id']}: Сработал фильтр безопасности GigaChat.")
            return None

        # 2. Очистка от markdown
        match = re.search(r"```(?:json)?\s*(.*?)\s*```", raw_content, re.DOTALL)
        clean_text = match.group(1) if match else raw_content
        
        # 3. Парсинг
        json_data = json.loads(clean_text.strip())
        return NewsItem(**json_data)

    except Exception as e:
        logger.error(f"❌ Ошибка для ID {row['id']}: {e}")
        return None

@flow(name="Формирование и сохранение в Голд слой (LLM)")
async def gold_pipeline() -> int:
    logger = get_run_logger()
    processed_count = 0
    
    if not GIGACHAT_CREDENTIALS:
        logger.error("❌ GIGACHAT_CREDENTIALS не задана!")
        return 0

    async with GigaChat(credentials=GIGACHAT_CREDENTIALS, scope=GIGACHAT_SCOPE, verify_ssl_certs=False) as chat:
        async_session_factory = get_async_session_factory()
        
        async with async_session_factory() as session:
            try:
                query_select = text("""
                    SELECT s.id, s.source_name, s.url, s.date, s.normalized_title, s.normalized_content, s.links, s.embeddings 
                    FROM silver s
                    LEFT JOIN gold g ON s.id = g.id
                    WHERE g.id IS NULL
                    LIMIT :batch_size;
                """)
                
                result = await session.execute(query_select, {"batch_size": BATCH_SIZE})
                rows_to_process = result.mappings().all()
                
                if not rows_to_process:
                    logger.info("ℹ️ Нет новых данных.")
                    return 0
                    
                logger.info(f"🤖 Последовательная обработка {len(rows_to_process)} новостей...")
                
                insert_payloads = []
                for i, row in enumerate(rows_to_process):
                    # Пауза, чтобы не словить 429
                    if i > 0:
                        await asyncio.sleep(1.5)
                        
                    ai_data = await process_single_news(chat, row, logger)
                    
                    if ai_data:
                        raw_emb = row['embeddings']
                        emb_str = '[' + ', '.join(str(x) for x in raw_emb) + ']' if isinstance(raw_emb, list) else (str(raw_emb) if raw_emb else None)
                        
                        insert_payloads.append({
                            "id": row['id'],
                            "ai_title": ai_data.title,
                            "ai_text": ai_data.summary,
                            "source_name": row['source_name'],
                            "url": row['url'],
                            "date": row['date'],
                            "links": row['links'],
                            "embeddings": emb_str
                        })
                
                if insert_payloads:
                    query_insert = text("""
                        INSERT INTO gold (id, ai_title, ai_text, source_name, url, date, links, embeddings)
                        VALUES (:id, :ai_title, :ai_text, :source_name, :url, :date, :links, CAST(:embeddings AS vector))
                        ON CONFLICT (id) DO NOTHING;
                    """)
                    await session.execute(query_insert, insert_payloads)
                    await session.commit()
                    processed_count = len(insert_payloads)
                    
                logger.info(f"✅ Сохранено в Gold: {processed_count} из {len(rows_to_process)}")
                
            except Exception as e:
                await session.rollback()
                logger.error(f"❌ Ошибка pipeline: {e}")
                raise e
                
        await async_session_factory.kw['bind'].dispose()
            
    return processed_count