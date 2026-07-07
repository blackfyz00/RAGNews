import os
import sys
from pathlib import Path
from datetime import datetime
from prefect import flow, get_run_logger
from gigachat import GigaChat
from sqlalchemy import text

root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

from database import get_async_session_factory

GIGACHAT_CREDENTIALS = os.getenv("GIGACHAT_CREDENTIALS")
GIGACHAT_SCOPE = os.getenv("GIGACHAT_SCOPE")

async def generate_ai_content_async(chat_client, original_title: str, original_content: str, logger) -> tuple[str | None, str | None]:
    """
    Асинхронный запрос в GigaChat API для генерации заголовка и пересказа.
    """
    prompt = f"Оригинальный заголовок: {original_title}\n\nОригинальный текст: {original_content}"
    try:
        response = await chat_client.achat(
            model="GigaChat",
            messages=[
                {
                    "role": "system", 
                    "content": (
                        "Ты профессиональный новостной редактор. Твоя задача — сделать краткий пересказ новости "
                        "и придумай для нее новый вовлекающий заголовок.\n"
                        "Ответ верни СТРОГО в следующем формате, используя разделитель [SPLIT]:\n"
                        "Новый заголовок\n"
                        "[SPLIT]\n"
                        "Текст краткого пересказа."
                    )
                },
                {"role": "user", "content": prompt}
            ]
        )
        ai_output = response.choices[0].message.content
        
        if "[SPLIT]" in ai_output:
            parts = ai_output.split("[SPLIT]")
            return parts[0].strip(), parts[1].strip()
        return f"AI: {original_title}", ai_output.strip()
    except Exception as e:
        logger.error(f"❌ Ошибка API GigaChat: {e}")
        return None, None

@flow(name="Формирование и сохранение в Голд слой (LLM)")
async def gold_pipeline() -> int:
    """
    Выбирает новые данные из silver (которых нет в gold), 
    обогащает через GigaChat API и записывает в таблицу gold через SQLAlchemy.
    """
    logger = get_run_logger()
    processed_count = 0
    
    logger.info(f"🔑 [Docker Контроль] CREDENTIALS={'Задан' if GIGACHAT_CREDENTIALS else 'ПУСТО!'}, SCOPE={GIGACHAT_SCOPE}")
    
    async with GigaChat(credentials=GIGACHAT_CREDENTIALS, scope=GIGACHAT_SCOPE, verify_ssl_certs=False, async_mode=True) as chat:
        async_session_factory = get_async_session_factory()
        
        async with async_session_factory() as session:
            try:
                logger.info("🔍 Поиск необработанных новостей в слое Silver...")
                
                query_select = text("""
                    SELECT 
                        s.id, 
                        s.source_name, 
                        s.url, 
                        s.date, 
                        s.normalized_title, 
                        s.normalized_content, 
                        s.links, 
                        s.embeddings 
                    FROM silver s
                    LEFT JOIN gold g ON s.id = g.id
                    WHERE g.id IS NULL;
                """)
                
                result = await session.execute(query_select)
                rows_to_process = result.mappings().all()
                
                if not rows_to_process:
                    logger.info("ℹ️ Все новости из слоя Silver уже обработаны нейросетью.")
                    return 0
                    
                logger.info(f"🤖 Найдено новостей для суммаризации: {len(rows_to_process)}")
                
                # ИСПРАВЛЕНО: Переносим сессию begin ВНУТРЬ цикла или убираем её блокировку,
                # так как await сетевого запроса к GigaChat РАЗРЫВАЕТ транзакцию базы данных!
                for row in rows_to_process:
                    silver_id = row['id']
                    
                    # Запрос к LLM идет БЕЗ открытой транзакции Postgres
                    ai_title, ai_text = await generate_ai_content_async(
                        chat, 
                        row['normalized_title'], 
                        row['normalized_content'],
                        logger
                    )
                    
                    if ai_title and ai_text:
                        # Открываем короткую транзакцию СТРОГО на момент записи одной новости
                        async with session.begin():
                            query_insert = text("""
                                INSERT INTO gold (id, ai_title, ai_text, source_name, url, date, links, embeddings)
                                VALUES (:id, :ai_title, :ai_text, :source_name, :url, :date, :links, :embeddings)
                                ON CONFLICT (id) DO NOTHING;
                            """)
                            
                            await session.execute(
                                query_insert,
                                {
                                    "id": silver_id,
                                    "ai_title": ai_title,
                                    "ai_text": ai_text,
                                    "source_name": row['source_name'],
                                    "url": row['url'],
                                    "date": row['date'],
                                    "links": row['links'],
                                    "embeddings": row['embeddings']
                                }
                            )
                        processed_count += 1
                        logger.info(f"✨ Новость ID {silver_id} успешно перенесена в Gold слой.")
                    else:
                        logger.warning(f"⚠️ Пропуск ID {silver_id} из-за ошибки GigaChat API.")
                            
                logger.info(f"✅ Итог работы Gold-конвейера: успешно обработано {processed_count} новостей.")
                
            except Exception as e:
                logger.error(f"❌ Ошибка в процессе gold_pipeline: {e}")
                raise e
                
        await async_session_factory.kw['bind'].dispose()
            
    return processed_count
