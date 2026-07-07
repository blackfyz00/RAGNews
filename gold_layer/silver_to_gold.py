import sys
from pathlib import Path
from datetime import datetime
import asyncpg
from prefect import flow, get_run_logger
from gigachat import GigaChat

# Решаем проблему с импортами: добавляем корень проекта RAGNEWS в пути поиска
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

# Импорты из папки readNews
from readNews.DB_CONFIG import DB_CONFIG
from readNews.ENV_PATH import ENV_PATH

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
    обогащает через GigaChat API и записывает в таблицу gold.
    Возвращает количество успешно обработанных новостей.
    """
    logger = get_run_logger()
    processed_count = 0
    
    # Инициализируем асинхронный клиент GigaChat
    async with GigaChat(credentials="YOUR_GIGACHAT_API_CREDENTIALS", verify_ssl_certs=False, async_mode=True) as chat:
        # Подключаемся к БД через асинхронный asyncpg
        conn = await asyncpg.connect(**DB_CONFIG)
        
        try:
            logger.info("🔍 Поиск необработанных новостей в слое Silver...")
            
            # Строгое соответствие названий полей вашей схеме silver
            query_select = """
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
            """
            rows_to_process = await conn.fetch(query_select)
            
            if not rows_to_process:
                logger.info("ℹ️ Все новости из слоя Silver уже обработаны нейросетью.")
                return 0
                
            logger.info(f"🤖 Найдено новостей для суммаризации: {len(rows_to_process)}")
            
            for row in rows_to_process:
                silver_id = row['id']
                
                # Передаем оригинальные normalized поля из silver
                ai_title, ai_text = await generate_ai_content_async(
                    chat, 
                    row['normalized_title'], 
                    row['normalized_content'],
                    logger
                )
                
                if ai_title and ai_text:
                    # Строгое соответствие названий полей вашей схеме gold
                    query_insert = """
                        INSERT INTO gold (id, ai_title, ai_text, source_name, url, date, links, embeddings)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (id) DO NOTHING;
                    """
                    await conn.execute(
                        query_insert,
                        silver_id,
                        ai_title,
                        ai_text,
                        row['source_name'],
                        row['url'],
                        row['date'],
                        row['links'],
                        row['embeddings']  
                    )
                    processed_count += 1
                    logger.info(f"✨ Новость ID {silver_id} успешно перенесена в Gold слой.")
                else:
                    logger.warning(f"⚠️ Пропуск ID {silver_id} из-за ошибки GigaChat API.")
                    
            logger.info(f"✅ Итог работы Gold-конвейера: успешно обработано {processed_count} новостей.")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в процессе gold_pipeline: {e}")
            raise e
        finally:
            await conn.close()
            
    return processed_count