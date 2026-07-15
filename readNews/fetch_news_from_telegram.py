import os
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from prefect import task, get_run_logger
from sqlalchemy import text

# Импортируем вашу фабрику сессий
from database import get_async_session_factory 

@task(retries=3, retry_delay_seconds=30, name="Парсинг Telegram каналов")
async def fetch_news_from_telegram() -> list[dict]:
    """
    Парсит последние посты из Telegram каналов, беря URL/username из таблицы sources базы данных.
    В поле date пишется реальное время публикации поста в формате YYYY-MM-DD.
    """
    logger = get_run_logger()
    all_news = []
    
    # 1. Получаем фабрику сессий и запрашиваем URL из БД
    async_session_factory = get_async_session_factory()
    
    try:
        async with async_session_factory() as session:
            # Выбираем только телеграм-каналы (tg) из таблицы sources
            query = text("SELECT url FROM sources WHERE source_type = 'tg';")
            result = await session.execute(query)
            channels = [row[0] for row in result.fetchall()]
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении Telegram каналов из таблицы sources: {e}")
        return all_news
    
    if not channels:
        logger.info("ℹ️ В таблице sources не найдено URL с типом 'tg' для парсинга.")
        return all_news
    
    logger.info(f"📱 Начинаю парсинг {len(channels)} Telegram каналов из базы данных...")
    
    # Используем один асинхронный клиент для всех запросов
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for channel in channels:
            try:
                # Очищаем и форматируем имя канала для генерации веб-ссылки t.me/s/
                channel_clean = channel.strip()
                if channel_clean.startswith("https://t.me/"):
                    channel_name = channel_clean.replace("https://t.me/", "")
                elif channel_clean.startswith("@"):
                    channel_name = channel_clean.replace("@", "")
                else:
                    channel_name = channel_clean

                # Для логирования и поля source оставляем красивый формат с @
                display_name = f"@{channel_name}"
                logger.info(f"  📡 Парсинг Telegram канала: {display_name}")
                
                tg_url = f"https://t.me/s/{channel_name}"
                
                try:
                    # Исправлено: теперь запрос выполняется асинхронно
                    response = await client.get(tg_url)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    messages = soup.find_all('div', class_='tgme_widget_message_wrap')
                    if not messages:
                        messages = soup.find_all('div', {'data-post': True})
                    
                    posts_count = 0
                    for message in messages[:15]:
                        try:
                            text_div = message.find('div', class_='tgme_widget_message_text')
                            if not text_div:
                                continue
                            
                            text_content = text_div.get_text('\n', strip=True)
                            
                            post_link = message.find('a', class_='tgme_widget_message_date')
                            post_url = post_link.get('href', '') if post_link else ''
                            
                            # ---- СТРОГИЙ ПАРСИНГ ДАТЫ ПУБЛИКАЦИИ ----
                            time_tag = message.find('time')
                            timestamp = None
                            
                            if time_tag and time_tag.get('datetime'):
                                raw_datetime = time_tag['datetime']
                                try:
                                    dt = date_parser.parse(raw_datetime)
                                    # Формат DATE (YYYY-MM-DD) для соответствия таблице bronze
                                    timestamp = dt.strftime("%Y-%m-%d")
                                except Exception as e:
                                    logger.warning(f"      ⚠️ Не удалось распарсить ISO дату '{raw_datetime}': {e}")
                            
                            if not timestamp:
                                logger.warning(f"      ⚠️ Дата поста не найдена в HTML. Пишем текущую.")
                                timestamp = datetime.now().strftime("%Y-%m-%d")
                            
                            payload = {
                                "title": text_content[:80] + "..." if len(text_content) > 80 else text_content,
                                "content": text_content,
                                "source_name": f"ТГ-канал {display_name}", # Переименовано в source_name под таблицу bronze
                                "date": timestamp,  # Переименовано в date под таблицу bronze
                                "url": post_url if post_url else f"https://t.me/{channel_name}" 
                            }
                            all_news.append(payload)
                            posts_count += 1
                                
                        except Exception as e:
                            logger.warning(f"    ⚠️ Ошибка при обработке поста: {e}")
                            continue
                    
                    logger.info(f"  ✅ Получено {posts_count} постов из канала {display_name}")
                    
                except Exception as e:
                    logger.error(f"    ⚠️ Не удалось получить доступ к {tg_url}: {e}")
                        
            except Exception as e:
                logger.error(f"  ❌ Ошибка при парсинге канала {channel}: {e}")
    
    logger.info(f"📊 Всего собрано новостей из Telegram: {len(all_news)}")
    return all_news
