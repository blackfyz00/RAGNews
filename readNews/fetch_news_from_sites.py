import os
from datetime import datetime
import feedparser
import httpx
from dateutil import parser as date_parser
from prefect import task, get_run_logger
import trafilatura
from sqlalchemy import text

# Импортируем вашу фабрику сессий
from database import get_async_session_factory 

@task(retries=3, retry_delay_seconds=30, name="Парсинг сайтов (RSS)")
async def fetch_news_from_sites() -> list[dict]:
    """Скачивает свежие новости с сайтов, беря URL RSS-лент из таблицы sources базы данных."""
    logger = get_run_logger()
    all_news = []
    
    # 1. Получаем фабрику сессий и запрашиваем URL из БД
    async_session_factory = get_async_session_factory()
    
    try:
        async with async_session_factory() as session:
            # Выбираем только сайты (site) из таблицы sources
            query = text("SELECT url FROM sources WHERE source_type = 'site';")
            result = await session.execute(query)
            # Извлекаем список URL строк
            urls = [row for row in result.fetchall()]
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении URL сайтов из таблицы sources: {e}")
        return all_news

    if not urls:
        logger.info("ℹ️ В таблице sources не найдено URL с типом 'site' для парсинга.")
        return all_news

    logger.info(f"🔍 Начинаю парсинг {len(urls)} сайтов из базы данных...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }
    
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers, verify=False) as client:
        for url in urls:
            try:
                logger.info(f"  📡 Парсинг RSS: {url}")
                response = await client.get(url)
                feed = feedparser.parse(response.content)
                
                if not feed.entries:
                    logger.warning(f"  ⚠️ Фид пуст или невалиден для URL: {url}")
                    continue
                
                entries_count = 0
                for entry in feed.entries[:5]:
                    title = entry.get("title", "").strip()
                    article_url = entry.get("link", "")
                    full_content = ""
                    
                    if article_url:
                        try:
                            logger.info(f"    📄 Загружаю полный текст: {article_url}")
                            article_response = await client.get(article_url, timeout=10.0)
                            
                            if article_response.status_code != 200:
                                logger.warning(f"    ❌ Сайт вернул ошибку {article_response.status_code} для {article_url}")
                            
                            full_content = trafilatura.extract(
                                article_response.content, 
                                target_language='ru', 
                                include_comments=False
                            )
                            
                            if full_content:
                                full_content = ' '.join(full_content.split())
                            else:
                                logger.warning(f"    ⚠️ Trafilatura не смогла найти блок текста на странице: {article_url}")
                                
                        except Exception as e:
                            logger.warning(f"    ⚠️ Не удалось загрузить полный текст: {e}")
                    
                    if not full_content:
                        raw_summary = entry.get("summary", entry.get("description", ""))
                        full_content = trafilatura.html2txt(raw_summary) if raw_summary else "Контент статьи не удалось извлечь."
                    
                    # ИСПРАВЛЕНО: Безопасное извлечение даты публикации из RSS (try/except перенесен выше)
                    try:
                        timestamp = entry.get("published", entry.get("updated", None))
                        if timestamp:
                            dt = date_parser.parse(timestamp)
                            date_str = dt.strftime("%Y-%m-%d")
                        else:
                            date_str = datetime.now().strftime("%Y-%m-%d")
                    except Exception:
                        date_str = datetime.now().strftime("%Y-%m-%d")
                    
                    payload = {
                        "title": title,                  
                        "content": full_content,         
                        "source_name": feed.feed.get("title", url), # ИСПРАВЛЕНО: Ключ source_name вместо source
                        "date": date_str,
                        "url": article_url
                    }
                    all_news.append(payload)
                    entries_count += 1
                
                logger.info(f"  ✅ Получено {entries_count} новостей с {url}")
                
            except Exception as e:
                logger.error(f"  ❌ Ошибка при парсинге сайта {url}: {e}")
                
    logger.info(f"📊 Всего собрано новостей с сайтов: {len(all_news)}")
    return all_news
