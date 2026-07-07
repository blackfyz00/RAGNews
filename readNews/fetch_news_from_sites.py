import os
from datetime import datetime
import feedparser
import httpx
from dateutil import parser as date_parser
from prefect import task, get_run_logger
import trafilatura

from read_lines_from_file import read_lines_from_file

@task(retries=3, retry_delay_seconds=30, name="Парсинг сайтов (RSS)")
async def fetch_news_from_sites(file_path: str = "sites.txt") -> list[dict]:
    """Скачивает свежие новости с сайтов, гарантированно обходя блокировки."""
    logger = get_run_logger()
    urls = read_lines_from_file(file_path)
    all_news = []
    
    if not urls:
        logger.info(f"ℹ️ Нет URL для парсинга сайтов в файле {file_path}")
        return all_news

    logger.info(f"🔍 Начинаю парсинг {len(urls)} сайтов...")
    
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
                for entry in feed.entries[:10]:
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
                    
                    timestamp = entry.get("published", datetime.now().isoformat())
                    try:
                        dt = date_parser.parse(timestamp)
                        timestamp = dt.strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
                    
                    payload = {
                        "title": title,                  
                        "content": full_content,         
                        "source": feed.feed.get("title", url),
                        "timestamp": timestamp,
                        "url": article_url
                    }
                    all_news.append(payload)
                    entries_count += 1
                
                logger.info(f"  ✅ Получено {entries_count} новостей с {url}")
                
            except Exception as e:
                logger.error(f"  ❌ Ошибка при парсинге сайта {url}: {e}")
                
    logger.info(f"📊 Всего собрано новостей с сайтов: {len(all_news)}")
    return all_news
