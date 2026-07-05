import os
from datetime import datetime
import feedparser
import httpx
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from prefect import task, get_run_logger

from read_lines_from_file import read_lines_from_file

def _extract_article_body(article_soup: BeautifulSoup) -> str:
    """Внутренняя чистая функция для поиска контента статьи."""
    article_body = article_soup.find('div', class_='article__body')
    if not article_body:
        article_body = article_soup.find('div', class_='article__text')
    if not article_body:
        article_body = article_soup.find('div', itemprop='articleBody')
    
    if not article_body:
        # Общий поиск - ищем самый большой блок текста
        paragraphs = article_soup.find_all('p')
        if paragraphs:
            return ' '.join([p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 50])
        return ""
        
    if hasattr(article_body, 'get_text'):
        return article_body.get_text().strip()
    return str(article_body)


@task(retries=3, retry_delay_seconds=30, name="Парсинг сайтов (RSS)")
def fetch_news_from_sites(file_path: str = "sites.txt") -> list[dict]:
    """Скачивает свежие новости с сайтов, указанных в файле sites.txt"""
    logger = get_run_logger()  # ИСПРАВЛЕНО: Подключен логгер Prefect
    urls = read_lines_from_file(file_path)
    all_news = []
    
    if not urls:
        logger.info(f"ℹ️ Нет URL для парсинга сайтов в файле {file_path}")
        return all_news

    logger.info(f"🔍 Начинаю парсинг {len(urls)} сайтов...")
    
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for url in urls:
            try:
                logger.info(f"  📡 Парсинг RSS: {url}")
                response = client.get(url)
                feed = feedparser.parse(response.text)
                
                entries_count = 0
                for entry in feed.entries[:10]:
                    title = entry.get("title", "").strip()
                    
                    # Получаем краткое описание (анонс) из RSS
                    raw_summary = entry.get("summary", entry.get("description", ""))
                    short_text = BeautifulSoup(raw_summary, "html.parser").get_text().strip()
                    
                    if not short_text:
                        short_text = title
                    
                    article_url = entry.get("link", "")
                    full_content = ""
                    
                    if article_url:
                        try:
                            logger.info(f"    📄 Загружаю полный текст: {article_url}")
                            article_response = client.get(article_url)
                            article_soup = BeautifulSoup(article_response.text, 'html.parser')
                            
                            full_content = _extract_article_body(article_soup)
                            full_content = ' '.join(full_content.split())
                            
                        except Exception as e:
                            logger.warning(f"    ⚠️ Не удалось загрузить полный текст: {e}")
                            full_content = short_text
                    else:
                        full_content = short_text
                    
                    if not full_content:
                        full_content = short_text
                    
                    # Форматируем timestamp
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
                    
                    if entries_count % 5 == 0:
                        logger.info(f"    ⏸ Обработано {entries_count} новостей...")
                
                logger.info(f"  ✅ Получено {entries_count} новостей с {url}")
                
            except Exception as e:
                logger.error(f"  ❌ Ошибка при парсинге сайта {url}: {e}")
                
    logger.info(f"📊 Всего собрано новостей с сайтов: {len(all_news)}")
    return all_news