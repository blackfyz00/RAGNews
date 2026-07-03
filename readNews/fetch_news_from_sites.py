from datetime import datetime
import feedparser
import httpx
from read_lines_from_file import read_lines_from_file
from bs4 import BeautifulSoup
from prefect import task

@task(retries=3, retry_delay_seconds=30, name="Парсинг сайтов (RSS)")
def fetch_news_from_sites(file_path: str = "sites.txt") -> list[dict]:
    """Скачивает свежие новости с сайтов, указанных в файле sites.txt"""
    urls = read_lines_from_file(file_path)
    all_news = []
    
    if not urls:
        print(f"ℹ️ Нет URL для парсинга сайтов в файле {file_path}")
        return all_news

    print(f"🔍 Начинаю парсинг {len(urls)} сайтов...")
    
    # Используем HTTPX для скачивания RSS-лент и страниц
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for url in urls:
            try:
                print(f"  📡 Парсинг RSS: {url}")
                response = client.get(url)
                # Парсим RSS xml структуру
                feed = feedparser.parse(response.text)
                
                entries_count = 0
                for entry in feed.entries[:10]:
                    # Получаем заголовок
                    title = entry.get("title", "").strip()
                    
                    # Получаем краткое описание (анонс) из RSS
                    raw_summary = entry.get("summary", entry.get("description", ""))
                    short_text = BeautifulSoup(raw_summary, "html.parser").get_text().strip()
                    
                    # Если нет summary в RSS, используем заголовок как анонс
                    if not short_text:
                        short_text = title
                    
                    # Получаем полный текст - переходим по ссылке на статью
                    article_url = entry.get("link", "")
                    full_content = ""
                    
                    if article_url:
                        try:
                            print(f"    📄 Загружаю полный текст: {article_url}")
                            article_response = client.get(article_url)
                            article_soup = BeautifulSoup(article_response.text, 'html.parser')
                            
                            # Ищем основной контент статьи (для РИА Новости)
                            article_body = article_soup.find('div', class_='article__body')
                            if not article_body:
                                article_body = article_soup.find('div', class_='article__text')
                            if not article_body:
                                article_body = article_soup.find('div', itemprop='articleBody')
                            if not article_body:
                                # Общий поиск - ищем самый большой блок текста
                                paragraphs = article_soup.find_all('p')
                                if paragraphs:
                                    article_body = ' '.join([p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 50])
                            
                            if article_body:
                                if hasattr(article_body, 'get_text'):
                                    full_content = article_body.get_text().strip()
                                else:
                                    full_content = article_body
                            
                            # Убираем лишние пробелы и переносы
                            full_content = ' '.join(full_content.split())
                            
                        except Exception as e:
                            print(f"    ⚠️ Не удалось загрузить полный текст: {e}")
                            full_content = short_text
                    else:
                        full_content = short_text
                    
                    # Если полный текст не найден, используем анонс
                    if not full_content:
                        full_content = short_text
                    
                    # Форматируем timestamp
                    timestamp = entry.get("published", datetime.now().isoformat())
                    try:
                        from dateutil import parser as date_parser
                        dt = date_parser.parse(timestamp)
                        timestamp = dt.strftime("%Y-%m-%d %H:%M")
                    except:
                        pass
                    
                    # Формируем payload для Бронзового слоя
                    payload = {
                        "text": title + "\n" + (short_text[:200] + "..." if len(short_text) > 200 else short_text),
                        "content": title + "\n" + full_content,
                        "source": feed.feed.get("title", url),
                        "timestamp": timestamp,
                        "url": article_url
                    }
                    all_news.append(payload)
                    entries_count += 1
                    
                    # Небольшая задержка между запросами к страницам
                    if entries_count % 5 == 0:
                        print(f"    ⏸ Обработано {entries_count} новостей...")
                
                print(f"  ✅ Получено {entries_count} новостей с {url}")
                
            except Exception as e:
                print(f"  ❌ Ошибка при парсинге сайта {url}: {e}")
                
    print(f"📊 Всего собрано новостей с сайтов: {len(all_news)}")
    return all_news