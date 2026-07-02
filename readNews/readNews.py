import os
import asyncio
from datetime import datetime
import feedparser
import httpx
import json
from bs4 import BeautifulSoup
from prefect import task, flow
from dotenv import load_dotenv

load_dotenv('telegram_creds.env')
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_DIR = os.path.dirname(__file__)

SITES_FILE = os.path.join(BASE_DIR, "sites.txt")
TG_FILE = os.path.join(BASE_DIR, "tgch.txt")

# ---- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ЧТЕНИЯ ФАЙЛОВ ----

def read_lines_from_file(file_path: str) -> list[str]:
    """Читает строки из файла, удаляя пустые строки и пробелы."""
    if not os.path.exists(file_path):
        print(f"⚠️ Предупреждение: Файл {file_path} не найден.")
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return lines


# ---- ЗАДАЧИ PREFECT (TASKS) ----

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

@task(retries=3, retry_delay_seconds=30, name="Парсинг Telegram каналов")
def fetch_news_from_telegram(file_path: str = "tgch.txt") -> list[dict]:
    """
    Парсит последние посты из Telegram каналов, указанных в файле tgch.txt
    Формат файла: один канал на строку
    """
    channels = read_lines_from_file(file_path)
    all_news = []
    
    if not channels:
        print(f"ℹ️ Нет Telegram каналов для парсинга в файле {file_path}")
        return all_news
    
    print(f"📱 Начинаю парсинг {len(channels)} Telegram каналов...")
    
    for channel in channels:
        try:
            # Проверяем что канал начинается с @
            if not channel.startswith("@"):
                channel = f"@{channel}"
            
            # Убираем @ для URL
            channel_name = channel.replace("@", "")
            
            print(f"  📡 Парсинг Telegram канала: {channel}")
            
            # Используем публичный веб-доступ к Telegram каналам
            tg_url = f"https://t.me/s/{channel_name}"
            
            with httpx.Client(timeout=15.0, follow_redirects=True) as client:
                try:
                    response = client.get(tg_url)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Ищем сообщения в канале
                    messages = soup.find_all('div', class_='tgme_widget_message_wrap')
                    
                    if not messages:
                        print(f"    ⚠️ Не найдено сообщений в канале {channel}")
                        # Пробуем альтернативный метод
                        messages = soup.find_all('div', {'data-post': True})
                    
                    posts_count = 0
                    # Ограничиваем последними 10 постами
                    for message in messages[:10]:
                        try:
                            # Извлекаем текст сообщения
                            text_div = message.find('div', class_='tgme_widget_message_text')
                            if not text_div:
                                continue
                            
                            # Получаем текст и очищаем от HTML
                            text = text_div.get_text('\n', strip=True)
                            
                            # Получаем ссылку на пост
                            post_link = message.find('a', class_='tgme_widget_message_date')
                            post_url = post_link.get('href', '') if post_link else ''
                            
                            # Получаем время публикации
                            time_tag = message.find('time')
                            if time_tag and time_tag.get('datetime'):
                                timestamp = time_tag['datetime']
                                try:
                                    from dateutil import parser as date_parser
                                    dt = date_parser.parse(timestamp)
                                    timestamp = dt.strftime("%Y-%m-%d %H:%M")
                                except:
                                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
                            else:
                                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
                            
                            # Формируем payload
                            payload = {
                                "text": text[:300] + "..." if len(text) > 300 else text,
                                "content": text,
                                "source": f"ТГ-канал {channel}",
                                "timestamp": timestamp,
                                "url": post_url if post_url else f"https://t.me/{channel_name}",
                                "raw_author": channel
                            }
                            all_news.append(payload)
                            posts_count += 1
                            
                            if posts_count % 3 == 0:
                                print(f"    ⏸ Обработано {posts_count} постов...")
                                
                        except Exception as e:
                            print(f"    ⚠️ Ошибка при обработке поста: {e}")
                            continue
                    
                    print(f"  ✅ Получено {posts_count} постов из канала {channel}")
                    
                except Exception as e:
                    print(f"    ⚠️ Не удалось получить доступ к {tg_url}: {e}")
                    print(f"    💡 Возможно канал приватный или требует прокси")
                    
        except Exception as e:
            print(f"  ❌ Ошибка при парсинге канала {channel}: {e}")
    
    print(f"📊 Всего собрано новостей из Telegram: {len(all_news)}")
    return all_news

@task(name="Сохранение в Бронзовый слой")
def save_to_bronze_layer_txt(data: list[dict], output_file: str = "test.txt") -> list[dict]:
    """Сохраняет сырые объекты в JSON-файл (Bronze Layer)."""
    if not data:
        print("❌ Нет данных для сохранения в Бронзовый слой.")
        return []

    output_path = os.path.join(BASE_DIR, output_file)

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"💾 Успешно записано {len(data)} объектов в файл {output_path}")

    except Exception as e:
        print(f"❌ Ошибка при записи в файл {output_path}: {e}")

    return data


# ---- ГЛАВНЫЙ ПОТОК (FLOW) ----

@flow(name="Новостной конвейер: Бронзовый Слой")
async def main_bronze_pipeline():
    print("🚀 Запуск процесса сбора новостей в Бронзовый слой...")
    print("="*50)
    
    # 1. Парсинг новостей с сайтов из файла sites.txt
    site_news = fetch_news_from_sites(file_path=SITES_FILE)
    tg_news = fetch_news_from_telegram(file_path=TG_FILE)
    
    # Объединяем массивы payload в один общий бронзовый пул
    raw_bronze_pool = site_news + tg_news
    
    # 3. Сохраняем все собранные данные в файл test.txt
    saved_bronze_data = save_to_bronze_layer_txt(
        data=raw_bronze_pool, 
        output_file="test.txt"
    )
    
    print("="*50)
    print(f"🏆 На этапе Бронзы собрано всего: {len(saved_bronze_data)} новостей.")
    
    # Статистика по источникам
    sources = {}
    for news in saved_bronze_data:
        source = news["source"]
        sources[source] = sources.get(source, 0) + 1
    
    if sources:
        print("\n📊 Статистика по источникам:")
        for source, count in sources.items():
            print(f"  • {source}: {count} новостей")
    
    return saved_bronze_data


if __name__ == "__main__":
    # Запуск потока на выполнение
    asyncio.run(main_bronze_pipeline())