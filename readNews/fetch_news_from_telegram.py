from datetime import datetime
import httpx
from read_lines_from_file import read_lines_from_file
from bs4 import BeautifulSoup
from prefect import task

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
        
    try:
        # Записываем массив словарей в файл с отступами для читаемости
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 Успешно записано {len(data)} объектов в файл {output_file}")
    except Exception as e:
        print(f"❌ Ошибка при записи в файл {output_file}: {e}")
    
    return data

