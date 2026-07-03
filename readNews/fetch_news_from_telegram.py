import os
import json
from datetime import datetime
import httpx
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from prefect import task, get_run_logger

from read_lines_from_file import read_lines_from_file

@task(retries=3, retry_delay_seconds=30, name="Парсинг Telegram каналов")
def fetch_news_from_telegram(file_path: str = "tgch.txt") -> list[dict]:
    """
    Парсит последние посты из Telegram каналов, указанных в файле tgch.txt.
    В timestamp пишется реальное время публикации поста.
    """
    logger = get_run_logger()
    channels = read_lines_from_file(file_path)
    all_news = []
    
    if not channels:
        logger.info(f"ℹ️ Нет Telegram каналов для парсинга в файле {file_path}")
        return all_news
    
    logger.info(f"📱 Начинаю парсинг {len(channels)} Telegram каналов...")
    
    for channel in channels:
        try:
            if not channel.startswith("@"):
                channel = f"@{channel}"
            
            channel_name = channel.replace("@", "")
            logger.info(f"  📡 Парсинг Telegram канала: {channel}")
            
            tg_url = f"https://t.me/s/{channel_name}"
            
            with httpx.Client(timeout=15.0, follow_redirects=True) as client:
                try:
                    response = client.get(tg_url)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    messages = soup.find_all('div', class_='tgme_widget_message_wrap')
                    if not messages:
                        messages = soup.find_all('div', {'data-post': True})
                    
                    posts_count = 0
                    for message in messages[:10]:
                        try:
                            text_div = message.find('div', class_='tgme_widget_message_text')
                            if not text_div:
                                continue
                            
                            text = text_div.get_text('\n', strip=True)
                            
                            post_link = message.find('a', class_='tgme_widget_message_date')
                            post_url = post_link.get('href', '') if post_link else ''
                            
                            # ---- СТРОГИЙ ПАРСИНГ ДАТЫ ПУБЛИКАЦИИ ----
                            time_tag = message.find('time')
                            timestamp = None  # Изначально дата неизвестна
                            
                            if time_tag and time_tag.get('datetime'):
                                raw_datetime = time_tag['datetime']  # Telegram отдает ISO строку, например: 2026-03-05T14:30:15+00:00
                                try:
                                    # Парсим ISO формат, который гарантирует точное время поста
                                    dt = date_parser.parse(raw_datetime)
                                    timestamp = dt.strftime("%Y-%m-%d %H:%M")
                                except Exception as e:
                                    logger.warning(f"      ⚠️ Не удалось распарсить ISO дату '{raw_datetime}': {e}")
                            
                            # Если дату поста вообще не нашли в HTML (редкий баг верстки ТГ)
                            if not timestamp:
                                logger.warning(f"      ⚠️ Дата поста не найдена в HTML. Пропускаем или пишем текущую.")
                                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
                            
                            payload = {
                                "title": text[:80] + "..." if len(text) > 80 else text,
                                "content": text,
                                "source": f"ТГ-канал {channel}",
                                "timestamp": timestamp,  
                                "url": post_url if post_url else f"https://t.me/{channel_name}" 
                            }
                            all_news.append(payload)
                            posts_count += 1
                                
                        except Exception as e:
                            logger.warning(f"    ⚠️ Ошибка при обработке поста: {e}")
                            continue
                    
                    logger.info(f"  ✅ Получено {posts_count} постов из канала {channel}")
                    
                except Exception as e:
                    logger.error(f"    ⚠️ Не удалось получить доступ к {tg_url}: {e}")
                    
        except Exception as e:
            logger.error(f"  ❌ Ошибка при парсинге канала {channel}: {e}")
    
    logger.info(f"📊 Всего собрано новостей из Telegram: {len(all_news)}")
    return all_news
