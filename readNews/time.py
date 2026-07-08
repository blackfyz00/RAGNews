# @task(retries=2, retry_delay_seconds=60, name="Парсинг Telegram-каналов")
# async def fetch_news_from_telegram(file_path: str, limit_per_channel: int = 5) -> list[dict]:
#     """Скачивает последние посты из Telegram-каналов, указанных в файле."""
#     channels = read_lines_from_file(file_path)
#     all_posts = []

#     if not channels:
#         return all_posts

#     # Создаем сессию Telethon (при первом запуске в консоли потребуется ввести код из TG)
#     client = TelegramClient("bot_parser_session", TG_API_ID, TG_API_HASH)
    
#     try:
#         await client.start()
        
#         for channel in channels:
#             try:
#                 # Очищаем юзернейм от ссылки, если скопировали целиком t.me/username
#                 channel_id = channel.split("/")[-1].replace("@", "")
                
#                 # Забираем сущность канала и последние посты
#                 entity = await client.get_entity(channel_id)
#                 async for message in client.iter_messages(entity, limit=limit_per_channel):
#                     # Нам нужны только текстовые посты (пропускаем пустые системные сообщения)
#                     if not message.text:
#                         continue
                        
#                     # Формируем payload для Бронзового слоя
#                     payload = {
#                         "text": message.text,
#                         "source": f"TG: {entity.title}",
#                         "timestamp": message.date.isoformat() if message.date else datetime.now().isoformat(),
#                         "url": f"https://t.me{entity.username}/{message.id}" if entity.username else f"https://t.mec/{entity.id}/{message.id}",
#                         "raw_author": channel
#                     }
#                     all_posts.append(payload)
#             except Exception as e:
#                 print(f"Ошибка при парсинге канала {channel}: {e}")
                
#     finally:
#         await client.disconnect()
        
#     return all_posts