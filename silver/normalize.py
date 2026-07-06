import re
import unicodedata
from bs4 import BeautifulSoup
from typing import Dict, List

class TextNormalizer:
    def __init__(self):
        self.url_re = re.compile(r"https?://\S+", re.IGNORECASE)
        self.tg_link_re = re.compile(r"https?://t\.me/\S+", re.IGNORECASE)
        self.whitespace_re = re.compile(r"\s+")
        
        # Время в конце строки (например, "09:08")
        self.time_re = re.compile(r"\d{2}:\d{2}$")

        self.junk_re = re.compile(
            "|".join([
                r"Подпишитесь.*?(?=\n|$)",
                r"Все права защищены.*?(?=\n|$)",
                r"Реклама\.?.*?(?=\n|$)",
                r"Источник:\s*.*",
                r"Краткий пересказ от РИА ИИ\s*",  # УДАЛЯЕМ эту фразу
                r"Краткий пересказ\s*",            # И другие варианты
                r"РИА Новости\.?\s*",              # Удаляем "РИА Новости." в начале
            ]),
            re.IGNORECASE | re.MULTILINE
        )

    # HTML
    def clean_html(self, text: str) -> str:
        if not text:
            return ""
        return BeautifulSoup(text, "html.parser").get_text(" ")

    # Удаление дублирующихся строк
    def remove_duplicate_lines(self, text: str) -> str:
        if not text:
            return ""

        seen = set()
        result = []

        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if line in seen:
                continue
            seen.add(line)
            result.append(line)

        return "\n".join(result)  # Сохраняем переносы строк

    # Удаление дублирующихся предложений
    def remove_duplicate_sentences(self, text: str) -> str:
        """Удаляет повторяющиеся предложения (по .!?)"""
        if not text:
            return ""
        
        # Разбиваем на предложения
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        seen = set()
        result = []
        
        for sent in sentences:
            sent = sent.strip()
            if not sent:
                continue
            # Нормализуем для сравнения (убираем лишние пробелы, приводим к нижнему регистру)
            sent_normalized = re.sub(r'\s+', ' ', sent).strip().lower()
            if sent_normalized in seen:
                continue
            seen.add(sent_normalized)
            result.append(sent)
        
        return " ".join(result)

    # Удаление коротких мусорных фраз в конце
    def remove_trailing_junk(self, text: str) -> str:
        """Удаляет мусор в конце текста (например, "В пяти аэропортах сняли ограничения09:08")"""
        if not text:
            return ""
        
        # Удаляем время в конце (09:08, 17:47 и т.д.)
        text = re.sub(r'\s*\d{2}:\d{2}\s*$', '', text)
        
        # Удаляем короткие фразы-ссылки в конце (например, "В пяти аэропортах сняли ограничения")
        # Ищем последнее предложение, если оно содержит "сняли ограничения" и короткое - удаляем
        sentences = re.split(r'(?<=[.!?])\s+', text)
        if len(sentences) > 1:
            last_sent = sentences[-1].strip()
            # Если последнее предложение содержит "сняли ограничения" и короткое (< 50 символов)
            if re.search(r'сняли ограничения', last_sent, re.IGNORECASE) and len(last_sent) < 50:
                sentences = sentences[:-1]
                text = " ".join(sentences)
        
        return text

    # core normalize
    def normalize(self, text: str) -> str:
        if not text:
            return ""

        # 1. Очистка HTML
        text = self.clean_html(text)
        
        # 2. Нормализация юникода
        text = unicodedata.normalize("NFKC", text)
        
        # 3. Удаление ссылок
        text = self.url_re.sub(" ", text)
        text = self.tg_link_re.sub(" ", text)
        
        # 4. Удаление мусорных фраз
        text = self.junk_re.sub(" ", text)
        
        # 5. Удаление дублирующихся строк
        text = self.remove_duplicate_lines(text)
        
        # 6. Удаление дублирующихся предложений
        text = self.remove_duplicate_sentences(text)
        
        # 7. Удаление мусора в конце
        text = self.remove_trailing_junk(text)
        
        # 8. Нормализация пробелов (но сохраняем абзацы)
        paragraphs = []
        for p in text.split("\n"):
            p = self.whitespace_re.sub(" ", p).strip()
            if p:
                paragraphs.append(p)
        
        text = "\n\n".join(paragraphs)

        return text.strip()

normalizer = TextNormalizer()

def extract_title(text: str) -> str:
    if not text:
        return ""
    title = text.split("\n")[0].strip()
    # Удаляем мусор из заголовка
    title = re.sub(r"Краткий пересказ от РИА ИИ\s*", "", title)
    title = re.sub(r"Краткий пересказ\s*", "", title)
    title = title.strip()
    return title if len(title) < 200 else ""

def remove_title_from_content(title: str, content: str) -> str:
    if not title or not content:
        return content

    content = content.strip()
    
    # Пробуем удалить заголовок из начала контента (разными способами)
    if content.startswith(title):
        content = content[len(title):].strip()
    
    # Если заголовок обрезан, пробуем удалить частично совпадающий
    # Например, title = "...выпуск воздуш", а content = "...выпуск воздушных судов"
    if title and len(title) > 20:
        # Ищем первое вхождение заголовка в контенте
        idx = content.find(title)
        if idx != -1:
            # Удаляем все до конца заголовка
            content = content[idx + len(title):].strip()
            # Если после заголовка идет "ных судов" и т.д., удаляем продолжение
            content = re.sub(r'^ных?\s+судов?\.?\s*', '', content)
            content = re.sub(r'^\.\s*', '', content)
    
    # Удаляем "Краткий пересказ от РИА ИИ" если осталось
    content = re.sub(r"Краткий пересказ от РИА ИИ\s*", "", content)
    content = re.sub(r"Краткий пересказ\s*", "", content)
    
    return content.strip()

def normalize_news(news: dict) -> dict:
    result = news.copy()

    # Берем title из поля title или из text
    title = result.get("title", "")
    if not title:
        title = extract_title(result.get("text", ""))
    else:
        title = extract_title(title)
    
    content = result.get("content", "")
    
    # Нормализуем контент
    content = normalizer.normalize(content)
    content = remove_title_from_content(title, content)
    
    # Если контент пустой, пробуем взять из text
    if not content:
        text = result.get("text", "")
        if text:
            # Убираем заголовок из text
            if title and text.startswith(title):
                text = text[len(title):].strip()
            content = normalizer.normalize(text)
    
    # Финальный текст для эмбеддинга
    if title and content:
        embedding_text = f"TITLE: {title}\nCONTENT: {content}"
    else:
        embedding_text = title or content

    result["title"] = title
    result["normalized_text"] = content
    result["embedding_text"] = embedding_text

    return result

def normalize_news_list(news_list: List[dict]) -> List[dict]:
    return [normalize_news(n) for n in news_list]