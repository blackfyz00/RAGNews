import re
import unicodedata
from bs4 import BeautifulSoup
from typing import Dict, List

class TextNormalizer:
    def __init__(self):
        self.url_re = re.compile(r"https?://\S+", re.IGNORECASE)
        self.tg_link_re = re.compile(r"https?://t\.me/\S+", re.IGNORECASE)
        self.whitespace_re = re.compile(r"\s+")

        self.time_re = re.compile(r"\d{2}:\d{2}$")

        self.junk_re = re.compile(
            "|".join([
                r"Подпишитесь.*?(?=\n|$)",
                r"Все права защищены.*?(?=\n|$)",
                r"Реклама\.?.*?(?=\n|$)",
                r"Источник:\s*.*",
                r"Краткий пересказ от РИА ИИ\s*",
                r"Краткий пересказ\s*",
                r"РИА Новости\.?\s*",
            ]),
            re.IGNORECASE | re.MULTILINE
        )

    # HTML
    def clean_html(self, text: str) -> str:
        if not text:
            return ""
        return BeautifulSoup(text, "html.parser").get_text(" ")

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

        return "\n".join(result)

    def remove_duplicate_sentences(self, text: str) -> str:
        """Удаляет повторяющиеся предложения (по .!?)"""
        if not text:
            return ""

        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        seen = set()
        result = []
        
        for sent in sentences:
            sent = sent.strip()
            if not sent:
                continue
            sent_normalized = re.sub(r'\s+', ' ', sent).strip().lower()
            if sent_normalized in seen:
                continue
            seen.add(sent_normalized)
            result.append(sent)
        
        return " ".join(result)

    def remove_trailing_junk(self, text: str) -> str:
        """Удаляет мусор в конце текста (например, "В пяти аэропортах сняли ограничения09:08")"""
        if not text:
            return ""

        text = re.sub(r'\s*\d{2}:\d{2}\s*$', '', text)

        sentences = re.split(r'(?<=[.!?])\s+', text)
        if len(sentences) > 1:
            last_sent = sentences[-1].strip()
            if re.search(r'сняли ограничения', last_sent, re.IGNORECASE) and len(last_sent) < 50:
                sentences = sentences[:-1]
                text = " ".join(sentences)
        
        return text

    # core normalize
    def normalize(self, text: str) -> str:
        if not text:
            return ""

        text = self.clean_html(text)

        text = unicodedata.normalize("NFKC", text)

        text = self.url_re.sub(" ", text)
        text = self.tg_link_re.sub(" ", text)

        text = self.junk_re.sub(" ", text)

        text = self.remove_duplicate_lines(text)

        text = self.remove_duplicate_sentences(text)

        text = self.remove_trailing_junk(text)

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
    title = re.sub(r"Краткий пересказ от РИА ИИ\s*", "", title)
    title = re.sub(r"Краткий пересказ\s*", "", title)
    title = title.strip()
    return title if len(title) < 200 else ""

def remove_title_from_content(title: str, content: str) -> str:
    if not title or not content:
        return content

    content = content.strip()

    if content.startswith(title):
        content = content[len(title):].strip()

    if title and len(title) > 20:
        idx = content.find(title)
        if idx != -1:
            content = content[idx + len(title):].strip()
            content = re.sub(r'^ных?\s+судов?\.?\s*', '', content)
            content = re.sub(r'^\.\s*', '', content)

    content = re.sub(r"Краткий пересказ от РИА ИИ\s*", "", content)
    content = re.sub(r"Краткий пересказ\s*", "", content)
    
    return content.strip()

def normalize_news(news: dict) -> dict:
    result = news.copy()

    title = result.get("title", "")
    if not title:
        title = extract_title(result.get("text", ""))
    else:
        title = extract_title(title)
    
    content = result.get("content", "")

    content = normalizer.normalize(content)
    content = remove_title_from_content(title, content)

    if not content:
        text = result.get("text", "")
        if text:
            if title and text.startswith(title):
                text = text[len(title):].strip()
            content = normalizer.normalize(text)

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
