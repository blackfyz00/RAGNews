import re
import unicodedata
from bs4 import BeautifulSoup
from typing import Dict, List


class TextNormalizer:
    """Нормализация текста для эмбеддингов (RAG-ready)."""

    def __init__(self):
        self.url_re = re.compile(r"https?://\S+", re.IGNORECASE)
        self.tg_link_re = re.compile(r"https?://t\.me/\S+", re.IGNORECASE)
        self.whitespace_re = re.compile(r"\s+")

        self.junk_re = re.compile(
            "|".join([
                r"Подпишитесь.*?(?=\n|$)",
                r"Все права защищены.*?(?=\n|$)",
                r"Реклама\.?.*?(?=\n|$)",
                r"Источник:\s*.*",
            ]),
            re.IGNORECASE | re.MULTILINE
        )

    # -------------------------
    # HTML
    # -------------------------
    def clean_html(self, text: str) -> str:
        if not text:
            return ""
        return BeautifulSoup(text, "html.parser").get_text(" ")

    # -------------------------
    # line dedup
    # -------------------------
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

        return " ".join(result)

    # -------------------------
    # core normalize
    # -------------------------
    def normalize(self, text: str) -> str:
        if not text:
            return ""

        text = self.clean_html(text)
        text = unicodedata.normalize("NFKC", text)

        text = self.url_re.sub(" ", text)
        text = self.tg_link_re.sub(" ", text)
        text = self.junk_re.sub(" ", text)

        text = self.remove_duplicate_lines(text)

        text = self.whitespace_re.sub(" ", text)

        return text.strip()


# -------------------------
# global instance (ВАЖНО)
# -------------------------
normalizer = TextNormalizer()


# -------------------------
# helpers
# -------------------------
def extract_title(text: str) -> str:
    if not text:
        return ""
    title = text.split("\n")[0].strip()
    return title if len(title) < 200 else ""


def remove_title_from_content(title: str, content: str) -> str:
    if not title or not content:
        return content

    content = content.strip()

    if content.startswith(title):
        content = content[len(title):].strip()

    return content


# -------------------------
# MAIN
# -------------------------
def normalize_news(news: dict) -> dict:
    result = news.copy()

    title = extract_title(result.get("text", ""))
    content = result.get("content", "")

    content = normalizer.normalize(content)
    content = remove_title_from_content(title, content)

    # final embedding text (ВАЖНО для RAG)
    if title and content:
        embedding_text = f"TITLE: {title}\nCONTENT: {content}"
    else:
        embedding_text = title or content

    result["normalized_text"] = content
    result["embedding_text"] = embedding_text
    result["normalized_title"] = title

    return result


def normalize_news_list(news_list: List[dict]) -> List[dict]:
    return [normalize_news(n) for n in news_list]