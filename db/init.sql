-- Подключаем расширение pgvector.
-- Оно нужно, чтобы PostgreSQL мог хранить embedding-векторы
-- и делать по ним векторный поиск для RAG.
CREATE EXTENSION IF NOT EXISTS vector;


-- ============================================================
-- BRONZE LAYER
-- ============================================================
-- Это сырой слой данных.
-- Сюда попадают новости сразу после парсинга RSS / Telegram.
-- На этом этапе мы НИЧЕГО не чистим и НЕ удаляем дубли.
-- Просто сохраняем исходный вид новости, чтобы потом можно было
-- переобработать данные, если что-то сломается дальше.
CREATE TABLE IF NOT EXISTS bronze (
    -- Уникальный ID сырой записи
    id SERIAL PRIMARY KEY,

    -- Название источника, например: Habr, TechCrunch, @proglib
    source_name TEXT,

    -- Тип источника: rss / telegram / site / unknown
    source_type TEXT,

    -- URL самого источника, например RSS-лента или ссылка на канал
    source_url TEXT,

    -- Ссылка на конкретную новость / пост
    original_url TEXT,

    -- Полный сырой JSON, который пришёл после парсинга.
    -- Тут можно хранить всё: title, text, content, timestamp, url и т.д.
    raw_payload JSONB NOT NULL,

    -- Когда мы собрали эту новость
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Статус обработки:
    -- new       = только что загружена
    -- processed = обработана и отправлена дальше
    -- failed    = при обработке была ошибка
    processing_status TEXT DEFAULT 'new',

    -- Текст ошибки, если новость не удалось обработать
    error_message TEXT
);


-- ============================================================
-- SILVER LAYER
-- ============================================================
-- Это очищенный слой данных.
-- Сюда попадают новости после очистки, нормализации,
-- дедупликации и получения embedding.
--
-- Silver = основной корпус для RAG-поиска.
-- Именно по этой таблице потом ищем похожие новости.
CREATE TABLE IF NOT EXISTS silver (
    -- Уникальный ID очищенной новости
    id SERIAL PRIMARY KEY,

    -- Нормальный заголовок новости
    title TEXT NOT NULL,

    -- Очищенный текст новости:
    -- без HTML, мусора, лишних пробелов и т.д.
    clean_text TEXT NOT NULL,

    -- Основная ссылка на новость.
    -- Если одна новость была найдена в нескольких источниках,
    -- тут можно хранить главную / каноническую ссылку.
    canonical_url TEXT,

    -- Дата публикации новости, если её удалось достать
    published_at TIMESTAMP,

    -- Хэш содержимого новости.
    -- Нужен для простой дедупликации:
    -- если у двух новостей одинаковый content_hash,
    -- значит текст у них одинаковый или почти одинаковый.
    content_hash TEXT UNIQUE,

    -- Embedding очищенного текста новости.
    -- Это вектор, по которому будет работать RAG-поиск.
    --
    -- VECTOR(1024) значит, что размерность embedding = 1024.
    -- Если ваша embedding-модель вернёт другую размерность,
    -- например 1536, это число надо будет поменять.
    embedding VECTOR(1024),

    -- Все ссылки на источники этой новости.
    -- Например, одна и та же новость могла прийти из RSS и Telegram.
    -- Тогда в source_links будет массив ссылок.
    source_links JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- ID записей из bronze, из которых получилась эта silver-новость.
    -- Например, если одна новость пришла из трёх источников,
    -- тут могут быть bronze_ids = {1, 7, 12}
    bronze_ids INTEGER[] DEFAULT '{}',

    -- Дополнительные метаданные.
    -- Сюда можно складывать теги, язык, автора, категорию,
    -- длину текста и другие поля, которые пока не хочется
    -- делать отдельными колонками.
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Когда запись была создана в silver
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Когда запись последний раз обновлялась
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- GOLD LAYER
-- ============================================================
-- Это готовый слой данных для пользователя.
-- Сюда попадает результат обработки нейронкой:
-- summary, topic, digest_text и т.д.
--
-- Gold = слой, откуда бот / API может быстро забрать
-- уже готовую информацию для дайджеста.
CREATE TABLE IF NOT EXISTS gold (
    -- Уникальный ID gold-записи
    id SERIAL PRIMARY KEY,

    -- Ссылка на очищенную новость из silver.
    -- То есть gold не хранит всю новость заново,
    -- а привязывается к её silver-версии.
    silver_id INTEGER REFERENCES silver(id) ON DELETE CASCADE,

    -- Тема новости, которую определила нейронка.
    -- Например: AI, Backend, Security, DevOps, Mobile
    topic TEXT,

    -- Краткое резюме новости от нейронки
    summary TEXT NOT NULL,

    -- Готовый текст для дайджеста.
    -- Может быть более красивым и коротким, чем summary.
    digest_text TEXT,

    -- Полный ответ нейронки в JSON.
    -- Например:
    -- {
    --   "topic": "AI",
    --   "summary": "...",
    --   "importance": "high",
    --   "keywords": ["OpenAI", "LLM"]
    -- }
    ai_payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Ссылки на источники.
    -- Дублируем их тут специально, чтобы бот мог быстро
    -- выдать summary + ссылки, не делая лишний JOIN.
    source_links JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- Флаг: можно ли эту запись показывать в дайджесте
    is_ready_for_digest BOOLEAN DEFAULT TRUE,

    -- Когда запись была создана в gold
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- INDEXES
-- ============================================================
-- Индексы нужны, чтобы база быстрее искала данные.
-- Без них всё тоже будет работать, но медленнее.


-- Индекс по ссылке на оригинальную новость в bronze.
-- Нужен, чтобы быстро проверить, загружали ли мы уже эту ссылку.
CREATE INDEX IF NOT EXISTS bronze_original_url_idx
ON bronze (original_url);


-- Индекс по статусу обработки bronze-записей.
-- Нужен, чтобы быстро находить новые необработанные новости:
-- SELECT * FROM bronze WHERE processing_status = 'new';
CREATE INDEX IF NOT EXISTS bronze_status_idx
ON bronze (processing_status);


-- Индекс по хэшу текста в silver.
-- Нужен для быстрой дедупликации по одинаковому тексту.
CREATE INDEX IF NOT EXISTS silver_hash_idx
ON silver (content_hash);


-- Индекс по канонической ссылке в silver.
-- Нужен, чтобы быстро проверять, есть ли уже такая новость.
CREATE INDEX IF NOT EXISTS silver_url_idx
ON silver (canonical_url);


-- Индекс по теме в gold.
-- Нужен, чтобы быстро доставать новости по теме:
-- AI, Backend, Security и т.д.
CREATE INDEX IF NOT EXISTS gold_topic_idx
ON gold (topic);


-- Векторный индекс по embedding.
-- Нужен для быстрого семантического поиска похожих новостей.
-- Именно он будет использоваться в RAG.
--
-- Пример будущего поиска:
-- SELECT *
-- FROM silver
-- ORDER BY embedding <=> '[вектор вопроса]'
-- LIMIT 5;
CREATE INDEX IF NOT EXISTS silver_embedding_idx
ON silver
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);