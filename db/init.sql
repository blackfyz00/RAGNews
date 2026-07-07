CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS bronze (
    id SERIAL PRIMARY KEY, -- ID новости

    source_name TEXT, -- Название источника: Habr, TechCrunch @proglib, Ria
    url TEXT, -- URL новости
    title TEXT, -- Заголовок новости
    content TEXT, -- Оригинальный текст новости
    date DATE -- дата публикации новости
);

CREATE TABLE IF NOT EXISTS silver ( -- очищенный от дубликатов слой
    id SERIAL PRIMARY KEY, -- ID новости

    source_name TEXT, -- Название источника: Habr, TechCrunch @proglib, Ria
    url TEXT, -- URL новости
    date DATE, -- дата публикации новости
    hash TEXT, -- хэш
    normalized_title TEXT, -- Заголовок новости
    normalized_content TEXT, -- текст структурирован и очищен
    links TEXT ARRAY, -- список ссылок
    embeddings VECTOR(1024) -- те же эмбеддинги
);

CREATE TABLE IF NOT EXISTS gold (
    id SERIAL PRIMARY KEY,  -- ID новости
    
    AI_title TEXT, -- заголовок от нейросети 
    AI_text TEXT, -- пересказ нейросетью 
    source_name TEXT, -- Название источника: Habr, TechCrunch @proglib, Ria
    url TEXT, -- URL новости
    date DATE, -- дата публикации новости
    links TEXT ARRAY, -- список ссылок
    embeddings VECTOR(1024) -- те же эмбеддинги
);
