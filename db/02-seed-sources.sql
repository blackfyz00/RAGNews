INSERT INTO sources (source_type, url)
VALUES
    -- RSS-ленты сайтов
    ('site', 'https://habr.com/ru/rss/news/?fl=ru'),
    ('site', 'www.sberbank.ru'),
    ('site', 'https://www.opennet.ru/opennews/opennews_all.rss'),
    ('site', 'https://vc.ru/rss/all'),
    ('site', 'https://techcrunch.com/feed/'),
    ('site', 'https://www.theverge.com/rss/index.xml'),

    -- Telegram-каналы
    ('tg', '@sberbank'),
    ('tg', '@kodddurov'),
    ('tg', '@tproger_official'),
    ('tg', '@techrocks_ru'),
    ('tg', '@hi_ai_news'),
    ('tg', '@proglib');