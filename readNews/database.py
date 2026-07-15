from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from DB_CONFIG import DB_CONFIG


DATABASE_URL = f"postgresql+asyncpg://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def get_async_session_factory():
    """
    Динамически создает движок и фабрику сессий.
    Это предотвращает утечку асинхронных блокировок в глобальную область видимости Prefect.
    """
    async_engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
    )

    return async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
