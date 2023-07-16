try:
    import asyncio
except ImportError:
    raise ImportError("asycio is not found, pleas make sure you have it or use sqliteORM instead of AsyncSqliteORM")