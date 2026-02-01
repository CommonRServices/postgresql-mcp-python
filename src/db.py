from asyncpg import create_pool
from dotenv import load_dotenv

from settings import settings


load_dotenv()


async def init_db_pool():
    pool = await create_pool(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
    )
    return pool
