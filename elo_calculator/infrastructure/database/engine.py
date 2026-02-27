from urllib.parse import quote_plus

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from elo_calculator.configs import env


def get_db_url() -> str:
    user = quote_plus(env.DB_USERNAME)
    password = quote_plus(env.DB_PASSWORD)
    return f'postgresql+asyncpg://{user}:{password}@{env.DB_HOST}:{env.DB_PORT}/{env.DB_NAME}'


engine = create_async_engine(get_db_url(), echo=True)

metadata = MetaData()
