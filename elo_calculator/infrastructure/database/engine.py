from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from elo_calculator.configs import env


def get_db_url() -> str:
    return f'postgresql+asyncpg://{env.DB_USERNAME}:{env.DB_PASSWORD}@{env.DB_HOST}:{env.DB_PORT}/{env.DB_NAME}'


engine = create_async_engine(get_db_url(), echo=True)

metadata = MetaData()
