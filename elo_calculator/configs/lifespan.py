from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from elo_calculator.infrastructure.database.data_seeder import seed_data


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[Any]:
    await seed_data()
    yield