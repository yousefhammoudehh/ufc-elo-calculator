from fastapi import APIRouter

app_router = APIRouter()


@app_router.get('/health', tags=['app'])
async def health() -> dict[str, str]:
    return {'status': 'ok'}


@app_router.get('/', tags=['app'])
async def root() -> dict[str, str]:
    return {'message': 'UFC ELO Calculator is running'}
