# UFC ELO Calculator

Current app surface:
- `GET /health` for service health
- `GET /` for root message

Core base components:
- `elo_calculator/application/base_service.py`
- `elo_calculator/infrastructure/repositories/base_repository.py`
- `elo_calculator/domain/entities/base_entity.py`
- `elo_calculator/presentation/routers/app_router.py`

## Local Run

1. Install dependencies:
   - `poetry install --with dev`
2. Run the API:
   - `poetry run uvicorn elo_calculator.app:app --reload --host 0.0.0.0 --port 80`
3. Verify health:
   - `curl http://localhost:80/health`

## Lint

- `make lint`

## Docker

- Build: `make build`
- Run: `make run`
- Stop: `make down`

## Data Normalization

- Example walkthrough notebook: `notebooks/01_normalized_fighter_walkthrough.ipynb`
