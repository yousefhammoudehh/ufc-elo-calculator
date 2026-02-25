POETRY_ENV = POETRY_VIRTUALENVS_IN_PROJECT=true POETRY_CACHE_DIR=.poetry-cache
POETRY_RUN = $(POETRY_ENV) poetry run

.PHONY: clean
clean:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf .poetry-cache
	rm -f .coverage

.PHONY: destroy
destroy:
	docker compose down

.PHONY: lint
lint:
	$(POETRY_RUN) ruff check .
	$(POETRY_RUN) ruff format --check .
	$(POETRY_RUN) mypy elo_calculator/

.PHONY: lint-fix
lint-fix:
	$(POETRY_RUN) ruff check --fix .
	$(POETRY_RUN) ruff format .

.PHONY: format
format:
	$(POETRY_RUN) ruff format .

.PHONY: format-check
format-check:
	$(POETRY_RUN) ruff format --check .

.PHONY: type-check
type-check:
	$(POETRY_RUN) mypy elo_calculator/

.PHONY: refreeze
refreeze:
	bash bin/refreeze.sh

.PHONY: run
run:
	docker compose up ufc-elo-calculator

.PHONY: build
build:
	docker compose build ufc-elo-calculator

.PHONY: down
down:
	docker compose down

.PHONY: test
test:
	docker compose run --rm ufc-elo-calculator-test

.PHONY: coverage
coverage:
	docker compose run --rm ufc-elo-calculator-test sh -c " \
		coverage run -m pytest --durations=10 && \
		coverage report -m "
