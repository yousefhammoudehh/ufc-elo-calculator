.PHONY: clean
clean:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -f .coverage

.PHONY: destroy
destroy:
	docker-compose down

.PHONY: lint
lint:
	poetry run ruff check .
	poetry run ruff format --check .
	poetry run mypy elo_calculator/

.PHONY: lint-fix
lint-fix:
	poetry run ruff check --fix .
	poetry run ruff format .

.PHONY: format
format:
	poetry run ruff format .

.PHONY: format-check
format-check:
	poetry run ruff format --check .

.PHONY: type-check
type-check:
	poetry run mypy elo_calculator/

.PHONY: refreeze
refreeze:
	bash bin/refreeze.sh

.PHONY: run
run:
	docker-compose up ufc-elo-calculator

build:
	docker-compose build ufc-elo-calculator

.PHONY: test
test:
	docker-compose run --rm ufc-elo-calculator-test

.PHONY: coverage
coverage:
	docker-compose run --rm ufc-elo-calculator-test sh -c " \
		coverage run -m pytest --durations=10 && \
		coverage report -m "

.PHONY: alembic-version

alembic-version:
	docker-compose run --rm ufc-elo-calculator sh -c " \
	    alembic revision --autogenerate -m \"${msg}\""