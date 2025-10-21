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
	docker-compose run --rm  olive-template-test sh -c " \
	    flake8 . && \
	    isort --check --diff . && \
		mypy olive_template/"

.PHONY: refreeze
refreeze:
	bash bin/refreeze.sh

.PHONY: run
run:
	docker-compose up olive-template

build:
	docker-compose build olive-template

.PHONY: test
test:
	docker-compose run --rm olive-template-test

.PHONY: coverage
coverage:
	docker-compose run --rm olive-template-test sh -c " \
		coverage run -m pytest --durations=10 && \
		coverage report -m "

.PHONY: alembic-version
alembic-version:
	docker-compose run --rm olive-template sh -c " \
	    alembic revision --autogenerate -m \"${msg}\""