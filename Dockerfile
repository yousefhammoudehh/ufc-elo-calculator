FROM python:3.13-alpine AS builder

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=off  
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV POETRY_VERSION=2.1.3
ENV POETRY_VIRTUALENVS_CREATE=false

RUN apk add --no-cache gcc musl-dev libffi-dev postgresql-dev build-base
RUN pip install "poetry==${POETRY_VERSION}"

WORKDIR /app

COPY pyproject.toml poetry.lock* ./

RUN poetry install --only main --no-interaction --no-ansi  --no-root

###########
###########
FROM python:3.13-alpine

ARG ENVIRONMENT
ENV ENVIRONMENT=${ENVIRONMENT}
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1 
ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_VIRTUALENVS_IN_PROJECT=false

RUN apk add --no-cache libpq bash

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY . .

EXPOSE 80
STOPSIGNAL SIGINT

CMD ["sh", "bin/boot.sh"]
