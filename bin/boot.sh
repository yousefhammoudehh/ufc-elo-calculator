#!/usr/bin/env bash

set -e

BIN_ROOT=$(dirname $0)

$BIN_ROOT/wait-for-it.sh -s -t 15 $DB_HOST:$DB_PORT

alembic upgrade head

if [ "$ENVIRONMENT" = "development" ]; then
    uvicorn --reload --workers 1 --host 0.0.0.0 --port 80 elo_calculator.app:app
else
    gunicorn --config=gunicorn_conf.py elo_calculator.app:app
fi
