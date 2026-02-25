#!/usr/bin/env sh

set -eu

BIN_ROOT=$(dirname "$0")
ENVIRONMENT="${ENVIRONMENT:-development}"
DB_HOST="${DB_HOST:-}"
DB_PORT="${DB_PORT:-}"

if [ -n "$DB_HOST" ] && [ -n "$DB_PORT" ]; then
    "$BIN_ROOT/wait-for-it.sh" -s -t 15 "$DB_HOST:$DB_PORT"
fi

if [ "$ENVIRONMENT" = "development" ] || [ "$ENVIRONMENT" = "dev" ]; then
    exec uvicorn --reload --workers 1 --host 0.0.0.0 --port 80 elo_calculator.app:app
else
    exec gunicorn --config=gunicorn_conf.py elo_calculator.app:app
fi
