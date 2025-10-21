# syntax=docker/dockerfile:1.3
FROM python:3.13-slim AS base

ENV PATH=/opt/local/bin:$PATH \
    PIP_PREFIX=/opt/local \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1


FROM base AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    make \
    build-essential \
    curl \
    libxml2-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM base AS deploy

ENV PATH=/opt/local/bin:$PATH \
    PYTHONPATH=/opt/local/lib/python3.13/site-packages:/app

COPY --from=build /opt/local /opt/local
COPY --from=build /usr/lib/x86_64-linux-gnu/ /lib/x86_64-linux-gnu/ /usr/lib/

WORKDIR /app
COPY . /app

EXPOSE 80
STOPSIGNAL SIGINT

CMD ["sh", "bin/boot.sh"]
