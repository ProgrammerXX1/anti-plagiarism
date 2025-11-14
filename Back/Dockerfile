FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAGIO_ROOT=/runtime

WORKDIR /app

# зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# код
COPY app ./app

# директория под runtime (corpus/index/uploads/logs)
RUN mkdir -p /runtime
