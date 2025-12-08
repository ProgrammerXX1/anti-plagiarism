FROM python:3.11-slim

WORKDIR /app

# системные зависимости для OCR, PDF и C++-toolchain
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        nlohmann-json3-dev \
        libsimdjson-dev \
        tesseract-ocr \
        tesseract-ocr-eng \
        tesseract-ocr-rus \
        tesseract-ocr-kaz \
        ocrmypdf \
        poppler-utils \
        ghostscript \
    && rm -rf /var/lib/apt/lists/*

# Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект (включая cpp/)
COPY . .

# Сборка C++ частей
RUN set -ex && \
    ls -R cpp && \
    g++ -O3 -std=c++17 -fPIC -shared \
        cpp/search_core.cpp \
        -o /usr/local/lib/libsearchcore.so && \
    g++ -O3 -std=c++17 \
        cpp/index_builder.cpp \
        -o /usr/local/bin/index_builder \
        -lsimdjson && \
    g++ -O3 -std=c++17 \
        cpp/etl_index_builder.cpp \
        -o /usr/local/bin/etl_index_builder \
        -lsimdjson

ENV LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}

CMD ["uvicorn", "app.main_ops:app", "--host", "0.0.0.0", "--port", "8002"]
