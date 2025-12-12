FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential nlohmann-json3-dev libsimdjson-dev \
    tesseract-ocr tesseract-ocr-eng tesseract-ocr-rus tesseract-ocr-kaz \
    ocrmypdf poppler-utils ghostscript \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN set -ex && \
    ls -R cpp && \
    mkdir -p /tmp/build && \
    \
    g++ -O2 -std=c++17 -fPIC -Icpp/common \
        -c cpp/common/search_core.cpp \
        -o /tmp/build/search_core.o && \
    g++ -shared -o /usr/local/lib/libsearchcore.so \
        /tmp/build/search_core.o && \
    \
    g++ -O2 -std=c++17 -fPIC -Icpp -Icpp/common -Icpp/levels_0_4 \
        -c cpp/levels_0_4/search_engine.cpp \
        -o /tmp/build/search_engine.o && \
    g++ -O2 -std=c++17 -fPIC -Icpp -Icpp/common -Icpp/levels_0_4 \
        -c cpp/levels_0_4/search_segments.cpp \
        -o /tmp/build/search_segments.o && \
    g++ -shared -Wl,--no-undefined \
        -o /usr/local/lib/libplagio_segments.so \
        /tmp/build/search_engine.o \
        /tmp/build/search_segments.o && \
    \
    g++ -O2 -std=c++17 \
        cpp/level5/index_builder.cpp \
        -Icpp/common \
        -o /usr/local/bin/index_builder \
        -lsimdjson && \
    \
    g++ -O2 -std=c++17 \
        cpp/levels_0_4/etl_index_builder.cpp \
        -Icpp/common \
        -o /usr/local/bin/etl_index_builder \
        -lsimdjson && \
    \
    ls -la /usr/local/lib && \
    ls -la /usr/local/bin

ENV LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}
ENV PLAGIO_SEGMENTS_SO=/usr/local/lib/libplagio_segments.so

CMD ["uvicorn", "app.main_ops:app", "--host", "0.0.0.0", "--port", "8000"]
