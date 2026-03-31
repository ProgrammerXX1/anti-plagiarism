# ── Build stage ──
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ cmake make git pkg-config \
    libicu-dev libzip-dev libpoppler-cpp-dev \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY CMakeLists.txt .
COPY src/ src/

RUN mkdir -p out && cd out \
 && cmake .. -DCMAKE_BUILD_TYPE=Release \
 && make -j$(nproc)

# ── Runtime stage ──
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libicu74 libzip4 libpoppler-cpp0t64 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/out/detect_norm_cpp /app/detect_norm_cpp

# Copy static frontend from the Python project (shared UI)
COPY app/static/ /app/app/static/

# Copy test files from the Python project
COPY tests/ /app/tests/

EXPOSE 8001

CMD ["/app/detect_norm_cpp", "8001"]
