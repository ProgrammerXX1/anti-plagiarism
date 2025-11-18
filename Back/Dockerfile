FROM python:3.11-slim

WORKDIR /app

# системные зависимости для OCR и PDF
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
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

COPY . .

CMD ["uvicorn", "app.main_ops:app", "--host", "0.0.0.0", "--port", "8002"]
