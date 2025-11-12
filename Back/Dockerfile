FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
ENV PLAGIO_ROOT=/data
RUN mkdir -p /data && adduser --disabled-password --gecos "" app && chown -R app:app /data /app
USER app
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000","--proxy-headers"]
