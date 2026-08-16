FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Создание пользователя для безопасности (не root)
RUN useradd -m -u 1000 telemetry && \
    chown -R telemetry:telemetry /app

USER telemetry

CMD ["python", "-m", "src.main"]