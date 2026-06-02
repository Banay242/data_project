FROM python:3.9-slim

WORKDIR /app

# Устанавливаем только необходимые легковесные библиотеки
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY strava_kafka_s3.py .

ENTRYPOINT ["python", "strava_kafka_s3.py"]