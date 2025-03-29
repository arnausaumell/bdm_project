FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir fastapi uvicorn kafka-python loguru websockets

CMD ["python3", "kafka_producer.py"]
