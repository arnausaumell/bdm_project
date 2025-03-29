FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install kafka-python loguru

CMD ["python3", "kafka_consumer.py"]
