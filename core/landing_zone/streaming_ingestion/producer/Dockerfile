FROM python:3.11-slim

WORKDIR /app

# Copy requirements files
COPY requirements/base.txt requirements/base.txt
COPY requirements/producer.txt requirements/producer.txt

# Install dependencies
RUN pip install -r requirements/producer.txt

# Copy the entire producer folder and utils
COPY core/landing_zone/streaming_ingestion/producer ./producer
COPY utils ./utils

CMD ["python3", "producer/kafka_producer.py"]
