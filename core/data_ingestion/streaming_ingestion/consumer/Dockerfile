FROM python:3.10-slim

WORKDIR /app

# Install Java 8
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    curl -s https://repos.azul.com/azul-repo.key | gpg --dearmor -o /usr/share/keyrings/azul.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/azul.gpg] https://repos.azul.com/zulu/deb stable main" | tee /etc/apt/sources.list.d/zulu.list && \
    apt-get update && \
    apt-get install -y zulu8-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/zulu8
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy requirements files
COPY requirements/base.txt requirements/base.txt
COPY requirements/consumer.txt requirements/consumer.txt

# Copy .env file
COPY .env .env

# Install dependencies
RUN pip install -r requirements/consumer.txt

# Copy service code and ensure proper Python package structure
COPY core/ ./core/
RUN touch ./core/__init__.py \
    && touch ./core/data_ingestion/__init__.py \
    && touch ./core/data_ingestion/batch_ingestion/__init__.py \
    && touch ./core/data_ingestion/streaming_ingestion/__init__.py \
    && touch ./core/landing_and_trusted_zone/__init__.py

# Set Python path to include the app directory
ENV PYTHONPATH=/app

# The correct path to the consumer script
CMD ["python3", "/app/core/data_ingestion/streaming_ingestion/consumer/kafka_consumer.py"]
