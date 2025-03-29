FROM python:3.10-slim

WORKDIR /app

# Copy requirements files
COPY requirements/base.txt requirements/base.txt
COPY requirements/batch_ingestion.txt requirements/batch_ingestion.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements/batch_ingestion.txt

# Copy the necessary code
COPY core/landing_zone/batch_ingestion /app/batch_ingestion
COPY core/data_ingestion /app/data_ingestion
COPY utils /app/utils

# Set the Python path
ENV PYTHONPATH=/app

CMD ["python3", "batch_ingestion/load_tables.py"] 