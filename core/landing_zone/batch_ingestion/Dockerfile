FROM apache/airflow:2.7.3

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Create the directory structure
RUN mkdir -p /opt/airflow/core/data_ingestion /opt/airflow/utils

USER airflow

# Install requirements
COPY requirements/batch_ingestion.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the necessary files
COPY core/data_ingestion /opt/airflow/core/data_ingestion/
COPY utils /opt/airflow/utils/
COPY core/landing_zone/batch_ingestion/load_tables.py /opt/airflow/dags/

# Set environment variables for API keys and configurations
ENV PYTHONPATH=/opt/airflow
ENV TMDB_API_KEY=${TMDB_API_KEY}
ENV TRAKT_CLIENT_ID=${TRAKT_CLIENT_ID}
ENV TRAKT_CLIENT_SECRET=${TRAKT_CLIENT_SECRET}
ENV OMDB_API_KEY=${OMDB_API_KEY}
ENV YOUTUBE_API_KEY=${YOUTUBE_API_KEY}
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
ENV AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}