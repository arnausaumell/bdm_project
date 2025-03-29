FROM python:3.11-slim

WORKDIR /app

# Copy requirements files
COPY requirements/base.txt requirements/base.txt
COPY requirements/comments.txt requirements/comments.txt

# Install dependencies
RUN pip install -r requirements/comments.txt

# Copy service code
COPY core/landing_zone/streaming_ingestion/comments_generator/ ./comments_generator

CMD ["python3", "comments_generator/app.py"] 