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
COPY requirements/batch_ingestion.txt requirements/batch_ingestion.txt

# Install dependencies
RUN pip install -r requirements/batch_ingestion.txt

# Copy the entire project
COPY . .

# Set environment variables
ENV PYTHONPATH=/app

# Set Spark environment variables
ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin