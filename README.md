## BDM Movies Database

### Landing Zone

Instructions to run the Data ingestion pipeline:

```bash
run_orchestration.sh # Start the Prefect server, worker and set up deployments
docker-compose up -d # Start the streaming ingestion and the comments generator
```

File structure:

- core
  - data_ingestion
    - batch_ingestion: set of external connectors to ingest data from external sources
    - streaming_ingestion: 
        - comments_generator: synthetic service to generate comments for movies
        - consumer: Kafka consumer to ingest comments from the comments_generator
        - producer: Kafka producer to send comments to the comments_generator
  - landing_and_trusted_zone: S3 and DeltaLake managers
  - exploitation_zone: Supabase and Pinecone
  - orchestration: Prefect flows
    - from_external_to_landing
    - from_landing_to_trusted
    - from_trusted_to_exploitation


### Run tests

```
python3 -m pytest tests/data_ingestion/batch_ingestion/*
python -m pytest tests/data_ingestion/streaming_ingestion/*
```