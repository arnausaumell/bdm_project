## BDM Movies Database

### Landing Zone

Instructions to run the Landing zone ingestion:

```bash
chmod +x core/landing_zone/run_landing_zone_ingestion.sh
./core/landing_zone/run_landing_zone_ingestion.sh
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
