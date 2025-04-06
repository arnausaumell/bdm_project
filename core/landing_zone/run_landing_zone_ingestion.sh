#!/bin/bash

# Run streaming ingestion with docker-compose
docker-compose up -d --build

# Run batch ingestion
chmod +x core/data_ingestion/batch_ingestion/run_batch_ingestion.sh
./core/data_ingestion/batch_ingestion/run_batch_ingestion.sh

