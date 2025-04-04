#!/bin/bash

# Run streaming ingestion with docker-compose
docker-compose up -d --build

# Run batch ingestion
chmod +x core/landing_zone/batch_ingestion/run_batch_ingestion.sh
./core/landing_zone/batch_ingestion/run_batch_ingestion.sh

