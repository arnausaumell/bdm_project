#!/bin/bash

# Run streaming ingestion with docker-compose
docker-compose up -d --build

# Run batch ingestion
chmod +x core/orchestration/deploy.sh
./core/orchestration/deploy.sh
