#!/bin/bash

# Exit on error
set -e

# Build the Docker image if it doesn't exist or if rebuild is requested
if [[ "$1" == "--build" || "$(docker images -q orchestration:latest 2> /dev/null)" == "" ]]; then
    echo "Building Docker image..."
    docker build -t orchestration:latest -f core/orchestration/Dockerfile .
fi

# Run the from_external_to_landing process in the Docker container
echo "Running from_external_to_landing process..."
docker run --rm \
    -v "$(pwd):/app" \
    -e PYTHONPATH=/app \
    orchestration:latest \
    python -m core.orchestration.from_external_to_landing
    
# Run the from_landing_to_trusted process in the Docker container
echo "Running from_landing_to_trusted process..."
docker run --rm \
    -v "$(pwd):/app" \
    -e PYTHONPATH=/app \
    orchestration:latest \
    python -m core.orchestration.from_landing_to_trusted

# Run the from_trusted_to_exploitation process in the Docker container
echo "Running from_trusted_to_exploitation process..."
docker run --rm \
    -v "$(pwd):/app" \
    -e PYTHONPATH=/app \
    orchestration:latest \
    python -m core.orchestration.from_trusted_to_exploitation
