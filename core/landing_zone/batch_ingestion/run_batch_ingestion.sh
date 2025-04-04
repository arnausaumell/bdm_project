#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
BLUE='\033[0;34m'

# Install dependencies
echo -e "${BLUE}Installing required dependencies...${NC}"
pip install -r requirements/batch_ingestion.txt
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to install dependencies${NC}"
    exit 1
fi
echo -e "${GREEN}Dependencies installed successfully${NC}"

echo -e "${BLUE}Setup Overview:${NC}"
echo "1. Local components (on your machine):"
echo "   - Prefect server (orchestrator)"
echo "   - Prefect worker (container manager)"
echo "   - Deployment script (one-time setup)"
echo ""
echo "2. Docker components (in container):"
echo "   - All flow code execution"
echo "   - All dependencies from requirements/batch_ingestion.txt"
echo "   - Actual data processing"

# Get the absolute path to the project root
PROJECT_ROOT=$(pwd)

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | xargs)
else
    echo -e "${RED}No .env file found. Please create one with required credentials${NC}"
    exit 1
fi

# Set PYTHONPATH to include project root
export PYTHONPATH=$PROJECT_ROOT:$PYTHONPATH

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

# Docker Hub Login using env variables
echo -e "${BLUE}Logging into Docker Hub...${NC}"
if ! echo "$DOCKER_PASSWORD" | docker login --username $DOCKER_USERNAME --password-stdin; then
    echo -e "${RED}Failed to log in to Docker Hub. Please check your credentials in .env file.${NC}"
    exit 1
fi
echo -e "${GREEN}Successfully logged into Docker Hub!${NC}"

# Configure Prefect server URL
echo -e "${BLUE}Configuring Prefect server...${NC}"
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

# Start Prefect server in the background
echo -e "${BLUE}Starting Prefect server...${NC}"
prefect server start &
SERVER_PID=$!

# Wait for server to be ready
echo "Waiting for server to start..."
sleep 10

# Create Docker work pool
echo -e "${BLUE}Creating Docker work pool...${NC}"
prefect work-pool create --type docker my-docker-pool --overwrite || true

# Start worker in the background
echo -e "${BLUE}Starting Prefect worker...${NC}"
prefect worker start --pool my-docker-pool &
WORKER_PID=$!

# Wait for worker to be ready
sleep 5

# Build and deploy the flow
echo -e "${BLUE}Building and deploying the batch ingestion flow...${NC}"
docker build -t arnausau11/batch-ingestion:latest -f core/landing_zone/batch_ingestion/Dockerfile .
docker push arnausau11/batch-ingestion:latest

# Run the Python script with the correct PYTHONPATH
python core/landing_zone/batch_ingestion/load_tables.py

echo -e "${GREEN}Setup complete!${NC}"
echo -e "${GREEN}Prefect UI is available at: http://127.0.0.1:4200${NC}"
echo -e "${BLUE}Process IDs:${NC}"
echo "Server PID: $SERVER_PID"
echo "Worker PID: $WORKER_PID"
echo -e "${BLUE}To stop all processes:${NC}"
echo "kill $SERVER_PID $WORKER_PID"

# Wait for user input before terminating
read -p "Press Enter to terminate all processes..."
kill $SERVER_PID $WORKER_PID

# Cleanup
docker logout 