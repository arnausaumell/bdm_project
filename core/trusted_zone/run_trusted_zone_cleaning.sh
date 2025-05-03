#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
BLUE='\033[0;34m'

# Install dependencies
echo -e "${BLUE}Installing required dependencies...${NC}"
pip install -r requirements/trusted_zone.txt
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
echo "   - All dependencies from requirements/trusted_zone.txt"
echo "   - Apache Spark data cleaning"

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
#