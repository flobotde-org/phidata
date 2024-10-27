#!/bin/sh

# run_neo4j.sh
# This script starts a Neo4j Docker container for use with phidata vectordb demos.

# Set the path to the docker-compose.yml file
COMPOSE_FILE="vectordb/neo4j/docker-compose.yml"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if Docker is installed
if ! command_exists docker; then
    echo "Error: Docker is not installed. Please install Docker and try again."
    exit 1
fi

# Check if Docker Compose is installed
if ! command_exists docker-compose; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Check if Docker daemon is running
if ! docker info >/dev/null 2>&1; then
    echo "Error: Docker daemon is not running. Please start Docker and try again."
    exit 1
fi

# Check if the docker-compose.yml file exists
if [ ! -f "$COMPOSE_FILE" ]; then
    echo "Error: docker-compose.yml file not found at $COMPOSE_FILE"
    exit 1
fi

# Start the Neo4j container
echo "Starting Neo4j container..."
docker-compose -f "$COMPOSE_FILE" up -d

# Check if the container started successfully
if [ $? -eq 0 ]; then
    echo "Neo4j container started successfully."
    echo "You can access the Neo4j browser at http://localhost:7474"
    echo "Default credentials: username 'neo4j', password 'phi-neo4j'"
    echo "To stop the container, run: docker-compose -f $COMPOSE_FILE down"
else
    echo "Error: Failed to start Neo4j container. Check the Docker logs for more information."
    exit 1
fi
