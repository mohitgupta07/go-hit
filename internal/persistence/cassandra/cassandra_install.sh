#!/bin/bash

# Define variables
CASSANDRA_CONTAINER_NAME="cassandra_local"
CASSANDRA_PORT=9042
CASSANDRA_VERSION="latest"

# Function to check if Docker is running
is_docker_running() {
    docker info >/dev/null 2>&1
}

# Function to check if a container is running
is_container_running() {
    docker ps -q -f name=$1
}

# Start Docker if it's not running
if ! is_docker_running; then
    echo "Starting Docker..."
    open --background -a Docker
    # Wait until Docker daemon is running
    while ! is_docker_running; do
        echo "Waiting for Docker to start..."
        sleep 2
    done
    echo "Docker started."
fi

# Pull the latest Cassandra image
echo "Pulling the latest Cassandra image..."
docker pull cassandra:$CASSANDRA_VERSION

# Stop and remove any existing Cassandra container
if [ "$(is_container_running $CASSANDRA_CONTAINER_NAME)" ]; then
    echo "Stopping and removing existing Cassandra container..."
    docker stop $CASSANDRA_CONTAINER_NAME
    docker rm $CASSANDRA_CONTAINER_NAME
fi

# Run the Cassandra container
echo "Running the Cassandra container..."
docker run -d --name $CASSANDRA_CONTAINER_NAME \
    -p $CASSANDRA_PORT:9042 \
    cassandra:$CASSANDRA_VERSION

# Wait for Cassandra to start
echo "Waiting for Cassandra to start..."
until docker exec $CASSANDRA_CONTAINER_NAME cqlsh -e "describe keyspaces"; do
    sleep 1
done

echo "Cassandra is up and running on port $CASSANDRA_PORT."
echo "You can connect to it using cqlsh or your application."

# Display container logs (optional)
# docker logs -f $CASSANDRA_CONTAINER_NAME
