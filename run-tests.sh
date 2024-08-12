#!/bin/bash

# Run unit tests
mvn test

# Build the application
mvn clean package

# Build Docker image (uncomment if needed)
# docker build -t stock-etl .

# Run integration tests
mvn verify -DskipUnitTests

# Run end-to-end tests
docker-compose -f docker-compose.test.yml up --abort-on-container-exit

# Clean up
docker-compose -f docker-compose.test.yml down