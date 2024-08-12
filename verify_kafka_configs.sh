#!/bin/bash

# Define the Docker service names
ZOOKEEPER_SERVICE="zookeeper"
KAFKA_SERVICE="kafka"
BROKER_ID=1

# Function to execute a command inside a Docker container
docker_exec() {
    local service=$1
    shift
    docker-compose exec -T "$service" "$@"
}

# Verify if Zookeeper is running
echo "Checking Zookeeper..."
docker_exec $ZOOKEEPER_SERVICE bash -c "echo ruok | nc localhost 2181"

# Verify Zookeeper nodes
echo "Verifying Zookeeper nodes..."
docker_exec $ZOOKEEPER_SERVICE /opt/kafka/bin/zookeeper-shell.sh localhost:2181 <<EOF
ls /
quit
EOF

# Verify Kafka broker configuration
echo "Verifying Kafka broker configuration..."
docker_exec $KAFKA_SERVICE bash -c "cat /opt/kafka/config/server.properties | grep -E 'broker\.id|listeners|log\.dirs|zookeeper\.connect|auto\.create\.topics\.enable'"

# Describe Kafka topics
echo "Describing Kafka topics..."
docker_exec $KAFKA_SERVICE bash -c "/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092" | while read -r topic; do
    echo "Topic: $topic"
    docker_exec $KAFKA_SERVICE bash -c "/opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic $topic"
done

# Describe Kafka broker config
echo "Describing Kafka broker configuration from Kafka itself..."
docker_exec $KAFKA_SERVICE bash -c "/opt/kafka/bin/kafka-configs.sh --describe --bootstrap-server localhost:9092 --entity-type brokers --entity-name $BROKER_ID"

# Review Kafka logs for errors or warnings
echo "Checking Kafka logs for errors or warnings..."
docker_exec $KAFKA_SERVICE bash -c "tail -n 50 /opt/kafka/logs/server.log | grep -E 'ERROR|WARN'"

echo "Kafka and Zookeeper verification completed."
