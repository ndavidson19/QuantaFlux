package com.stockmarket;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StockDataConsumer {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID_PREFIX = "stock-data-group";
    
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public StockDataConsumer(String topic) {
        this(DEFAULT_BOOTSTRAP_SERVERS, topic);
    }

    public StockDataConsumer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", GROUP_ID_PREFIX + "-" + topic);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void subscribe() {
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void assignToPartition(int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        this.consumer.assign(Collections.singletonList(topicPartition));
        this.consumer.seekToBeginning(Collections.singletonList(topicPartition));
    }

    public String receiveMessage() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
            return record.value();
        }
        return null;
    }

    public void close() {
        consumer.close();
    }
}