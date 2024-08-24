package com.stockmarket;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import java.util.stream.Collectors;


public class StockDataConsumer {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID_PREFIX = "stock-data-group";
    
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final int totalPartitions;

    public StockDataConsumer(String topic, int totalPartitions) {
        this(DEFAULT_BOOTSTRAP_SERVERS, topic, totalPartitions);
    }

    public StockDataConsumer(String bootstrapServers, String topic, int totalPartitions) {
        this.topic = topic;
        this.totalPartitions = totalPartitions;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", GROUP_ID_PREFIX + "-" + topic);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void subscribeToAll() {
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void subscribeToStocks(List<String> stockSymbols) {
        List<TopicPartition> partitions = stockSymbols.stream()
            .map(symbol -> new TopicPartition(topic, StockPartitioner.getPartitionForStock(symbol, totalPartitions)))
            .collect(Collectors.toList());
        this.consumer.assign(partitions);
    }

    public void assignToPartition(int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        this.consumer.assign(Collections.singletonList(topicPartition));
        this.consumer.seekToBeginning(Collections.singletonList(topicPartition));
    }

    public List<ConsumerRecord<String, String>> receiveMessages(Duration timeout) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        records.forEach(result::add);
        return result;
    }

    public void close() {
        consumer.close();
    }

    private int getPartitionForStock(String symbol) {
        return Math.abs(symbol.hashCode()) % totalPartitions;
    }
}