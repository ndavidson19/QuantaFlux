package com.stockmarket;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ProcessedDataProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String RAW_DATA_TOPIC = "raw-stock-data";
    private static final String PROCESSED_DATA_TOPIC = "processed-stock-data";
    private static final String CONSUMER_GROUP_ID = "processed-data-producer-group";

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;

    public ProcessedDataProducer() {
        this.consumer = createConsumer();
        this.producer = createProducer();
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(RAW_DATA_TOPIC));
        return consumer;
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StockPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }

    public void processData() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String processedData = processRawData(record.key(), record.value());
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(PROCESSED_DATA_TOPIC, record.key(), processedData);
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error producing processed data: " + exception.getMessage());
                    } else {
                        System.out.println("Produced processed data for " + record.key() + " to partition " + metadata.partition() + " at offset " + metadata.offset());
                    }
                });
            }
        }
    }

    private String processRawData(String symbol, String rawData) {
        try {
            String[] parts = rawData.split(", ");
            JSONObject json = new JSONObject();
            json.put("symbol", symbol);
            for (String part : parts) {
                String[] keyValue = part.split(": ");
                if (keyValue.length == 2) {
                    String key = keyValue[0].toLowerCase().replace(" ", "_");
                    String value = keyValue[1];
                    try {
                        double numericValue = Double.parseDouble(value);
                        json.put(key, numericValue);
                    } catch (NumberFormatException e) {
                        json.put(key, value);
                    }
                }
            }
            json.put("processed", true);
            return json.toString();
        } catch (Exception e) {
            System.err.println("Error processing raw data: " + e.getMessage());
            return null;
        }
    }

    public void close() {
        consumer.close();
        producer.close();
    }
}