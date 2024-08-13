package com.stockmarket.strategies;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.*;

public class StrategyProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String PROCESSED_DATA_TOPIC = "processed-stock-data";
    private static final String CONSUMER_GROUP_ID = "strategy-producer-group";

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final List<TradingStrategy> strategies;

    public StrategyProducer(List<TradingStrategy> strategies) {
        this.consumer = createConsumer();
        this.producer = createProducer();
        this.strategies = strategies;
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(PROCESSED_DATA_TOPIC));
        return consumer;
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public void processData() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    String symbol = record.key();
                    JSONObject data = new JSONObject(record.value());

                    for (TradingStrategy strategy : strategies) {
                        try {
                            strategy.updateState(symbol, data);
                            JSONObject signal = strategy.evaluate(symbol, data);
                            if (signal != null) {
                                produceSignal(strategy.getName(), symbol, signal);
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing strategy " + strategy.getName() + " for symbol " + symbol + ": " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error processing record: " + e.getMessage());
                }
            }
        }
    }

    private void produceSignal(String strategyName, String symbol, JSONObject signal) {
        String topic = strategyName + "-signals";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, symbol, signal.toString());
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error producing signal for " + symbol + " using " + strategyName + ": " + exception.getMessage());
            } else {
                System.out.println("Produced signal for " + symbol + " using " + strategyName + ": " + signal);
            }
        });
    }

    public void close() {
        consumer.close();
        producer.close();
    }
}