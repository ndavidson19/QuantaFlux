package com.stockmarket;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class StockDataProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "stock-data";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all"); // Ensure message durability

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String stockData = "{\"symbol\":\"AAPL\",\"open\":150.75,\"high\":151.25,\"low\":149.50,\"close\":150.25,\"volume\":1000000}";
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, stockData);

            try {
                producer.send(record).get(); // Synchronous send to ensure message is sent
                System.out.println("Message sent successfully");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}