package com.stockmarket;

import org.junit.Test;

import main.java.com.stockmarket.StockDataProducer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StockDataProducerTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "stock-data";

    @Test
    public void testProduceMessage() throws Exception {
        // Create and use the producer
        StockDataProducer producer = new StockDataProducer(BOOTSTRAP_SERVERS);
        producer.sendMessage();
        producer.close();

        // Set up a consumer to read the produced message
        Properties props = new Properties();
        // ... (previous properties setup)

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            // Consume all existing messages
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    break;
                }
            }

            // Now consume the newly produced message
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            assertNotNull(records);
            assertTrue("No records received", records.count() > 0);
            
            boolean foundExpectedMessage = false;
            for (ConsumerRecord<String, String> record : records) {
                if ("{\"symbol\":\"AAPL\",\"open\":150.75,\"high\":151.25,\"low\":149.50,\"close\":150.25,\"volume\":1000000}".equals(record.value())) {
                    foundExpectedMessage = true;
                    break;
                }
            }
            assertTrue("Expected message not found", foundExpectedMessage);
        }
    }
}