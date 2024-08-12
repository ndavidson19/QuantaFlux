package com.stockmarket;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.junit.Test;

import main.java.com.stockmarket.StockDataProducer;
import main.java.com.stockmarket.StockDataStreamsJob;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.time.Duration;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StockDataIntegrationTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Test
    public void testStockDataStreamsJob() throws InterruptedException {
        // Start the Streams job
        Thread streamsThread = new Thread(() -> StockDataStreamsJob.main(new String[]{BOOTSTRAP_SERVERS}));
        streamsThread.start();

        // Produce test data
        StockDataProducer producer = new StockDataProducer(BOOTSTRAP_SERVERS);
        producer.sendMessage();

        // Consume processed data
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("processed-stock-data"));

            ConsumerRecords<String, String> records = null;
            int attempts = 0;
            while (attempts < 5 && (records == null || records.isEmpty())) {
                records = consumer.poll(Duration.ofSeconds(2));
                attempts++;
            }

            assertNotNull(records);
            assertTrue("No records received after " + attempts + " attempts", records.count() > 0);

            for (ConsumerRecord<String, String> record : records) {
                JSONObject json = new JSONObject(record.value());
                assertTrue(json.has("percent_change"));
            }
        }


        producer.close();
        streamsThread.interrupt();
    }
}