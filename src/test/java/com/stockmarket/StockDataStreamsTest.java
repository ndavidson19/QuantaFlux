package com.stockmarket;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StockDataStreamsTest {

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    @Test
    public void testStockDataStreamsJob() throws InterruptedException {
        String bootstrapServers = kafka.getBootstrapServers();

        // Start the Streams job
        Thread streamsThread = new Thread(() -> StockDataStreamsJob.main(new String[]{bootstrapServers}));
        streamsThread.start();

        // Produce test data
        StockDataProducer producer = new StockDataProducer(bootstrapServers);
        producer.sendMessage();

        // Consume processed data
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "test-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("processed-stock-data"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        
        assertNotNull(records);
        assertTrue(records.count() > 0);

        for (ConsumerRecord<String, String> record : records) {
            JSONObject json = new JSONObject(record.value());
            assertTrue(json.has("percent_change"));
        }

        consumer.close();
        producer.close();
        streamsThread.interrupt();
    }
}