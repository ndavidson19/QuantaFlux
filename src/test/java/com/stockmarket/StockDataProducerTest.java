package com.stockmarket;

import org.junit.Test;
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

    @Test
    public void testProduceMessage() throws Exception {
        // Run the producer
        StockDataProducer.main(new String[]{});

        // Set up a consumer to read the produced message
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-consumer-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("stock-data"));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            assertNotNull(records);
            assertEquals(1, records.count());

            for (ConsumerRecord<String, String> record : records) {
                assertEquals("{\"symbol\":\"AAPL\",\"open\":150.75,\"high\":151.25,\"low\":149.50,\"close\":150.25,\"volume\":1000000}", record.value());
            }
        }
    }
}