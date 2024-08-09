import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class StockDataProducerTest {
    @Test
    public void testProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String stockData = "{\"symbol\":\"AAPL\",\"open\":150.75,\"high\":151.25,\"low\":149.50,\"close\":150.25,\"volume\":1000000}";
            ProducerRecord<String, String> record = new ProducerRecord<>("stock-data", stockData);

            assertDoesNotThrow(() -> producer.send(record).get());
        }
    }
}