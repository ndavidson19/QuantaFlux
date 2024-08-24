package com.stockmarket;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StockDataProducer {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String RAW_DATA_TOPIC = "raw-stock-data";
    private static final List<String> DEFAULT_SYMBOLS = Arrays.asList("AAPL", "GOOGL", "MSFT", "AMZN", "META");

    private final KafkaProducer<String, String> producer;
    private final Map<String, StockDataSource> dataSources;
    private StockDataSource defaultSource;
    private final List<String> symbols;

    public StockDataProducer() {
        this(DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_SYMBOLS);
    }

    public StockDataProducer(String bootstrapServers, List<String> symbols) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StockPartitioner.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        this.producer = new KafkaProducer<>(props);
        this.dataSources = new HashMap<>();
        this.symbols = symbols;
        
        // Initialize data sources
        this.dataSources.put("yahoo", new YahooFinanceSource());
        this.dataSources.put("alphavantage", new AlphaVantageSource());
        
        // Set Yahoo Finance as the default source
        this.defaultSource = dataSources.get("yahoo");

        System.out.println("StockDataProducer initialized with bootstrap servers: " + bootstrapServers);
        System.out.println("Default source set to: " + this.defaultSource.getSourceName());
    }

    public void produceData() {
        produceData(null);  // Use default source
    }

    public void produceData(String sourceName) {
        StockDataSource source = (sourceName != null) ? dataSources.get(sourceName) : defaultSource;
        
        if (source == null) {
            System.err.println("Invalid source specified. Using default source.");
            source = defaultSource;
        }

        System.out.printf("Fetching data from %s\n", source.getSourceName());

        for (String symbol : symbols) {
            String stockData = source.fetchStockData(symbol);
            if (stockData != null) {
                sendToKafka(RAW_DATA_TOPIC, symbol, stockData);
            } else {
                System.err.println("Failed to fetch data for " + symbol + " from " + source.getSourceName() + ". Skipping this symbol.");
            }
        }
        producer.flush();
    }

    private void sendToKafka(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(30, TimeUnit.SECONDS);
            System.out.printf("Sent record(key=%s) to topic=%s, partition=%d, offset=%d\n",
                    key, topic, metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("Failed to send message for " + key + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void setDefaultSource(String sourceName) {
        StockDataSource newDefaultSource = dataSources.get(sourceName);
        if (newDefaultSource != null) {
            this.defaultSource = newDefaultSource;
            System.out.println("Default source set to: " + sourceName);
        } else {
            System.err.println("Invalid source name. Default source not changed.");
        }
    }

    public void close() {
        producer.close();
    }
}