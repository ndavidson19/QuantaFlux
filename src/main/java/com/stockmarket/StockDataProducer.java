package com.stockmarket;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class StockDataProducer {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "stock-data";
    private static final String[] SYMBOLS = {"AAPL", "GOOGL", "MSFT", "AMZN", "META"};

    private final KafkaProducer<String, String> producer;
    private final Map<String, StockDataSource> dataSources;
    private StockDataSource defaultSource;

    public StockDataProducer() {
        this(DEFAULT_BOOTSTRAP_SERVERS);
    }

    public StockDataProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        this.producer = new KafkaProducer<>(props);
        this.dataSources = new HashMap<>();
        
        // Initialize data sources
        YahooFinanceSource yahooSource = new YahooFinanceSource();
        AlphaVantageSource alphaVantageSource = new AlphaVantageSource();
        
        this.dataSources.put("yahoo", yahooSource);
        this.dataSources.put("alphavantage", alphaVantageSource);
        
        // Set Yahoo Finance as the default source
        this.defaultSource = yahooSource;

        System.out.println("StockDataProducer initialized with bootstrap servers: " + bootstrapServers);
        System.out.println("Default source set to: " + this.defaultSource.getSourceName());

    }

    public void sendMessage() {
        sendMessage(null);  // Use default source
    }

    public void sendMessage(String sourceName) {
        try {
            System.out.println("Attempting to connect to Kafka and retrieve partitions for topic: " + TOPIC);
            producer.partitionsFor(TOPIC);
            System.out.println("Successfully connected to Kafka and retrieved partitions for topic: " + TOPIC);
        } catch (Exception e) {
            System.err.println("Failed to connect to Kafka: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        StockDataSource source = (sourceName != null) ? dataSources.get(sourceName) : defaultSource;
        
        System.out.println("source: " + source.getSourceName());

        if (source == null) {
            System.err.println("Invalid source specified. Using default source.");
            source = defaultSource;
        }

        System.out.printf("Fetching data from %s\n", source.getSourceName());

        for (String symbol : SYMBOLS) {
            System.out.println("Fetching data for " + symbol + " from " + source.getSourceName());
            String stockData = source.fetchStockData(symbol);
            System.out.println("Fetched data for " + symbol + ": " + stockData);
            if (stockData != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, symbol, stockData);
                System.out.println("Created ProducerRecord for " + symbol);
                System.out.println("Sending message for " + symbol + " to Kafka topic " + TOPIC);
                try {
                    Future<RecordMetadata> future = producer.send(record);
                    System.out.println("Message sent to Kafka, waiting for acknowledgement...");
                    RecordMetadata metadata = future.get(30, TimeUnit.SECONDS);
                    System.out.printf("Sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                            record.key(), record.value(), metadata.partition(), metadata.offset());
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    System.err.println("Failed to send message for " + symbol + ": " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.err.println("Failed to fetch data for " + symbol + " from " + source.getSourceName() + ". Skipping this symbol.");
            }
        }
        producer.flush(); // Add this line to ensure all messages are sent before moving on
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