package com.stockmarket;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


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
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");

        this.producer = new KafkaProducer<>(props);
        this.dataSources = new HashMap<>();
        
        // Initialize data sources
        YahooFinanceSource yahooSource = new YahooFinanceSource();
        AlphaVantageSource alphaVantageSource = new AlphaVantageSource();
        
        this.dataSources.put("yahoo", yahooSource);
        this.dataSources.put("alphavantage", alphaVantageSource);
        
        // Set Yahoo Finance as the default source
        this.defaultSource = yahooSource;
    }

    public void sendMessage() {
        sendMessage(null);  // Use default source
    }

    public void sendMessage(String sourceName) {
        StockDataSource source = (sourceName != null) ? dataSources.get(sourceName) : defaultSource;
        
        if (source == null) {
            System.err.println("Invalid source specified. Using default source.");
            source = defaultSource;
        }

        for (String symbol : SYMBOLS) {
            String stockData = source.fetchStockData(symbol);
            System.out.println("Fetched data for " + symbol + " from " + source.getSourceName());
            if (stockData != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, symbol, stockData);
                System.out.println("Sending message for " + symbol + " to Kafka topic " + TOPIC);
                try {
                    System.out.println("Record Key: " + symbol);
                    System.out.println("Record Value: " + stockData);
                    producer.send(record).get();
                    System.out.println("Message sent successfully for " + symbol + " from " + source.getSourceName());
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("Failed to send message for " + symbol + ": " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.err.println("Failed to fetch data for " + symbol + " from " + source.getSourceName() + ". Skipping this symbol.");
            }
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