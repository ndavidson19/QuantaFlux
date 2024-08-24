package com.stockmarket;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;

import java.util.Properties;

public class StockDataStreamsJob {

    public static void main(final String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-data-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Optional configurations
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // Commit interval
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760); // Cache size

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stockData = builder.stream("processed-stock-data");

        KStream<String, String> processedData = stockData.mapValues(value -> {
            try {
                JSONObject json = new JSONObject(value);
                double price = json.getDouble("price");
                double previousClose = json.getDouble("previous_close");
                double changePercent = ((price - previousClose) / previousClose) * 100;

                json.put("calculated_change_percent", String.format("%.2f", changePercent));
                json.put("data_source", "alpha_vantage");
                System.out.println("Processed data via streams: " + json.toString());
                return json.toString();
            } catch (Exception e) {
                // Log error or handle it
                System.err.println("Error processing message: " + value + " Error: " + e.getMessage());
                return value; // Return the original value or an error message
            }
        });

        processedData.to("streamed-stock-data", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
