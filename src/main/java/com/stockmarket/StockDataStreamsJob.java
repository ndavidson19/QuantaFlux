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

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stockData = builder.stream("stock-data");

        KStream<String, String> processedData = stockData.mapValues(value -> {
            JSONObject json = new JSONObject(value);
            double price = json.getDouble("price");
            double previousClose = json.getDouble("previousClose");
            double changePercent = ((price - previousClose) / previousClose) * 100;
            
            json.put("calculated_change_percent", String.format("%.2f", changePercent));
            json.put("data_source", "alpha_vantage");
            return json.toString();
        });

        processedData.to("processed-stock-data", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}