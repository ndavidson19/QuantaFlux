package com.stockmarket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Start the producer
        executor.submit(() -> {
            try {
                StockDataProducer producer = new StockDataProducer();
                while (true) {
                    producer.sendMessage();
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Start the Kafka Streams job
        executor.submit(() -> StockDataStreamsJob.main(new String[0]));

        // Start the Spark job
        executor.submit(() -> StockDataSparkJob.main(new String[0]));

        // Start the consumer (optional, for monitoring)
        executor.submit(() -> {
            StockDataConsumer consumer = new StockDataConsumer();
            consumer.subscribe();
            while (true) {
                String message = consumer.receiveMessage();
                if (message != null) {
                    System.out.println("Received: " + message);
                }
            }
        });

        // Keep the main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
        }
    }
}