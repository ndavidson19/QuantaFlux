package com.stockmarket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Start the producer
        executor.submit(() -> {
            try {
                System.out.println("Starting producer...");
                StockDataProducer producer = new StockDataProducer();
                while (!Thread.currentThread().isInterrupted()) {
                    producer.sendMessage();
                    System.out.println("Producer sent a message");
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                System.out.println("Producer interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Error in producer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the Kafka Streams job
        executor.submit(() -> {
            try {
                System.out.println("Starting Kafka Streams job...");
                StockDataStreamsJob.main(new String[0]);
            } catch (Exception e) {
                System.err.println("Error in Kafka Streams job: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the Spark job
        executor.submit(() -> {
            try {
                System.out.println("Starting Spark job...");
                StockDataSparkJob.main(new String[0]);
            } catch (Exception e) {
                System.err.println("Error in Spark job: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the consumer
        executor.submit(() -> {
            try {
                System.out.println("Starting consumer...");
                StockDataConsumer consumer = new StockDataConsumer();
                consumer.subscribe();
                while (!Thread.currentThread().isInterrupted()) {
                    String message = consumer.receiveMessage();
                    if (message != null) {
                        System.out.println("Received: " + message);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in consumer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Keep the main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Main thread interrupted");
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.err.println("Executor did not terminate in the specified time.");
                }
            } catch (InterruptedException e) {
                System.err.println("Executor shutdown interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }
}