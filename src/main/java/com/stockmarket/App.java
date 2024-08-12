package com.stockmarket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {
    private static final int PRODUCER_INTERVAL_MS = 60000; // 1 minute

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Start the producer
        executor.submit(() -> {
            try {
                System.out.println("Starting producer...");
                StockDataProducer producer = new StockDataProducer();
                
                while (!Thread.currentThread().isInterrupted()) {
                    // Use default source (Yahoo Finance)
                    producer.sendMessage();
                    
                    // Or specify a source
                    // producer.sendMessage("alphavantage");
                    
                    // Or change the default source
                    // producer.setDefaultSource("alphavantage");
                    // producer.sendMessage();
                    
                    System.out.println("Producer completed a round of messages");
                    Thread.sleep(PRODUCER_INTERVAL_MS);  // Wait for 1 minute before next round
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
                StockDataStreamsJob.main(new String[]{});
            } catch (Exception e) {
                System.err.println("Error in Kafka Streams job: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the Spark job
        executor.submit(() -> {
            try {
                System.out.println("Starting Spark job...");
                StockDataSparkJob.main(new String[]{});
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
                    try {
                        String message = consumer.receiveMessage();
                        if (message != null) {
                            System.out.println("Received: " + message);
                        }
                    } catch (Exception e) {
                        System.err.println("Error in consumer while receiving message: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in consumer setup: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("Consumer shutting down");
            }
        });

        // Keep the main thread alive and handle shutdown
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Main thread interrupted, initiating shutdown");
        } finally {
            shutdownExecutor(executor);
        }
    }

    private static void shutdownExecutor(ExecutorService executor) {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("Executor did not terminate in the specified time.");
                System.err.println("List of runnable tasks: " + executor.shutdownNow());
            }
        } catch (InterruptedException e) {
            System.err.println("Executor shutdown interrupted");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}