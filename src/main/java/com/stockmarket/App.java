package com.stockmarket;

import com.stockmarket.strategies.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class App {
    private static final int PRODUCER_INTERVAL_MS = 60000; // 1 minute

    public static void main(String[] args) {

    ExecutorService executor = Executors.newFixedThreadPool(8); // Increased to 5 for the new StrategyProducer

        // Start the raw data producer
        executor.submit(() -> {
            try {
                System.out.println("Starting raw data producer...");
                StockDataProducer producer = new StockDataProducer();
                while (!Thread.currentThread().isInterrupted()) {
                    producer.produceData();
                    System.out.println("Producer completed a round of messages");
                    Thread.sleep(PRODUCER_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                System.out.println("Raw data producer interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Error in raw data producer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the ProcessedDataProducer
        executor.submit(() -> {
            try {
                System.out.println("Starting processed data producer...");
                ProcessedDataProducer processedProducer = new ProcessedDataProducer();
                processedProducer.processData();
            } catch (Exception e) {
                System.err.println("Error in processed data producer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the StrategyProducer
        executor.submit(() -> {
            try {
                System.out.println("Starting strategy producer...");
                List<TradingStrategy> strategies = Arrays.asList(
                    new MovingAverageCrossoverStrategy(10, 30)
                    // Add more strategies here as needed
                );
                StrategyProducer strategyProducer = new StrategyProducer(strategies);
                strategyProducer.processData();
                System.out.println("Strategy producer completed a round of messages");
            } catch (Exception e) {
                System.err.println("Error in strategy producer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the consumer for raw data
        executor.submit(() -> {
            try {
                System.out.println("Starting raw data consumer...");
                StockDataConsumer consumer = new StockDataConsumer("raw-stock-data", 10); // Assume 10 partitions
                consumer.subscribeToAll();
                while (!Thread.currentThread().isInterrupted()) {
                    List<ConsumerRecord<String, String>> messages = consumer.receiveMessages(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : messages) {
                        System.out.println("Received raw data: " + record.key() + " - " + record.value());
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in raw data consumer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the consumer for processed data
        executor.submit(() -> {
            try {
                System.out.println("Starting processed data consumer...");
                StockDataConsumer consumer = new StockDataConsumer("processed-stock-data", 10); // Assume 10 partitions
                consumer.subscribeToAll();
                while (!Thread.currentThread().isInterrupted()) {
                    List<ConsumerRecord<String, String>> messages = consumer.receiveMessages(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : messages) {
                        System.out.println("Received processed data: " + record.key() + " - " + record.value());
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in processed data consumer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the consumer for strategy signals
        executor.submit(() -> {
            try {
                System.out.println("Starting strategy signal consumer...");
                StockDataConsumer consumer = new StockDataConsumer("MovingAverageCrossover-signals", 1); // Assume 1 partition
                consumer.subscribeToAll();
                while (!Thread.currentThread().isInterrupted()) {
                    List<ConsumerRecord<String, String>> messages = consumer.receiveMessages(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : messages) {
                        System.out.println("Received strategy signal: " + record.key() + " - " + record.value());
                        // Here you would typically act on the strategy signal
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in strategy signal consumer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start the consumer for AAPL stock
        executor.submit(() -> {
            try {
                System.out.println("Starting consumer for AAPL stock...");
                int totalPartitions = 10; // Make sure this matches your topic configuration
                StockDataConsumer consumer = new StockDataConsumer("processed-stock-data", totalPartitions);
                int aaplPartition = StockPartitioner.getPartitionForStock("AAPL", totalPartitions);
                consumer.assignToPartition(aaplPartition);
                System.out.println("AAPL consumer assigned to partition: " + aaplPartition);
                
                while (!Thread.currentThread().isInterrupted()) {
                    List<ConsumerRecord<String, String>> messages = consumer.receiveMessages(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : messages) {
                        if ("AAPL".equals(record.key())) {
                            System.out.println("Received AAPL data: " + record.key() + " - " + record.value());
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in AAPL stock consumer: " + e.getMessage());
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
                // StockDataSparkJob.main(new String[]{});
            } catch (Exception e) {
                System.err.println("Error in Spark job: " + e.getMessage());
                e.printStackTrace();
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