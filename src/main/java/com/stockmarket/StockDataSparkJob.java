package com.stockmarket;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class StockDataSparkJob {
    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        SparkSession spark = SparkSession
            .builder()
            .appName("StockDataSparkJob")
            .master("local[*]")
            .getOrCreate();

        try {
            // Read data from Kafka
            Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "processed-stock-data")
                .load();

            // Convert the value column from binary to string
            Dataset<Row> stockData = df.selectExpr("CAST(value AS STRING)");

            // Define the schema for the JSON data
            StructType schema = new StructType()
                .add("symbol", DataTypes.StringType)
                .add("open", DataTypes.DoubleType)
                .add("high", DataTypes.DoubleType)
                .add("low", DataTypes.DoubleType)
                .add("price", DataTypes.DoubleType)
                .add("volume", DataTypes.LongType)
                .add("latestTradingDay", DataTypes.StringType)
                .add("previousClose", DataTypes.DoubleType)
                .add("change", DataTypes.DoubleType)
                .add("changePercent", DataTypes.StringType)
                .add("calculated_change_percent", DataTypes.DoubleType)
                .add("data_source", DataTypes.StringType);

            // Parse the JSON data
            Dataset<Row> parsedData = stockData.select(functions.from_json(
                stockData.col("value"),
                schema
            ).alias("data")).select("data.*");

            // Perform some analysis
            Dataset<Row> analysis = parsedData.groupBy("symbol")
                .agg(
                    functions.avg("calculated_change_percent").alias("avg_change_percent"),
                    functions.max("high").alias("max_high"),
                    functions.min("low").alias("min_low"),
                    functions.avg("volume").alias("avg_volume"),
                    functions.last("price").alias("latest_price"),
                    functions.last("latestTradingDay").alias("latest_trading_day")
                );

            // Write the results to console (for demonstration)
            StreamingQuery query = analysis.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

            query.awaitTermination();

        } catch (TimeoutException e) {
            System.err.println("Timeout occurred: " + e.getMessage());
        } catch (StreamingQueryException e) {
            System.err.println("Streaming query exception: " + e.getMessage());
        } finally {
            spark.stop();
        }
    }
}