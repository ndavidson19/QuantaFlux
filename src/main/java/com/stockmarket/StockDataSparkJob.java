import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class StockDataSparkJob {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("StockDataSparkJob").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-consumer-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("stock-data");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> lines = stream.map(ConsumerRecord::value);

        lines.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
                Dataset<Row> df = spark.read().json(rdd);

                df.groupBy("symbol")
                  .agg(org.apache.spark.sql.functions.avg("close").alias("average_close"))
                  .write()
                  .mode("append")
                  .csv("output/stock_data");
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}