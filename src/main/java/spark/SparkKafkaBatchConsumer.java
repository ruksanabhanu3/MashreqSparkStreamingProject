package spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.*;

public class SparkKafkaBatchConsumer {
    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "C:\\Winutils");

        // Spark session
        SparkSession spark = SparkSession.builder()
                .appName("KafkaStreamToDataset")
                .master("local[*]")
                .config("spark.sql.streaming.checkpointLocation", "E:\\RUKSANA\\MashreqTest\\checkpoint")
                .getOrCreate();

        // Reading from source Kafka topic -my-test-topic
        Dataset<Row> kafkaStreamDataset = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "my-test-topic")
                .option("startingOffsets", "latest")
                .load();

        // Convert Kafka value to string and split by comma to parse CSV data
        Dataset<Row> parsedStream = kafkaStreamDataset
                .selectExpr("CAST(value AS STRING) AS csvData")
                .selectExpr("split(csvData, ',') AS csvFields");

        //Transformations to align w.r.t data types
        Dataset<Row> transformedStream = parsedStream
                .select(
                        expr("csvFields[0]").as("step"),
                        expr("csvFields[1]").as("type"),
                        expr("CAST(csvFields[2] AS double)").as("amount"),
                        expr("csvFields[3]").as("nameOrig"),
                        expr("CAST(csvFields[4] AS double)").as("oldbalanceOrg"),
                        expr("CAST(csvFields[5] AS double)").as("newbalanceOrig"),
                        expr("csvFields[6]").as("nameDest"),
                        expr("CAST(csvFields[7] AS double)").as("oldbalanceDest"),
                        expr("CAST(csvFields[8] AS double)").as("newbalanceDest"),
                        expr("CAST(csvFields[9] AS int)").as("isFraud"),
                        expr("CAST(csvFields[10] AS int)").as("isFlaggedFraud"),
                        lit("USD").as("currency") // Static value as a new column
                );

        //Creating a new column "value" using existing cols with conctae operations.concat_Ws
        Dataset<Row> finalDataset = transformedStream
                .withColumn("value",
                        concat_ws("|",
                                col("step"), col("type"), col("amount"), col("nameOrig"),
                                col("oldbalanceOrg"), col("newbalanceOrig"), col("nameDest"),
                                col("oldbalanceDest"), col("newbalanceDest"), col("isFraud"),
                                col("isFlaggedFraud"), col("currency"))
                );

        //querying  value column  data to print messages of each batch
        StreamingQuery query = finalDataset
                .selectExpr("CAST(value AS STRING) AS message")
                .as(Encoders.STRING())
                .groupBy()
                .count()
                .writeStream()
                .foreach(new ForeachWriter<Row>() {
                    @Override
                    public boolean open(long partitionId, long version) {
                        return true; // Open connection or initialize resources
                    }

                    @Override
                    public void process(Row value) {
                        long count = value.getLong(0);
                        System.out.println("Number of messages in this batch: " + count);
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        // Close connection or release resources
                    }
                })
                .outputMode("complete")
                .start();

        // Publishing to sink-topic
        StreamingQuery kafkaSinkQuery = finalDataset
                .selectExpr("CAST(value AS STRING)")
                .writeStream()
                .outputMode("append")
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("group.id","kafka-consumer-group")
                .option("topic", "sink-topic")
                .start();

        try {
            query.awaitTermination();
            kafkaSinkQuery.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
