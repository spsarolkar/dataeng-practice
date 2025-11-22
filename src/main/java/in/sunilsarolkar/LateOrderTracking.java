package in.sunilsarolkar;

// 🔹 9. Streaming + Window Join: Late-Order Tracking
// Scenario: Match late product deliveries with order time.

// Data:

// Kafka Topic A: orders — orderId, userId, orderTime

// Kafka Topic B: deliveries — orderId, deliveryTime

// Task:

// Join orders and deliveries streams with watermark

// Find orders delivered more than 3 days late

// Output to console or file

// 🧩 Involves: joinWith, event-time joins, datediff(), watermark on both sides




import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;


import org.apache.spark.sql.types.StructType;

import scala.Symbol;

class LateOrderTracking {
    public static void main(String[] args) throws Exception {

        StructType ordersSchema=new StructType().add("orderId", DataTypes.StringType)
                .add("userId", DataTypes.StringType)
                .add("ordertime", DataTypes.TimestampType);

        StructType deliverySchema=new StructType().add("orderId", DataTypes.StringType)
                .add("deliveryTime", DataTypes.TimestampType);
        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Late Order Tracking")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> orderRawStream = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "orders")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> deliveryRawStream = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "delivery")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> orders = orderRawStream.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),ordersSchema).as("data")).select("data.*");
        Dataset<Row> deliveries = deliveryRawStream.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),deliverySchema).as("data")).select("data.*");
        orders.join(deliveries, orders.col("orderId").equalTo(deliveries.col("orderId")),"left").
                withWatermark("ordertime", "3 days").withWatermark("deliveryTime", "3 days").
                filter(datediff(col("deliveryTime"), col("ordertime")).gt(3)).
                select(orders.col("orderId"), orders.col("userId"), orders.col("ordertime"), deliveries.col("deliveryTime")).
                writeStream().outputMode("append").format("console").start().awaitTermination();
        // Stop the Spark session
        spark.stop();
    }
}