/**
// 🔹 7. Streaming Query: Login Spike Detection
// Scenario: Detect user login spikes in a short period.

// Task:

// Ingest login events from Kafka: userId, loginTime

// Apply a 5-minute watermark

// Raise an alert if a user logs in more than 5 times in 2 minutes

 */

package in.sunilsarolkar;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

class LoginSpikeDetection{
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("Login Spike Detection")
                .master("local[*]")
                .getOrCreate();

        // Read from Kafka
        Dataset<Row> loginEvents = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "login-events")
                .load();

        // Parse the login events
        Dataset<Row> parsedEvents = loginEvents.selectExpr("CAST(value AS STRING)")
                .selectExpr("split(value, ',')[0] AS userId", "CAST (split(value, ',')[1] as TIMESTAMP) AS loginTime");

        // Apply watermark and group by userId
        Dataset<Row> spikes = parsedEvents
                .withWatermark("loginTime", "5 minutes")
                .groupBy(col("userId"),window(col("loginTime"), "2 minute","1 minutes")) // userId
                .count()
                .filter("count > 5");

        // Start the streaming query
        StreamingQuery query = spikes.writeStream()
                .outputMode("update")
                .format("console")
                .trigger(Trigger.ProcessingTime("2 minutes"))
                .start();

        query.awaitTermination();
    }
}