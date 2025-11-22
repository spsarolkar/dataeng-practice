package in.sunilsarolkar;


import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.from_json;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

// 🔹 10. RDD Query: Top Referrers in Access Logs
// Scenario: Analyze website referrer traffic.

// Input: Web access logs (ip, referrer, timestamp, url)

// Task:

// Extract referrer field

// Count total visits per referrer

// Identify top 10 referrers sorted by traffic

// 🧩 Involves: textFile(), map, reduceByKey, sortByKey

class TopReferrerInAccessLog{
    public static void main(String[] args) throws TimeoutException,StreamingQueryException{
        SparkSession spark=SparkSession.builder()
                .appName("Top Referrer in Access Log")
                .master("local[*]")
                .getOrCreate();

        // StructType schema=new StructType()
        // .add("ip",DataTypes.StringType)
        // .add("referrer",DataTypes.StringType)
        // .add("timestamp",DataTypes.TimestampType)
        // .add("url",DataTypes.StringType);
        // // Read the access log file
        // Dataset<Row> queue=spark.readStream().format("kafka").option("","").load();

        // Dataset<Row> raw=queue.selectExpr("CAST (value as String)").select(from_json(col("value"),schema).alias("data")).select("data.*");

        // Dataset<Row> groupedByReferrerOrdered=raw.groupBy(col("referrer")).agg(count(col("ip")).alias("total_visits")).orderBy(desc("total_visits"));
        // StreamingQuery outputStream=groupedByReferrerOrdered.writeStream().format("console").outputMode("append").option("truncate", false).start();
        // outputStream.awaitTermination();
        // spark.stop();

        SparkConf conf = new SparkConf().setAppName("TopReferrers").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("access_log.txt");
        JavaRDD<String> referredRDD=lines.map(l->l.split(",")[1]);
        JavaPairRDD<String,Integer> referrersCounts=referredRDD.mapToPair(referrer->new Tuple2<>(referrer,1)).reduceByKey(Integer::sum);
        JavaPairRDD<Integer,String> sortedCounts=referrersCounts.mapToPair((t)->new Tuple2<>(t._2,t._1)).sortByKey();

        List<Tuple2<Integer,String>> top10=sortedCounts.take(10);
        System.out.println(top10);
    }


}