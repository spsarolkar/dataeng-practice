/** / 🔹 8. Dataset Query: Monthly Sales Summary
// Scenario: You have a CSV of product sales across time.

// Schema: productId, category, quantity, price, saleDate

// Task:

// Use Java Bean + Dataset<Sale>

// Calculate total revenue per product per month

// Show top 3 best-selling products per category

// 🧩 Involves: Encoders, groupByKey, Typed aggregations, month() function
**/
package in.sunilsarolkar;
import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;

class MonthlySalesSummary {

    public static class Sale implements Serializable {
        String productId;
        String category;
        int quantity;
        double price;
        java.sql.Date saleDate; /**
        Date in your class is likely java.util.Date, which doesn’t integrate well with Spark SQL functions like month()
        saleDate should be **java.sql.Date** or **java.sql.Timestamp**
**/

    }

    public static void main(String[] args) {
        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Monthly Sales Summary")
                .master("local[*]")
                .getOrCreate();

        
        Encoder<Sale> salesEncoder = Encoders.bean(Sale.class);
        // Read the CSV file into a Dataset
        Dataset<Row> raw = spark.read().option("header", true).option("inferSchema", true).csv("sales.csv");
        Dataset<Sale> sales=raw.withColumn("saleDate", to_date(col("saleDate"), "yyyy-MM-dd")).as(salesEncoder);
        // Calculate total revenue per product per month
        Dataset<Row> monthlyRevenue = sales.withColumn("revenue", col("quantity").multiply(col("price"))).groupBy(col("productId"), month(col("saleDate")).alias("month")).agg(sum("revenue").alias("totalRevenue"));
        monthlyRevenue.show();
        // Show top 3 best-selling products per category
        
        //Dataset<Row> topSellingProducts = sales.groupBy(col("category")).agg(sum("quantity").alias("totalQuantity")).orderBy(desc("totalQuantity")).limit(3); 
        //                                                                                                                                              ^
        //                                                                                                                                              |
        //                                                                                                           This is Wrong as limit applied on overall result not categorywise

        WindowSpec categoryWindow = Window
            .partitionBy("category")
            .orderBy(col("totalQuantity").desc());

        Dataset<Row> ranked = sales
            .groupBy("category", "productId")
            .agg(sum("quantity").alias("totalQuantity"))
            .withColumn("rank", row_number().over(categoryWindow))
            .filter("rank <= 3");
        // Stop the Spark session
        spark.stop();
    }
}