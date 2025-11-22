package in.sunilsarolkar;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


// 🔧 Scenario
// You have two datasets:

// sales.csv: contains saleId, productId, quantity, amount, timestamp

// products.csv: contains productId, subcategory, category

// ✅ Goal
// Join sales with products to get category and subcategory

// Aggregate total sales per category

// Optimize the join using:

// broadcast() hint (for small dimension)

// or repartition() / bucketing

// Save the output as partitioned Parquet (partitioned by category)

public class SalesByProduct {

    

    public static void main(String[] args){
        SparkSession sparkSession = SparkSession.builder().appName("SalesByProduct").master("local[*]").getOrCreate();
        // StructType structType=new StructType()
        // .add("salesId",DataTypes.StringType)
        // .add("productId",DataTypes.StringType)
        // .add("quantity",DataTypes.LongType)
        // .add("amount",DataTypes.DoubleType)
        // .add("timestamp",DataTypes.DateType);

        Dataset<Row> salesDf=sparkSession.read().option("inferSchema", true).option("header", true).csv("sales.csv");
        Dataset<Row> productsDf=sparkSession.read().option("inferSchema", true).option("header", true).csv("products.csv");
        Dataset<Row> joinedAggregated=salesDf.join(broadcast(productsDf),salesDf.col("productId").equalTo(productsDf.col("productId")),"inner").groupBy(productsDf.col("category")).agg(sum(salesDf.col("amount")).alias("totalSales"));

        joinedAggregated.write().mode("overwrite").partitionBy("category").parquet("salesPercategories/");
        
       
    }
    
}
