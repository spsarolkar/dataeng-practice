// 🔹 12. DataFrame + Dataset: Employee Salary Bucketing
// Scenario: HR wants to bucket employees by salary range.

// Data: employeeId, name, dept, salary

// Task:

// Load as DataFrame

// Create buckets: <50k, 50k–100k, 100k+

// Count employees per dept per bucket

// 🧩 Involves: withColumn, when, groupBy, pivot-style output
package in.sunilsarolkar;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.when;


public class EmployeeSalaryBucketing {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("EmployeeSalaryBucketing").master("local[*]").getOrCreate();
        Dataset<Row> raw=spark.read().option("header", true).option("inferSchema", true).csv("employee.csv");

        Dataset<Row> with_bucket=raw.withColumn("salary_bucket", when(col("salary").lt(50000),"<50K").when(col("salary").between(50000,100000),"50k-100k").otherwise("100k+"));
        Dataset<Row> result= with_bucket.groupBy(col("dept")).pivot(col("salary_bucket")).agg(count("employeeid")).na().fill(0);

        result.show();

        spark.stop();
    }
    
}