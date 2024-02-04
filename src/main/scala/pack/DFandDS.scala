package pack

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, sum}
import org.apache.spark.{SparkConf,SparkContext}

class DFandDS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("SalesAnalysis")
      .master("local") // Use local master for running Spark locally
      .getOrCreate()

    // Import Spark implicits for automatic conversions like converting Seq to DataFrame/Dataset
    import spark.implicits._

    // Sample data - a sequence of Sale instances
    val data = Seq(
      Sale(1, 101, 2, 40.0),
      Sale(2, 102, 1, 20.0),
      Sale(3, 101, 1, 20.0),
      Sale(4, 103, 3, 60.0),
      Sale(5, 102, 2, 40.0)
    )

    // DataFrame Operations
    // Convert the sequence to a DataFrame
    val salesDF = data.toDF()

    // Calculate total amount of sales for each product using DataFrame API
    val totalSalesDF = salesDF.groupBy($"productId")
      .agg(sum("amount").alias("totalAmount"))

    // Calculate average quantity sold for each product using DataFrame API
    val avgQuantityDF = salesDF.groupBy($"productId")
      .agg(avg("quantity").alias("averageQuantity"))

    // Show the results
    totalSalesDF.show()
    avgQuantityDF.show()

    // Dataset Operations
    // Convert the sequence to a Dataset
    val salesDS = data.toDS()

    // Calculate total amount of sales for each product using Dataset API
    val totalSalesDS = salesDS.groupByKey(_.productId)
      .agg(sum("amount").alias("totalAmount").as[Double])

    // Calculate average quantity sold for each product using Dataset API
    val avgQuantityDS = salesDS.groupByKey(_.productId)
      .agg(avg("quantity").alias("averageQuantity").as[Double])

    // Show the results
    totalSalesDS.show()
    avgQuantityDS.show()
  }

}
