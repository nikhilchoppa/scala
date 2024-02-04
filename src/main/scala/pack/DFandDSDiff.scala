package pack

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Define the Sale case class outside of the class for global accessibility
object DFandDSDiff {
  case class Sale(transactionID: Int, productId: Int, quantity: Int, amount: Double)

  def main(args: Array[String]): Unit = {
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
    val salesDF = data.toDF()
    val totalSalesDF = salesDF.groupBy($"productId").agg(sum("amount").alias("totalAmount"))
    val avgQuantityDF = salesDF.groupBy($"productId").agg(avg("quantity").alias("averageQuantity"))
    totalSalesDF.show()
    avgQuantityDF.show()

    // Dataset Operations
    val salesDS = data.toDS()
    val totalSalesDS = salesDS.groupByKey(_.productId).agg(sum("amount").alias("totalAmount").as[Double])
    val avgQuantityDS = salesDS.groupByKey(_.productId).agg(avg("quantity").alias("averageQuantity").as[Double])
    totalSalesDS.show()
    avgQuantityDS.show()

    // Stop the SparkSession
    spark.stop()
  }
}
