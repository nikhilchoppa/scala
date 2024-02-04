package pack
import org.apache.spark.sql.SparkSession

object coalesce {
  def main(args:Array[String]):Unit={

    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("CoalesceExample")
      .master("local[*]") // Use local master, '*' allows using all available cores
      .getOrCreate()

    // Create an RDD with a high number of partitions
    val initialRDD = spark.sparkContext.parallelize(1 to 100000, 100)

    // Filter out numbers not divisible by 10
    val filteredRDD = initialRDD.filter(_ % 10 == 0)

    // Coalesce the RDD to reduce the number of partitions
    val coalescedRDD = filteredRDD.coalesce(10)


    // Test Case 1: Number of partitions before and after coalesce
    println(s"Number of partitions before coalesce: ${filteredRDD.getNumPartitions}")
    println(s"Number of partitions after coalesce: ${coalescedRDD.getNumPartitions}")

    // Test Case 2: Take a sample of the data from coalesced RDD
    println("Sample data from coalesced RDD:")
    coalescedRDD.take(10).foreach(println)

    // Stop the SparkSession
    spark.stop()


  }

}
