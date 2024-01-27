package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkSQLDataframeOperations {

  def main(array: Array[String]): Unit={
    // spark application is used to set property that specifies location of hadddop installation directory.
    System.setProperty("hadoop.home.dir", "file:///Users/nikhilsaireddychoppa/Downloads/scala/hadoop")

    println("========Started========")

    val conf = new SparkConf()
      .setAppName("DataFrameRevision")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // this enables implicit conversions and functions that enhance usability and functionality of spark dataframe and dataset APIs
    // mainly converting rdd to dataframe or datasets, .toDF() method on Seq and RDD (Resilient Distributed Dataset) collections, allowing them to be easily converted into DataFrames.
    // convert column names into Column objects using the $"column_name" syntax, and more. $ = col
    import spark.implicits._

    val df = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/df.csv")
    val df1 = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/df1.csv")
    val cust = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/cust.csv")
    val prod = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/prod.csv")

    df.show()
    df1.show()
    cust.show()
    prod.show()

    // 1st Transformation = Select two columns
    df.select("id", "tdate").show()

    // 2nd Transformation = select column with category filter = Exercise
    /*
    val condition = "category" == "Exercise" is a Scala expression that compares two strings and returns a boolean value,
    but it's not useful for DataFrame operations.
    df.filter($"category" === "Exercise") is used to filter rows where the 'category' column equals the string "Exercise".
     */
    df.filter($"category"==="Exercise").orderBy("id").select("id", "tdate", "category").show()

    // 3rd Transformation = Multi column filter
    df.filter($"category"==="Exercise" && $"spendby" === "cash").select("id", "tdate","category","spendby").show()

    // 4th Transformation = Multi Value Filter
    df.filter($"category".isin("Exercise","Gymnastics")).show()

    // 5th Transformation = Like filter
    df.filter($"category".like("Gym%")).show()

    // 6th Transformation = Not filter
    /*
    =!= operator is specific to Spark's DataFrame API. It is used for inequality checks between a DataFrame column and a value, or between two DataFrame columns.
    != operator, when used in the context of Spark's Column expressions, actually behaves similarly to =!= and is typically translated to the same underlying logical plan in Spark.
     */
    df.filter($"category" =!= "Exercise").show()

    // 7th Transformation = Not In filter
    df.filter(!$"category".isin("Exercise","Gymnastics")).show()

    // 8th Transformation = Null filters
    df.filter($"product".isNull).show()

    // 9th Transformation = Not Null filters
    df.filter($"product".isNotNull).show()

    // 10th Transformation = Max Function
    /*
    agg is a method in Spark's DataFrame API that is used for aggregating data. It performs aggregate functions on the DataFrame,
    such as max, min, count, sum, etc.
     */
    df.agg(max("id")).show()

    // 11th Transformation = Min Function
    df.agg(min("id")).show()

    // 12th Transformation = Count Function
    df.agg(count("*")).show()

    // 13th Transformation = Condition Statement
    /*
      withColumn is used to add a new column to a DataFrame or to replace an existing column with a new one.
      It takes two arguments: the name of the new or existing column, and the Column expression that defines the values for this column.
     */
    df.withColumn("status", when($"spendby" === "cash", 1).otherwise(0)).show()

    // 14th Transformation = concat data
    /*
    lit is used to introduce constant or literal values into DataFrame transformations in Apache Spark. It's essential for
    mixing constant values with column-based operations.
     */
    df.withColumn("condata", concat($"id", lit("-"), $"category")).show()

    // 15th Transformation = concat_ws data
    /*
    numRows = 20: This indicates the number of rows to show. You can adjust this number based on your preference or the size of your DataFrame.
    truncate = false: This ensures that the content of the columns is not truncated.
     */
    df.withColumn("condata", concat_ws("-", $"id", $"category",$"product")).select("id", "category", "product", "condata").show(numRows = 20, truncate = false)





  }


}
