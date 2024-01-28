package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
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

    // 16th Transformation = Lowercase data
    /*
    .select() is only when you want to show only few columns as output.
    but if you want to show all columns you don't need to put .select(), directly .show()
     */
    df.withColumn("lower", lower($"category")).show()

    // 17th Transformation = Replace Nulls data = coalesce
    df.withColumn("replaceNulls", coalesce($"product", lit("nikhil"))).show()

    // 18th Transformation = Trim the space
    /*
    Operation: trim specifically targets and removes any spaces at the start and end of the string. It does not affect spaces in the middle of the string.
    Example:If a value in the product column is " Gymnastics Mat ", the trim function will change it to "Gymnastics Mat" (removing the leading and trailing spaces).
    */
    df.withColumn("trimmedProduct", trim($"product")).show()

    // 19th Transformation = Distinct the columns
    /*
       distinct: This method returns a new DataFrame with distinct rows, effectively removing duplicates. It ensures that each combination of category and
        spendby in the resulting DataFrame is unique.
        */
    df.select("category", "spendby").show()
    df.select("category", "spendby").distinct().show()

    // 20th Transformation = Substring with Trim
    // pos is the starting position of substring and len is the length of substring
    df.withColumn("sub", substring($"product",1,11)).show()
    df.select(substring($"product", 3, 11).as("sub2")).show()

    // 21th Transformation = Substring/Split operation
    df.select(substring_index($"category"," ", 3)).show()
    df.select(substring_index($"category", ",", 1).as("spl")).show()

    // 22th Transformation = Union all
    df.union(df1).show()

    // 23rd Transformation = Union
    df.union(df1).orderBy($"id").show()

    // 24th Transformation = Aggregate Sum
    /*
    performing a grouping and aggregation operation on the DataFrame df.
    It groups the data by the category column and then calculates the sum of the amount column for each category.
    The result is a new DataFrame with two columns: category and total, where total is the sum of amount for each category.
     */
    df.groupBy("category")
      .agg(sum("amount").as("total"))
      .show()

    // 25th Transformation = Aggregate sum with two columns
    df.groupBy("category", "spendby")
      .agg(sum("amount").as("total"))
      .show()

    // 26th Transformation = Aggregate count
    df.groupBy("category", "spendby")
      .agg(
        sum("amount").as("total"),
        count("amount").as("cnt")
      )
      .show()

    df.show()

    // 27th Transformation: Aggregate with Order Descending
    df.groupBy("category")
      .agg(max("amount").as("max"))
      .orderBy($"category".desc)
      .show()

    // 28th Transformation: Window Row Number
    /*
    Window.partitionBy("category").orderBy($"amount".desc): Defines a window specification that partitions data by category and orders it by amount in descending order within each partition.
    df.withColumn("row_number", row_number().over(windowSpec)): Adds a new column row_number to your DataFrame, where the row number is assigned based on the defined window specification.
    Here row_number() is an inbuilt method in Apache Spark's SQL functionality. It's a type of window function used to assign a unique sequential integer (starting from 1) to each row within a partition of the dataset.
     */
    val windowSpec = Window.partitionBy("category").orderBy($"amount".desc)
    df.withColumn("row_number", row_number().over(windowSpec)).show()


    // 29th Transformation: Window Dense_rank Number
    // dense_rank() will make Rows with the same values receive the same rank, and the next rank is increased by 1,
    val windowSpec1 = Window.partitionBy("category").orderBy($"amount".desc)
    df.withColumn("dense_rank", dense_rank().over(windowSpec1)).show()

    // 30th Transformation: Window rank Number
    val windowSpec2 = Window.partitionBy("category").orderBy($"amount".desc)
    df.withColumn("rank", rank().over(windowSpec2)).show()

    // 31st Transformation: Window Lead function
    df.withColumn("lead", lead($"amount", 1).over(windowSpec2)).show()

    // 32nd Transformation: Window LAd function
    df.withColumn("lag", lag($"amount", 1).over(windowSpec2)).show()

    // 33rd Transformation: Having functionality using agg and filter
    df.groupBy("category").agg(count("category").as("cnt")).filter($"cnt" > 1).show()

    // 34th Transformation Joins(Inner, Left, Right, Full, Left Anti)
    cust.join(prod, cust("id") === prod("id")).select(cust("id"), cust("name"), prod("product")).show() // Inner Join
    cust.join(prod, cust("id") === prod("id"), "left").select(cust("id"), cust("name"), prod("product")).show() // Left Join
    cust.join(prod, cust("id") === prod("id"), "right").select(cust("id"), cust("name"), prod("product")).show() // Right Join
    cust.join(prod, cust("id") === prod("id"), "full").select(cust("id"), cust("name"), prod("product")).show() // Full Join
    cust.join(prod, cust("id") === prod("id"), "leftanti").select(cust("id"), cust("name")).show() // Left Anti Join

    // 35th Transformation: date format
    df.select($"id", $"tdate", date_format(to_date($"tdate", "MM-dd-yyyy"), "yyyy-MM-dd").as("con_date")).show()

    // 36th Transformation: sub query
    /*
    Subqueries in DataFrame API are typically handled by creating temporary views or DataFrames and then performing operations on them.
     */
    val subDF = df.withColumn("con_date", date_format(to_date($"tdate", "MM-dd-yyyy"), "yyyy-MM-dd"))
    subDF.groupBy("con_date").agg(sum("amount").as("total")).show()

    /*
    Date and time formatting involves various symbols to represent different components of date and time values. Here's an overview of the most commonly used format specifiers in date and time formats, particularly relevant in the context of Apache Spark and similar systems:

    Date Components
    Year:

    yyyy or YYYY: Represents the year as a four-digit number (e.g., 2023).
    yy: Represents the last two digits of the year (e.g., 23 for 2023).
    Month:

    MM: Represents the month as a two-digit number (01-12).
    MMM: Represents the abbreviated name of the month (Jan, Feb, Mar, etc.).
    MMMM: Full month name (January, February, etc.).
    Day:

    dd: Represents the day of the month as a two-digit number (01-31).
    Time Components
    Hour:

    HH: Represents the hour in a 24-hour format (00-23).
    hh: Represents the hour in a 12-hour format (01-12).
    Minute:

    mm: Represents the minute within the hour (00-59).
    Second:

    ss: Represents the second within the minute (00-59).
    SSS: Represents milliseconds.
    AM/PM Marker
    AM/PM:
    a: Represents the AM/PM marker in a 12-hour format.
    Time Zone
    Time Zone:
    z: Time zone abbreviated name (e.g., PST, EST).
    Z: Time zone offset from UTC (e.g., +0200).
     */


  }


}
