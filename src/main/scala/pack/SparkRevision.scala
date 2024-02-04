package pack

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark._
object SparkRevision {
  def main(args : Array[String]): Unit= {
    /*
    "setAppName()"-> This is the name of the application that you want to run.
    "setMaster()"=> This parameter denotes the master URL to connect the spark application to.
    use of "local[*]" when running standlone mode. x should be an integer value greater than 0;
    this represents how many partitions it should create when using RDD, DataFrame, and Dataset.
    Ideally, the X value should be the number of CPU cores you have.
    */
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")

    /* Creating a [RDD] SparkContext Object */
    val sc = new SparkContext(conf)

    /* Read input file and store in variable */
    val input =  sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/st.txt")
    val count = input.flatMap(line => line.split(""))
      .map(word=>(word,1))
      .reduceByKey(_+_)
    count.saveAsTextFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/saveddata/outfile")


    /*
    SparkSession.builder() -> Return SparkSession.Builder class.This is a builder for sparkSession.
    master(), appName() and getOrCreate() are methods of SparkSession.Builder.

    "appName()" -> This is the name of the application that you want to run.
    "master()"-> This parameter denotes the masterUEL to connect the spark application to. */

    // Creating a [DataFrame] spark Session Object

    val sparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    // read input file and store in variable
    // SparkSession: Very good integration to read data from external source due to Unified formula: (spark.read.format("csv").load("path"))

    val input1 = sparkSession.read.csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/sqlfile.csv")

    // after hive and spark is integrated by adding hive-site.xml  to spark/conf
    // Read from hive
    val df = sparkSession.sql("select * from db.tablename")
    // OR
    val df1 = sparkSession.table("db.tablename")
    // To write
    df.write.format("parquet").saveAsTable("db.tablename")
    /*
    -->If we don’t provide format while writing data to Hive, data will be save as
    Parquet, (when we don’t provide any location data will be created in hive
    warehouse)
    df.write.saveAsTable("db.test")
    -->If table is already created in Hive and just need to insert data in table then just give
    format ‘hive’, which means hive will store data in same format, table is created. suppose
    table is created for ORC format then using below command data will store in ORC →
    df.write.format(“hive”).mode(“append”).saveAsTable("db.test")
     */



  }

  }
