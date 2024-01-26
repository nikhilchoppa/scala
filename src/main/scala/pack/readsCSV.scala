package pack

// '_' means import all classes in that spark package
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object readsCSV {
  def main(args:Array[String]):Unit={
    // SparkConf object is created with the application name "first"
    // and master URL "local[*]", which runs spark locally with as many worker threads as logical cores on your machine
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    // 'SparkContext' (sc) is initialized with configuration, and its
    // log level is set to "ERROR" to minimize log output.
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // a SparkSession named spark is created, which is entry point for
    // working with structured data (like dataframes) in spark.
    val spark = SparkSession.builder().getOrCreate()

    // a dataframe df is created to read a csv file located at path.
    // the option 'header' is set to 'true', indicating that the first line
    // of the file is header
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/dt.txt")
    df.show()
    // 'procdf' is created with transformations
    // 'expr' is expression
    // withColumn creates a new column 'tdate' using "split" is derived by splitting the existing 'tdate' column on '-'
    // and extracting third part with index [2] as tdate format is DD-MM-YYYY
    // then 'withColumnRenamed' change the column name 'tdate' to 'year'
    // a new column 'status' is created using "case" expression: if spendby is cash, credit, null
    // here the procdf is transformed
    val procdf = df.withColumn("tdate",expr("split(tdate, '-')[2]"))
      .withColumnRenamed("tdate", "year")
      .withColumn("status", expr("case when spendby='cash' then 'nikhil' when spendby='credit' then 'nikhil loan' when spendby is null or spendby='null' or spendby='NULL' then 'NA' end"))
    procdf.show()
  }


}
