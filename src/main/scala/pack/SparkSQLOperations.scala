package pack

//import org.apache.spark._
//import org.apache.spark.sql._

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object SparkSQLOperations {

  def main(args:Array[String]):Unit={
    System.setProperty("hadoop.home.dir", "file:///Users/nikhilsaireddychoppa/Downloads/scala/hadoop")

    println("=========Started==========")

    val conf = new SparkConf()
      .setAppName("revision")
      .setMaster(("local[*]"))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/df.csv")

    val df1 = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/df1.csv")

    val cust = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/cust.csv")

    val prod = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/prod.csv")


    // scala spark sql dataframe
    df.show()
    df1.show()
    cust.show()
    prod.show()

    // create Temp views
    df.createOrReplaceTempView("df")
    df1.createOrReplaceTempView("df1")
    cust.createOrReplaceTempView("cust")
    prod.createOrReplaceTempView("prod")

    // 1st Transformation =  Select two columns
    spark.sql("select id, tdate from df").show()

    // 2nd Transformation = Select column with category filter = Exercise
    spark.sql("select id, tdate, category from df where category='Exercise' order by id").show()

    // 3rd Transformation = Multi Column filter
    spark.sql("select id, tdate, category, spendby from df where category='Exercise' and spendby='cash'").show()

    // 4th Transformation = Multi Value filter
    spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()

    // 5th Transformation = Like filter
    spark.sql("select * from df where product like('%Gymnastics%')").show()

    // 6th Transformation = Not filters
    spark.sql("select * from df where category!='Exercise' ").show()

    // 7th Transformation = Not In Filters
    spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()

    // 8th Transformation = Null Filters
    spark.sql("select * from df where product is null").show()

    // 9th Transformation = Not Null Filters
    spark.sql("select * from df where product is not null").show()

    // 10th Transformation = Max Function
    spark.sql("select max(id) from df").show()

    // 11th Transformation = Min Function
    spark.sql("select min(id) from df").show()

    // 12th Transformation = Count
    spark.sql("select count(1) from df").show()

    // 13th Transformation = Condition Statement
    spark.sql("select *, case when spendby='cash' then 1 else 0 end as status from df").show()

    // 14th Transformation = concat data
    spark.sql("select id, category, concat(id, '-',category) as condata from df").show()

    // 14th + 1 Transformation = concat data
    spark.sql("select concat(id, '-',category) as condata from df").show()

    // 15th Transformation = concat_ws data
    spark.sql("select id, category, product, concat_ws('-', id, category, product) as condata from df").show()







  }


}
