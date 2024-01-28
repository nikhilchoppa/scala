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

    // 16th Transformation = Lowercase data
    spark.sql("select category, lower(category) as lower from df").show()

    // 17th Transformation = Replace Nulls data = coalesce
    spark.sql("select product, coalesce(product, '1000') as replaceNulls from df").show()

    // 18th Transformation = Trim the space
    /*
    Operation: trim specifically targets and removes any spaces at the start and end of the string. It does not affect spaces in the middle of the string.
    Example:If a value in the product column is " Gymnastics Mat ", the trim function will change it to "Gymnastics Mat" (removing the leading and trailing spaces).
     */
    spark.sql("select trim(product) from df").show()

    // 19th Transformation = Distinct the columns
    /*
    distinct: This method returns a new DataFrame with distinct rows, effectively removing duplicates. It ensures that each combination of category and
     spendby in the resulting DataFrame is unique.
     */
    spark.sql("select distinct category, spendby from df").show()

    // 20th Transformation = Substring with Trim
    spark.sql("select product from df")

    spark.sql("select substring(product,1,10) as sub from df")

    // 21th Transformation = Substring/Split operation
    spark.sql("select substring_index(category,'',1) as spl from df").show()

    // 22th Transformation = Union all
    spark.sql("select * from df union all select * from df1").show()

    // 23rd Transformation = Union
    spark.sql("select * from df union select * from df1 order by id").show()

    // 24th Transformation = Aggregate Sum
    /*
    performing a grouping and aggregation operation on the DataFrame df.
    It groups the data by the category column and then calculates the sum of the amount column for each category.
    The result is a new DataFrame with two columns: category and total, where total is the sum of amount for each category.
     */
    spark.sql("select category,sum(amount) as total from df group by category").show()

    // 25th Transformation = Aggregate sum with two columns
    spark.sql("select category,spendby,sum(amount) as total from df group by category,spendby").show()

    // 26th Transformation = Aggregate count
    spark.sql("select category, spendby,sum(amount) as total, count(amount) as cnt from df group by category,spendby").show()

    // 27th Transformation: Aggregate with Order Descending
    spark.sql("select category, max(amount) as max from df group by category order by category desc").show()

    // 28th Transformation: Window Row Number
    /*
    The SQL query you're using involves a window function, specifically row_number(), in conjunction with partition by and order by.
     This is a powerful feature in SQL used for more advanced data processing tasks.

     */
    spark.sql("select category, amount, row_number() over(partition by category order by amount desc) as row_number from df").show()

    // 29th Transformation: Window Dense_rank Number
    /*
dense_rank() is a window function that assigns a rank to each row within a partition of a result set. The ranks are consecutive integers starting from 1. Rows with equal values for the ranking criteria receive the same rank.
     */
    spark.sql("select category, amount, dense_rank() over(partition by category order by amount desc) as dense_rank from df").show()

    // 30th Transformation: Window rank Number
    spark.sql("select category, amount, rank() over(partition by category order by amount desc) as rank from df").show()

    // 31st Transformation: Window Lead function
    /*
    Purpose: The lead function is used to access data from a subsequent row in the dataset. It's similar to lag but looks forward instead of backward.
    Syntax: lead(column, offset, defaultValue)
    column: The column from which to fetch the value.
    offset: The number of rows ahead of the current row from which to fetch the value. An offset of 1 refers to the immediate next row.
    defaultValue: An optional parameter that provides a default value if the lead operation goes beyond the end of the window. If not specified, null is returned for these cases.
     */
    spark.sql("select category, amount, lead(amount) over(partition by category order by amount desc) as lead from df").show()

    // 32nd Transformation: Window lag function
    /*
    Purpose: The lag function is used to access data from a previous row in the dataset without having to do a self-join. It returns the value from a column in a row that is a certain number of rows behind the current row within the same partition.
    Syntax: lag(column, offset, defaultValue)
    column: The column from which to fetch the value.
    offset: The number of rows behind the current row from which to fetch the value. An offset of 1 refers to the immediate previous row.
    defaultValue: An optional parameter that provides a default value if the lag operation goes beyond the beginning of the window. If not specified, null is returned for these cases.
     */
    spark.sql("select category, amount, lag(amount) over(partition by category order by amount desc) as lag from df").show()

    // 33rd Transformation: Having function
    spark.sql("select category, count(category) as cnt from df group by category having count(category)>1").show()

    // 34th Transformation: Inner Join
    spark.sql("select a.id, a.name, b.product from cust a join prod b on a.id=b.id").show()

    // 35th Transformation: right Join
    spark.sql("select a.id, a.name, b.product from cust a right join prod b on a.id=b.id").show()

    // 36th Transformation: full Join
    spark.sql("select a.id, a.name, b.product from cust a full join prod b on a.id=b.id").show()

    // 37th Transformation: left anti Join
    spark.sql("select a.id, a.name from cust a left anti join prod b on a.id=b.id").show()

    // 38th Transformation: date format
    spark.sql("select id, tdate, from_unixtime(unix_timestamp(tdate, 'mm-dd-yyyy'),'yyyy-mm-dd') as con_date from df").show()

    // 39th Transformation: sub query
    spark.sql("" +
      "select sum(amount) as total, con_date from(" +
      "select id, tdate, from_unixtime(unix_timestamp(tdate, 'MM-dd-yyyy'), 'yyyy-MM-dd') as con_date, amount, category, product, spendby from df)" +
      "group by con_date" +
      "").show()

  }


}
