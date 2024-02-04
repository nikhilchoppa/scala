package pack

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
object DFJoin {
  // to manipulate and combine large datasets
  // join types: inner, other(full outer), left outer, right outer, and cross join

  def main(args: Array[String]):Unit={

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


    val df1 = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/df1.txt")
    val df2 = spark.read.option("header", "true").csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/df2.txt")
    df1.show()
    df1.printSchema()
    df2.show()
    df2.printSchema()


    // inner join
    // desc: it returns rows that have matching values in both dataframes
    // scenario: find employees who have a department assigned
    println("inner join")
    val joinDF = df1.join(df2, df1("emp_id") === df2("emp_id"))
    joinDF.show()

    // full outer join
    // desc: returns all rows from both dataframes, with matching rows from both sides
    // if there is no match, the result is 'null' on the side of the dataframe without a match
    // scenario: list all employees and all departments, showing null where there is no match
    println("full outer join")
    val fullOuterJoinDF = df1.join(df2, df1("emp_id") === df2("emp_id"), "outer")
    fullOuterJoinDF.show()

    // left outer join
    println("left outer join")
    // Desc: returns all rows from left df, and matched rows from the right df. the result is 'null' from right side if there is no match.
    // scenario: list all employees and their departments, showing null from departments if they don't belong to any
    val leftOuterJoinDF = df1.join(df2, df1("emp_id") === df2("emp_id"), "left_outer")
    leftOuterJoinDF.show()

    // in scala spark df "=== is equal to for comparison" and "=!= for not equal to for comparison"

    // right outer join
    println("right outer join")
    // Desc: returns all rows from right df and the matched rows from the left df. the result is 'null' from the left side if there is no match.
    // scenario: list all departments and their employees, showing null for employees if no one is assigned to the department
    val rightOuterJoinDF = df1.join(df2,df1("emp_id") === df2("emp_id"), "right_outer" )
    rightOuterJoinDF.show()

    // cross join
    println("cross join")
    // desc: returns the cartesian product of both df, meaning every row of 'df1' is joined to every row of 'df2'
    // scenario: pair every employee with every department, regardless of their actual department
    val crossJoinDF = df1.crossJoin(df2)
    crossJoinDF.show()

    // left anti join
    println("left anti join")
    // desc: returns rows from the left DataFrame that do not have corresponding rows in the right DataFrame. Essentially, it's the opposite of an inner join, as it selects rows that are only in the left DataFrame and not in the right DataFrame.
    // scenario: Suppose you have two DataFrames: df1 (employee data) and df2 (department data), and you want to find out which employees do not belong to any department.
    val antiJoinDF = df1.join(df2, df1("emp_id") === df2("emp_id"), "left_anti")
    antiJoinDF.show()



  }

}
