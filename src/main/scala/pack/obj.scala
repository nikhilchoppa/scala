package pack

import org.apache.spark._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object obj {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // text file method -- RDD
    val data = sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/files/d.txt")
    data.foreach(println)

    // csv method -- Dataframes
    val spark = SparkSession.builder.getOrCreate()

    val df = spark.read.csv("file:///Users/nikhilsaireddychoppa/Downloads/scala/files/d.txt")

    df.show()

  }


}