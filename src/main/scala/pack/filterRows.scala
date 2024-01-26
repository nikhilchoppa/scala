package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object filterRows {
  def main(args:Array[String]):Unit= {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val data = sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/datatxns.txt")
    data.foreach(println)

    println
    println("=====Gym filter=======")
    println

    val gymdata = data.filter(x => x.contains("Gymnastics"))
    gymdata.foreach(println)


  }


  }
