/*
Do Mapsplit
Define Schema
Import Schema
Do column filters
write into parquet
 */
package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object colFilterWriteParquet {

  // if you are using id as int then you have to mention .toInt in the schema mapping function
  case class schema(id:String, category:String, product:String)

  def main(args:Array[String]):Unit= {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // sc.textFile(...): this method is used to read a text file and create RDD with each line in the file becoming an element in the RDD
    // now data variable is RDD contains strings, each line representing a line from the input file
    println
    println("====Raw Data=====")
    println
    val data = sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/datatxns.txt")
    data.foreach(println)
    println

    println("====column filter======")
    val split = data.map(x=>x.split(","))

    // mapping the split data with the schema columns
    val schemardd = split.map(x=>schema(x(0),x(1),x(2)))

    val filrdd = schemardd.filter(x=>x.product.contains("Gymnastics"))

    filrdd.foreach(println)

    // now i have filtered rdd, i have to convert it to dataframe
    // we cannot write this RDD into Parquet
    // we need dataframe to write it as parquet
    println
    println("====converted to dataframe=====")
    println
    val df = filrdd.toDF()
    df.show()

    println
    println("====dataframe written in parquet=====")
    println
    df.write.parquet("file:///Users/nikhilsaireddychoppa/Downloads/scala/saveddata/parquetfile")


  }


}
