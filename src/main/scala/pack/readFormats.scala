// Read formulae
// spark  read  format() options() load()
package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object readFormats {
  def main(args:Array[String]):Unit= {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // same for reading different formats
    val parquetdf = spark.read.format("parquet").load("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/part.parquet")
    parquetdf.show()
    println()
    println()

    // but not for sql or cloud environments
//    val sqldf = spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://zdb.cqjyiv6gmvqg.ap-south-1.rds.amazonaws.com/zdb")
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("dbtable", "htab1")
//      .option("user", "root")
//      .option("password", "Aditya908")
//      .load()

    val sqldf = spark.read
      .format("jdbc")
      .option("url", "jdbc:sqlserver://nikhil009.database.windows.net:1433;database=zeyo")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", "Employees")
      .option("user", "nikhil009")
      .option("password", "Zeyobron009@")
      .load()

    sqldf.show()

    // read and load json file
//    val jsondf = spark.read.format("json").load("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/devices.json")
//    jsondf.show()
//
//    jsondf.createOrReplaceTempView("animal")
//
//    val finaldf = spark.sql("select * from animal where lat>40")
//    finaldf.show()






  }


}
