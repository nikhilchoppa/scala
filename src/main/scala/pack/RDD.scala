package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDD {
  case class schema(firstcol:String, secondcol:String, thirdcol:String, fourthcol:String, fifthcol:String, sixthcol:String)

  def main(args:Array[String]):Unit={
    println("========started========")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")

    val sc = new SparkContext(conf)
    // The setLogLevel method is used to set the minimum level of log messages that will be printed to the console.
    // so now after this line it will show only error related issues in the console. in spark, log levels include INFO, WARN, ERROR, and DEBUG.
    sc.setLogLevel("ERROR")

    // 'SparkSession' is the main entry point to dataframe and sql functionality. it is unified interface to various spark features.
    // '.builder' is method that returns a 'builder' object. used to construct a 'sparksession'. builder pattern is used to create complex object step by step
    // '.getOrCreate()' is a method check the sparksession already exists for current application/project. if it does it returns existing seesion . if doesn't exist it will create a new spark-session. it ensures that there has to be only one sparksession per JVM, adhereing singleton pattern.
    val spark = SparkSession.builder.getOrCreate()
    // it allows you to convert a case class in Scala to a DataFrame, which can then be queried using Spark SQL.
    // it enriches the language features available when working with Spark DataFrames and Datasets in Scala, making it more concise and functional, especially for operations like column selection, data conversion, and type-safe transformations.
    import spark.implicits._

    // Step 1: Read Data
    // here '6' is the number of partitions when reading the data into an RDD
    // if you are not giving the partitions spark by default do partitions as the limit is 128mb per partitions
    val data = sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/dt.txt", 6)
    data.foreach(println)
    println("end")
    println(data)

    // Step 2: Map Split
    val mapsplit = data.map(x => x.split(","))
    // When you print an array directly in Scala, it doesn't output the contents of the array, but rather the reference to the array in memory.
    println(mapsplit)
    // mkString will create a string from the array x with each element
    // this will give you more readable
    mapsplit.foreach(x => println(x.mkString(" ")))


    // Step 4: Define Schema RDD
    // here map operation will check id an index exists in the array before trying to access it, and if doesn't exists, it will use an empty string as a default
    //val schemardd = mapsplit.map(x => schema(x(0), x(1), x(2), x(3), x(4), x(5)))
    val schemardd = mapsplit.map { x =>
      val firstcol = if (x.isDefinedAt(0)) x(0) else ""
      val secondcol = if (x.isDefinedAt(1)) x(1) else ""
      val thirdcol = if (x.isDefinedAt(2)) x(2) else ""
      val fourthcol = if (x.isDefinedAt(3)) x(3) else ""
      val fifthcol = if (x.isDefinedAt(4)) x(4) else ""
      val sixthcol = if (x.isDefinedAt(5)) x(5) else ""
      schema(firstcol, secondcol, thirdcol, fourthcol, fifthcol, sixthcol)
    }
    // Step 5: Filter Data
    val filterdata = schemardd.filter(x => x.fourthcol.contains("Gymnastics"))
    filterdata.foreach(println)

    // filtered RDD is converted to a DataFrame, which is distributed collection
    // data organized into named columns
    val df = filterdata.toDF()
    df.show()
  }
}