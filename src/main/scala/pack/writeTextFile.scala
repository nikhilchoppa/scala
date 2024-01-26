package pack

// SparkContext: the entry point to spark functionalities. it represents the connection to the spark cluster
import org.apache.spark.SparkContext
// SparkConf: Configuration for a spark application. used to set various spark parameters
import org.apache.spark.SparkConf
// SparkSession: introduced in spark2.0, it provides a unified entry point for dataframe and dataset API. It subsumes(absorb) SparkContext, SQLContext, and HiveContext
import org.apache.spark.sql.SparkSession

object writeTextFile {

  def main(args:Array[String]):Unit={
    println("===Started===")
    println

    // While set("spark.driver.allowMultipleContexts", "true") allows for multiple SparkContext instances, it is typically not recommended, especially in production settings.
    // Spark configuration setting to allow more than one SparkContext per JVM.
    // configaration nothing but settings
    // this config is used to run multiple spark application. but Allowing multiple contexts is generally discouraged in production environments. It can lead to unexpected behavior, resource contention, and complications in cluster resource management. Each SparkContext will try to manage cluster resources independently, which can lead to conflicts.
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf) // RDD
    sc.setLogLevel("ERROR")

    // 'SparkSession' is the main entry point to dataframe and sql functionality. it is unified interface to various spark features.
    // '.builder' is method that returns a 'builder' object. used to construct a 'sparksession'. builder pattern is used to create complex object step by step
    // '.getOrCreate()' is a method check the sparksession already exists for current application/project. if it does it returns existing seesion . if doesn't exist it will create a new spark-session. it ensures that there has to be only one sparksession per JVM, adhereing singleton pattern.
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val data = sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/usdata.csv",1)
    data.foreach(println)

    println
    println("====len data=====")
    println

    val lendata = data.filter(x => x.length>200)
    lendata.foreach(println)

    println
    println("======flat data=====")
    println

    val flat = lendata.flatMap(x=>x.split(","))
    flat.foreach(println)

    println
    println("======replace data=====")
    println

    val rep = flat.map(x=>x.replace("-",""))
    rep.foreach(println)

    println
    println("======concat data=====")
    println

    val con = rep.map(x=>x.concat(",zeyo")) // or you can use x=>x+",zeyo"
    con.foreach(println)

    // save as textfile
    con.saveAsTextFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/saveddata/procdataWithSinglePartitionV1")

    println("=======data written=======")



  }


}
