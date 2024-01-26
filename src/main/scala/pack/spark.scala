package pack

import org.apache.spark.{SparkConf, SparkContext}

object spark {
  def main(args:Array[String]):Unit={
    val conf = new
        SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.hostname","localhost")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data = sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/files/st.txt") // change according yourdrive
    data.foreach(println)
  }

}
