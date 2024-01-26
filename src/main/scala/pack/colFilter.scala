package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object colFilter {

  case class columns(id:String, category:String, product:String)

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    // sc.textFile(...): this method is used to read a text file and create RDD with each line in the file becoming an element in the RDD
    // now data variable is RDD contains strings, each line representing a line from the input file
    val data = sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/datatxns.txt")
    data.foreach(println)
    println
    // 'data.map(....)': this is transformation which applies a function to each element of RDD data
    // 'x.split(",")': this is a function splits each string 'x'(a line from the file) into an array of strings, using the comma ',' as the delimiter
    // val 'split': the resulting rdd contains array of strings. each array represents one line from the file, split into its comma-separated components.
    // it splits each element x (which is expected to be a string) into an array of strings based on the comma , delimiter.
    // if a line in your file is "apple,banana,orange", applying x.split(",") to this line will result in an array ["apple", "banana", "orange"].
    val split = data.map(x=>x.split(","))
    // The split method in Scala splits a string into an array of substrings based on the specified delimiter (in your case, a comma ,)
    // This println line doesn't print the contents of the split RDD. Instead, it prints the reference to the array in memory.
    println(split)
    println

    // The result of variable split is just an array of strings (Array[String]), but there is no built-in formatting for how this array is displayed or printed. It doesn't automatically include square brackets or commas between elements when printed.
    // printing each array of the split RDD
    // mkString("[", ", ", "]"):
    //The first argument "[" is the starting string to be placed before the first element of the array.
    //The second argument ", " is the separator placed between elements of the array.
    //The third argument "]" is the ending string to be placed after the last element of the array.
    split.foreach(array => println(array.mkString("[" , ", " , "]")))
    println

    // assigning column names to the split data
    val schemardd = split.map(x=>columns(x(0), x(1),x(2)))

    // filter the records which has "Gymnastics" in product column
    val filterrdd = schemardd.filter(x=>x.product.contains("Gymnastics"))

    println
    filterrdd.foreach(println)

  }


}
