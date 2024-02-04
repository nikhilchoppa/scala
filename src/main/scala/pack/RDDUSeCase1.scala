package pack

/*
● Read the data file (data.csv) [just copy below data in a file]
● /*
first_name,last_name,company_name,address,city,county,state,zip,age,phone1,phone2,email,web
James,Butt,"Benton, John B Jr",6649 N Blue Gum St,New
Orleans,Orleans,LA,70116,9,504-621-8927,504-845-1427,jbutt@gmail.com,http://www.bentonjohnbjr.com
Josephine,Darakjy,"Chanay, Jeffrey A Esq",4 B Blue Ridge
Blvd,Brighton,Livingston,MI,48116,8,810-292-9388,810-374-9840,josephine_darakjy@darakjy.org,http://www.chan
ayjeffreyaesq.com
Art,Venere,"Chemel, James L Cpa",8 W Cerritos Ave
#54,Bridgeport,Gloucester,NJ,8014,7,856-636-8749,856-264-4130,art@venere.org,http://www.chemeljameslcpa.com
*/
● Filter rows greater than which length > 200
● Flatten(split) the Filtered data with comma
● Replace hyphen with Nothing (Remove the Hyphen)
● Concat string →",zeyo" at the End of each element
● Store result in a file
 */

import org.apache.spark.{SparkContext, SparkConf}

object RDDUSeCase1 {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("Spark practise").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /*
    val (Immutable Variable) and var (Mutable Variable)
    it's generally recommended to use val over var as much as possible. Immutable variables lead to safer and more predictable code, especially in concurrent and multi-threaded environments.
    Use var only when you really need a variable whose value needs to change. For example, in a loop or when keeping track of a state that changes over time.
     */
     var data = sc.textFile("file:///Users/nikhilsaireddychoppa/Downloads/scala/data/data.csv")




  }



}
