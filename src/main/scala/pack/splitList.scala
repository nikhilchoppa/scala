package pack

object splitList {

  def main(args:Array[String]):Unit={

    val lis = List("A~B","C~D","E~F")
    println
    println("=========RAW LIST=========")
    println(lis)
    lis.foreach(println)


    val split = lis.flatMap(x => x.split("~"))

    println
    println("=========SPLIT LIST========")
    println
    println(split)
    split.foreach(println)


  }


}
