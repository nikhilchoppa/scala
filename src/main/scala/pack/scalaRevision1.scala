package pack

object scalaRevision1 {

  /*
  Write a Scala program to compute the sum of the two given integer
  values. If the two values are the same, then return triples of their sum.
   */
  def main(args:Array[String]):Unit={
    println(sum(1,2))
    println(sum(2,2))
    println(sum(3,2))
    println(sum(4,4))


  }

  def sum(i:Int, i1:Int):Int={
    if(i == i1) (i+i1)*3 else (i+i1)
  }

}
