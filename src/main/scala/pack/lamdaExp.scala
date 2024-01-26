package pack

object lamdaExp {

  def main(args:Array[String]):Unit={

    println("====Started=====")

    val a = 3

    val ls = List(1,2,3,4)

    println(ls)

    val addls = ls.map(x=>x+a)

    println("addition: " + addls)

    val mulls = ls.map(x=>x*a)

    println("multiplication: " + mulls)
// string interpolation
    mulls.foreach(x => println(s"$x ,"))



  }




}
