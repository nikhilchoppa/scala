// program begins with declaration of package
package pack

// in scala object is a singleton instance of a class
object basicOne {
  // main method is the entry point of a scala program
  // it takes an array of strings as arguments returns Unit (similar to void)
  def main(args:Array[String]):Unit={
    // program logic started
    // list creation and functional transformations
    println("Started")

    val listin = List(1,2,3,4)
    println(listin)

    val proclis = listin.map(x=>x+1)
    println(proclis)
  }

}
