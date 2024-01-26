package pack

object basicTwo {
  // the `args` parameter is an array of strings,
  // typically used for command line arguments
  def main(args:Array[String]):Unit={
    println("======started=======")
    println("Raw List")

    val ls = List(1,2,3,4)
    println(ls)

    println("Addition List")
    // applies a transformation to each element of the list 'ls'
    // by adding 10 to each element
    val proclis = ls.map(z=>z+10)
    println(proclis)

    println("multiply list")
    val mullis = ls.map(x=>x*10)
    println(mullis)
  }

}
/*
  |Concepts Demonstrated:
  |
  |Immutable Collections: The list ls is immutable, a fundamental concept in functional programming.
  |Higher-Order Functions: The map function is a higher-order function that takes a function as an argument
  |(in this case, anonymous functions x => x + 10 and x => x * 10) and applies it to each element of the collection.
  |Functional Programming Style: This example uses a functional style of programming, evident in the use of immutability and higher-order functions.
  |
 */
