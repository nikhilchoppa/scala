package pack

object splitFilter {
  def main(args: Array[String]):Unit={
    val data = List(
      "State->TN~City->Chennai",
      "State->AP~City->Amaravati"
    )
    println
    println("=====raw data=======")
    println
    data.foreach(println)


    val flat = data.flatMap(x=>x.split("~"))
    println
    println("====flat data====")
    println
    flat.foreach(println)


    val city = flat.filter(x=>x.contains("City"))
    println
    println("======City Data=====")
    println
    city.foreach(println)

    val state = flat.filter(x=>x.contains("State"))
    println
    println("======State Data=======")
    println
    state.foreach(println)

    val finalState = state.map(x=>x.replace("State->",""))
    println
    println("======Final State Data=======")
    println
    finalState.foreach(println)

    val finalCity = city.map(x=>x.replace("City->",""))
    println
    println("======Final City Data=======")
    println
    finalCity.foreach(println)


  }

}
