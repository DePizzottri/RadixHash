import depizzottri._

object Main extends App {
  val SIZE = 20000000

  def rand() =  {
    val a = scala.util.Random.nextLong()
    if(a < 0) -a else a
  }

  def radixCreation[T](data:Seq[T]) = {
    val table = AggregateTable(data)
  }

  def scalaCreation[T](data:Seq[T]) = {
    val table = Set(data)
  }

  def javaCreation[T](data:Seq[T]) = {
    var table = new java.util.HashSet[T]()
    for (x <- data) {
      table.add(x)
    }
  }

  val FACTOR = 10

  def radixAdd() = {
    var table = AggregateTable.empty[Long]
    var i = SIZE
    while(i != 0) {
      if(rand % FACTOR == 0)
        table = table.add(rand)
      else
        table.contains(rand)
      i-=1
    }
  }

  def scalaAdd = {
    var table = scala.collection.immutable.HashSet.empty[Long]
    var i = SIZE
    while(i != 0) {
      if(rand % FACTOR == 0)
        table = table + rand
      else
        table.contains(rand)
      i-=1
    }
  }

  def javaAdd = {
    var table = new java.util.HashSet[Long]()
    var i = SIZE
    while(i != 0) {
      if(rand % FACTOR == 0)
        table.add(rand)
      else
        table.contains(rand)
      i-=1
    }
  }


  def measure[T](name:String, data:Seq[T], f:Seq[T] => Unit) = {
    val start = System.currentTimeMillis()
    f(data)
    val end = System.currentTimeMillis()
    println(s"$name ${end - start} millis")
  }

  def measure[T](name:String, f: => Unit) = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    println(s"$name ${end - start} millis")
  }

  //val SIZE = 15000000
//  val data = for(x <- 0 to SIZE) yield {
//    val a = scala.util.Random.nextLong()
//    if(a < 0) -a else a
//  }
//
  println(s"Begin $SIZE")
//  measure("Java", data, javaCreation)
//  measure("Scala", data, scalaCreation)
//  measure("Radix", data, radixCreation)
  measure("Java", javaAdd)
  measure("Radix", radixAdd)
  measure("Scala", scalaAdd)

  //val data = (0 to 10000).toArray

//  val table = AggregateTable(data)
//  //table.print
//  for(x <- data) {
//    if(!table.contains(x)) {
//      println(s"Error $x")
//    }
//  }

  println("End")
}
