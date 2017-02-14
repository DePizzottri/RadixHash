import depizzottri._

object Main extends App {
  //val SIZE = 20000000

  def rand() =  {
    val a = scala.util.Random.nextLong()
    if(a < 0) -a else a
  }

//  def radixCreation(data:Seq[Long]) = {
//    val table = AggregateTable(data)
//  }
//
//  def scalaCreation(data:Seq[Long]) = {
//    val table = Set(data)
//  }
//
//  def javaCreation(data:Seq[Long]) = {
//    var table = new java.util.HashSet[Long]()
//    for (x <- data) {
//      table.add(x)
//    }
//  }
//
//  val FACTOR = 10
//
//  def radixAdd() = {
//    var table = AggregateTable.empty
//    var i = SIZE
//    while(i != 0) {
//      if(rand % FACTOR == 0)
//        table = table.add(rand)
//      else
//        table.contains(rand)
//      i-=1
//    }
//  }
//
//  def scalaAdd = {
//    var table = scala.collection.immutable.HashSet.empty[Long]
//    var i = SIZE
//    while(i != 0) {
//      if(rand % FACTOR == 0)
//        table = table + rand
//      else
//        table.contains(rand)
//      i-=1
//    }
//  }
//
//  def javaAdd = {
//    var table = new java.util.HashSet[Long]()
//    var i = SIZE
//    while(i != 0) {
//      if(rand % FACTOR == 0)
//        table.add(rand)
//      else
//        table.contains(rand)
//      i-=1
//    }
//  }
//
//
//  def measure[T](name:String, data:Seq[Long], f:Seq[Long] => Unit) = {
//    val start = System.currentTimeMillis()
//    f(data)
//    val end = System.currentTimeMillis()
//    println(s"$name ${end - start} millis")
//  }
//
//  def measure[T](name:String, f: => Unit) = {
//    val start = System.currentTimeMillis()
//    f
//    val end = System.currentTimeMillis()
//    println(s"$name ${end - start} millis")
//  }

  //val SIZE = 15000000
//  val data = for(x <- 0 to SIZE) yield {
//    val a = scala.util.Random.nextLong()
//    if(a < 0) -a else a
//  }
//
  //println(s"Begin $SIZE")
//  measure("Java", data, javaCreation)
//  measure("Scala", data, scalaCreation)
//  measure("Radix", data, radixCreation)
  // measure("Java", javaAdd)
  // measure("Radix", radixAdd)
  // measure("Scala", scalaAdd)

  //val data = (0 to 10000).toArray

//  val table = AggregateTable(data)
//  //table.print
//  for(x <- data) {
//    if(!table.contains(x)) {
//      println(s"Error $x")
//    }
//  }

  //val eqFactor = 0.99
  val eqFactors = List(0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99)
  for(eqFactor <- eqFactors) {
    val SIZE = 100000
    //println(s"$SIZE factor:$eqFactor same:${SIZE*eqFactor} different:${SIZE*(1-eqFactor)}")

    val sameData = (1 to ((SIZE*eqFactor)).toInt).map(x => rand)
    val diffData1 = (1 to ((SIZE*(1-eqFactor)).toInt)).map(x => rand)
    val diffData2 = (1 to ((SIZE*(1-eqFactor)).toInt)).map(x => rand)

    var table1 = AggregateTable.empty
    for(d <- sameData)
      table1 = table1.add(d)

    for(d <- diffData1)
      table1 = table1.add(d)

    var table2 = AggregateTable.empty
    for(d <- sameData)
      table2 = table2.add(d)
    for(d <- diffData2)
      table2 = table2.add(d)

    for(lvl <- 0 to 1) {
      //println("================================")
      val hash1 = table1.levelCrc32(lvl).sorted
      val hash2 = table2.levelCrc32(lvl).sorted

      println(s"Hash size ${hash1.size}")

      val diff = hash1 diff hash2

      val exactDiff = (diffData1.sorted diff diffData2.sorted).size

      // println(s"$lvl-level hash diff size == ${diff.size}");
      val expected = (SIZE.toDouble/hash2.size)*(diff.size)
      // println(s"Expected elements to transfer ${expected}")
      //
      // println(s"Full elements to transfer $SIZE")
      // println(s"Exact elements to transfer ${exactDiff}")
      //
      // println(s"Improvement: ${(1-expected/SIZE.toDouble)*100}%")
      // println(s"Honest Improvement: ${(1-(expected+hash2.size)/SIZE.toDouble)*100}%")
      println(s"$eqFactor, ${(1-(expected+hash2.size)/SIZE.toDouble)*100}")
    }
  }
}
