import org.scalatest.WordSpec

class RecurisiveHasBasicSpec extends WordSpec {
  import depizzottri._

  def rand() =  {
    val a = scala.util.Random.nextLong()
    if(a < 0) -a else a
  }

  "RecursiveHashTable" when {
  //   "empty" should {
  //     "have size 0" in {
  //       var table = AggregateTable.empty
  //       (table.size === 0l)
  //     }
  //   }
  //   "being added some values" should {
  //     "have same size" in {
  //       var table = AggregateTable.empty
  //       val SIZE = 1000000
  //       val data = (1 to SIZE)
  //       for(d <- data)
  //         table = table.add(rand)
  //       assert(table.size == SIZE)
  //     }
  //
  //     "contains that values" in  {
  //       var table = AggregateTable.empty
  //       val SIZE = 1000000
  //       val data = (1 to SIZE).map(x => rand)
  //       for(d <- data)
  //         table = table.add(d)
  //       for(d <- data)
  //         assert(table.contains(d))
  //     }
  //
  //     "be empty after removing all that values" in {
  //       var table = AggregateTable.empty
  //       val SIZE = 1000000
  //       val data = (1 to SIZE).map(x => rand)
  //       //val edata = (1 to SIZE).map(x => rand)
  //       for(d <- data) {
  //         table = table.add(d)
  //       }
  //       assert(table.size == SIZE)
  //       for(d <- data) {
  //         table = table.remove(d)
  //       }
  //       assert(table.size == 0l)
  //     }
  //
  //     "not contains some removed values" in {
  //       var table = AggregateTable.empty
  //       val SIZE = 1000000
  //       val data = (1 to SIZE).map(x => rand)
  //       for(d <- data) {
  //         table = table.add(d)
  //       }
  //       assert(table.size == SIZE)
  //       for(d <- data)
  //         assert(table.contains(d))
  //
  //       val edata = (1 to SIZE).map(x => rand)
  //       for(d <- edata) {
  //         table = table.remove(d)
  //       }
  //
  //       assert((data.toSet diff edata.toSet).size == table.size)
  //       for(d <- edata) {
  //         assert(!table.contains(d))
  //       }
  //     }
  //   }
  //
  //   "being added some values twice (non-multiset)"  should {
  //     "have size as not multiset" in {
  //       var table = AggregateTable.empty
  //       val SIZE = 1000000
  //       val data = (1 to SIZE).map(x => rand)
  //       for(d <- data)
  //         table = table.add(d)
  //       for(d <- data)
  //         table = table.add(d)
  //       assert(table.size == SIZE)
  //     }
  //
  //     "being empty after add duplicates and remove it once (non-multiset)" in  {
  //       var table = AggregateTable.empty
  //       val SIZE = 1000000
  //       val data = (1 to SIZE).map(x => rand)
  //       for(d <- data)
  //         table = table.add(d)
  //       for(d <- data)
  //         table = table.add(d)
  //       assert(table.size == SIZE)
  //       for(d <- data)
  //         table = table.remove(d)
  //       assert(table.size == 0)
  //     }
  //   }
  }

  "CRC32 of Hash table" when {
    // "empty" should {
    //   "be equal 0" in {
    //     var table = AggregateTable.empty
    //     assert(table.crc32 == 0l)
    //   }
    //
    //   "have zero level one crc32's" in {
    //     var table = AggregateTable.empty
    //
    //     val lvlCrc32 = table.curLevelCrc32
    //     assert(lvlCrc32.size == RecursiveHashTable.BUCKET_SIZE)
    //     for(e <- lvlCrc32)
    //       assert(e == 0l)
    //   }
    // }

    "have some data" should {
      // "calc same crc32 independenly of add order" in {
      //   val SIZE = 100000
      //   val data = (1 to SIZE).map(x => rand)
      //
      //   var table1 = AggregateTable.empty
      //   for(d <- data)
      //     table1 = table1.add(d)
      //
      //   var table2 = AggregateTable.empty
      //   for(d <- scala.util.Random.shuffle(data))
      //     table2 = table2.add(d)
      //
      //   assert(table1.crc32 == table2.crc32)
      // }
      //
      // "calc same current level crc32 independenly of add order" in {
      //   val SIZE = 100000
      //   val data = (1 to SIZE).map(x => rand)
      //
      //   var table1 = AggregateTable.empty
      //   for(d <- data)
      //     table1 = table1.add(d)
      //
      //   var table2 = AggregateTable.empty
      //   for(d <- scala.util.Random.shuffle(data))
      //     table2 = table2.add(d)
      //
      //   assert(table1.curLevelCrc32 == table2.curLevelCrc32)
      // }
      //
      // "calc same first level crc32 independenly of add order" in {
      //   val SIZE = 1000000
      //   val data = (1 to SIZE).map(x => rand)
      //   // val data = Vector[Long](
      //   //    6506470447544980355l,
      //   //    4014110659905715843l)
      //   //val data = List(1,2)
      //
      //   var table1 = AggregateTable.empty
      //   import com.jcraft.jzlib._
      //
      //     def crc32(n:Long) = {
      //       var crc32 = new CRC32();
      //       import scala.math.BigInt
      //       var buf = BigInt(n).toByteArray
      //       buf = Array.fill[Byte](8 - buf.length)(0) ++ buf
      //       crc32.update(buf, 0, buf.length)
      //       require(buf.length == 8)
      //       crc32.getValue
      //     }
      //
      //   for(d <- data) {
      //     table1 = table1.add(d)
      //     //println(crc32(d))
      //   }
      //
      //   var table2 = AggregateTable.empty
      //   for(d <- scala.util.Random.shuffle(data))
      //     table2 = table2.add(d)
      //
      //   //println(data.filter{_!=0})
      //   //println("asdasdasd")
      //   val levelCrc3201 = table1.levelCrc32(0)
      //   //val levelCrc3202 = table2.levelCrc32(0)
      //
      //     // val calcedCrc32 = data.sorted.foldLeft(0l){ (prev, cur)=>
      //     //   println(cur, crc32(cur))
      //     //   JZlib.crc32_combine(prev, crc32(cur), 8)
      //     // }
      //     //
      //     // println("CALCED:", calcedCrc32)
      //
      //   assert(levelCrc3201.size == RecursiveHashTable.BUCKET_SIZE)
      //   assert(table1.curLevelCrc32.size == RecursiveHashTable.BUCKET_SIZE)
      //
      //   assert(levelCrc3201 == table1.curLevelCrc32)
      //
      //   assert(table1.curLevelCrc32 == table2.curLevelCrc32)
      //   assert(table1.levelCrc32(0) == table2.levelCrc32(0))
      //   assert(table1.curLevelCrc32 == table2.levelCrc32(0))
      // }
      //
      // "correct calc size of first level crc32" in {
      //   val SIZE = 1000000
      //   val data = (1 to SIZE).map(x => rand)
      //
      //   var table = AggregateTable.empty
      //   for(d <- data)
      //     table = table.add(d)
      //
      //   val lvlCrc32 = table.curLevelCrc32
      //   assert(lvlCrc32.size == RecursiveHashTable.BUCKET_SIZE)
      //
      //   assert(table.levelCrc32(0).size == lvlCrc32.size)
      // }
      //
      "correct calc size of second level crc32" in {
        val SIZE = 1000000
        val data = (1 to SIZE).map(x => rand)

        var table = AggregateTable.empty
        for(d <- data)
          table = table.add(d)

        val secondlvlCrc32 = table.levelCrc32(1)
        assert(secondlvlCrc32.size == RecursiveHashTable.BUCKET_SIZE * RecursiveHashTable.BUCKET_SIZE)
      }

        // "correct calc size of third level crc32" in {
        //   val SIZE = 1000
        //   val data = (1 to SIZE).map(x => rand)
        //
        //   var table = AggregateTable.empty
        //   for(d <- data)
        //     table = table.add(d)
        //
        //   val thirdlvlCrc32 = table.levelCrc32(2)
        //   assert(thirdlvlCrc32.size == RecursiveHashTable.BUCKET_SIZE * RecursiveHashTable.BUCKET_SIZE * RecursiveHashTable.BUCKET_SIZE)
        // }
    }
  }
}
