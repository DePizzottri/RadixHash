import org.scalatest.WordSpec

class RecurisiveHasBasicSpec extends WordSpec {
  import depizzottri._

  def rand() =  {
    val a = scala.util.Random.nextLong()
    if(a < 0) -a else a
  }

  "RecursiveHashTable" when {
    "empty" should {
      "have size 0" in {
        var table = AggregateTable.empty[Long]
        (table.size === 0l)
      }
    }
    "being added some values" should {
      "have same size" in {
        var table = AggregateTable.empty[Long]
        val SIZE = 1000000
        val data = (1 to SIZE)
        for(d <- data)
          table = table.add(rand)
        assert(table.size == SIZE)
      }

      "contains that values" in  {
        var table = AggregateTable.empty[Long]
        val SIZE = 1000000
        val data = (1 to SIZE).map(x => rand)
        for(d <- data)
          table = table.add(d)
        for(d <- data)
          assert(table.contains(d))
      }

      "be empty after removing all that values" in {
        var table = AggregateTable.empty[Long]
        val SIZE = 1000000
        val data = (1 to SIZE).map(x => rand)
        //val edata = (1 to SIZE).map(x => rand)
        for(d <- data) {
          table = table.add(d)
        }
        assert(table.size == SIZE)
        for(d <- data) {
          table = table.remove(d)
        }
        assert(table.size == 0l)
      }

      "not contains some removed values" in {
        var table = AggregateTable.empty[Long]
        val SIZE = 1000000
        val data = (1 to SIZE).map(x => rand)
        for(d <- data) {
          table = table.add(d)
        }
        assert(table.size == SIZE)
        for(d <- data)
          assert(table.contains(d))

        val edata = (1 to SIZE).map(x => rand)
        for(d <- edata) {
          table = table.remove(d)
        }

        assert((data.toSet diff edata.toSet).size == table.size)
        for(d <- edata) {
          assert(!table.contains(d))
        }
      }
    }

    "being added some values twice" should {
      "have size as not multiset" in {
        var table = AggregateTable.empty[Long]
        val SIZE = 1000000
        val data = (1 to SIZE).map(x => rand)
        for(d <- data)
          table = table.add(d)
        for(d <- data)
          table = table.add(d)
        assert(table.size == SIZE)
      }

      "being empty dublicates all values once" in  {
        var table = AggregateTable.empty[Long]
        val SIZE = 1000000
        val data = (1 to SIZE).map(x => rand)
        for(d <- data)
          table = table.add(d)
        for(d <- data)
          table = table.add(d)
        assert(table.size == SIZE)
        for(d <- data)
          table = table.remove(d)
        assert(table.size == 0)
      }
    }
  }

  "CRC32 of Hash table" when {
    "empty" should {
      "be equal 0" in {
        var table = AggregateTable.empty[Long]
        assert(table.crc32 == 0l)
      }
    }

    "have some data" should {
      "calc same crc32 independenly of add order" in {
        val SIZE = 1000
        val data = (1 to SIZE).map(x => rand)

        var table1 = AggregateTable.empty[Long]
        for(d <- data)
          table1.add(d)

        var table2 = AggregateTable.empty[Long]
        for(d <- scala.util.Random.shuffle(data))
          table2.add(d)

        assert(table1.crc32 == table2.crc32)
      }
    }
  }
}
