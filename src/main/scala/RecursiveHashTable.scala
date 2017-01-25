package depizzottri

import scala.collection.immutable.{HashSet, HashMap}
import com.jcraft.jzlib._

object RecursiveHashTable {

  val BUCKET_SIZE = 256l //must be power of two
  val MASK = BUCKET_SIZE - 1
  val SHIFT = 8

  def hash[T](h:T):Long = {
      // This function ensures that hashCodes that differ only by
      // constant multiples at each bit position have a bounded
      // number of collisions (approximately 8 at default load factor).
     val hh = h.hashCode
     val hr = hh ^ ((hh >>> 20) ^ (hh >>> 12))
     hr ^ (hr >>> 7) ^ (hr >>> 4)
  }
}

abstract class RecursiveHashTable[T] {
  def add(t:T): RecursiveHashTable[T]
  def remove(t:T): RecursiveHashTable[T]
  def isEmpty: Boolean
  def contains(t:T): Boolean

  def size:Long
  def crc32:Long

  def crc32WithLen:(Long,Long)

  def print:Unit
}

class AggregateTable[T](data:HashMap[Long, RecursiveHashTable[T]], val depth:Int) extends RecursiveHashTable[T] {
  import AggregateTable._
  import RecursiveHashTable._

  override def print =
    data.foreach{_._2.print}

  override def add(t:T): RecursiveHashTable[T] = {
    val idx = indexFor(hash(t), depth)
    val subbucket = data(idx)
    new AggregateTable(data.updated(idx, subbucket.add(t)), depth)
  }

  override def remove(t:T) : RecursiveHashTable[T] = {
    val idx = indexFor(hash(t), depth)
    val subbucket = data(idx)
    new AggregateTable(data.updated(idx, subbucket.remove(t)), depth)
  }

  override def contains(t:T) = {
    val idx = indexFor(hash(t), depth)
    val subbucket = data(idx)
    subbucket.contains(t)
  }

  override def isEmpty = false

  override def size = data.foldLeft(0l){(s, d) => s + d._2.size}

  override def crc32 = {
    data.map{ case (idx, elem) =>
      elem.crc32WithLen
    }
    .foldLeft(0l){ (prev, cur) =>
      JZlib.crc32_combine(prev, cur._1, cur._2)
    }
  }

  override def crc32WithLen = {
    data.map{ case (idx, elem) =>
      elem.crc32WithLen
    }
    .foldLeft((0l, 0l)){ (prev, cur) =>
      (JZlib.crc32_combine(prev._1, cur._1, cur._2), prev._2 + cur._2)
    }
  }
}

object AggregateTable {
  import RecursiveHashTable._

  def indexFor(h:Long, depth:Int):Int = {
    ((h >> SHIFT*depth) & MASK).toInt
  }

  def empty[T]:RecursiveHashTable[T] = {
    val array = for(i <- 0l until BUCKET_SIZE) yield {
      (i, new FinalTable(HashSet.empty[T], 0)).asInstanceOf[(Long,RecursiveHashTable[T])]
    }

    new AggregateTable[T](HashMap(array:_*), 0)
  }

  def apply[T] (data:Seq[T]):RecursiveHashTable[T] = {
    data.foldLeft(AggregateTable.empty[T]){ (t,e) =>
      t.add(e)
    }
  }
}

class FinalTable[T](val data:HashSet[T], val depth:Int) extends RecursiveHashTable[T] {
  override def print = {
    if(data.size != 0) {
      println("Bucket")
      println(s"Depth $depth")
      println(s"Size: ${data.size}")
      println(data.mkString(","))
      println("Bucket end\n")
    }
  }

  override def add(t: T): RecursiveHashTable[T] = {
    if (data.size < RecursiveHashTable.BUCKET_SIZE) {
      new FinalTable(data + t, depth)
    } else {
      repack(t)
    }
  }

  private def repack(t: T): RecursiveHashTable[T] = {
//    val arra = (for (i <- 0l until RecursiveHashTable.BUCKET_SIZE) yield {
//      (i, new FinalTable((data + t).filter { x =>
//        i == AggregateTable.indexFor(RecursiveHashTable.hash(x), depth + 1)
//      }.toSet, depth + 1)).asInstanceOf[(Long, RecursiveHashTable[T])]
//    }).toMap
//    new AggregateTable(arra, depth + 1)


//      val a = (0l until RecursiveHashTable.BUCKET_SIZE).foldLeft((Map.empty[Long, RecursiveHashTable[T]], data+t)){ (s, i) =>
////        val take = s._2.filter{ x=> i == AggregateTable.indexFor(RecursiveHashTable.hash(x), depth+1)}
////        val past = s._2.filterNot{ x=> i == AggregateTable.indexFor(RecursiveHashTable.hash(x), depth+1)}
//        val (take, past) = s._2.partition{x=> i == AggregateTable.indexFor(RecursiveHashTable.hash(x), depth+1)}
//        //(s._1 :+ new FinalTable(take, depth + 1), past)
//        (s._1 + (i-> new FinalTable(take, depth + 1)), past)
//      }
//      new AggregateTable(a._1, depth+1)

      val ar = (0l until RecursiveHashTable.BUCKET_SIZE).map{ x=>
        (x, new FinalTable[T](HashSet.empty[T], depth + 1)).asInstanceOf[(Long, RecursiveHashTable[T])]
      }

      var table:RecursiveHashTable[T] = new AggregateTable[T](HashMap(ar:_*), depth+1)

      for(d <- (data+t)) yield {
        table = table.add(d)
      }

      table
  }

  override def remove(t:T): RecursiveHashTable[T] = {
    new FinalTable(data - t, depth)
  }

  override def isEmpty = data.isEmpty

  override def contains(t:T) = {
    data.contains(t)
  }

  override def size = data.size
  override def crc32 = {
    var crc32 = new CRC32();
    data.foreach { elem =>
      import scala.math.BigInt
      val buf = BigInt(elem.asInstanceOf[Long]).toByteArray
      crc32.update(buf, 0, buf.length)
    }
    crc32.getValue
  }

  override def crc32WithLen = {
    var crc32 = new CRC32();
    val len = data.foldLeft(0l) { (prev, cur) =>
      import scala.math.BigInt
      val buf = BigInt(cur.asInstanceOf[Long]).toByteArray
      crc32.update(buf, 0, buf.length)
      prev + buf.length
    }
    (crc32.getValue, len)
  }
}
