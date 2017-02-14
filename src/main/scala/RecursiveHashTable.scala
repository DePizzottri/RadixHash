package depizzottri

import scala.collection.immutable.{SortedSet, HashMap}
import com.jcraft.jzlib._

import scala.collection.mutable.ListBuffer//List[Long]

object RecursiveHashTable {
  // val BUCKET_SIZE = 64l //must be power of two
  // val MASK = BUCKET_SIZE - 1
  // val SHIFT = 2
  // val depth = 32


  // val BUCKET_SIZE = 128l //must be power of two
  // val MASK = BUCKET_SIZE - 1
  // val SHIFT = 4
  // val depth = 16

  // val BUCKET_SIZE = 192 //must be power of two
  // val MASK = BUCKET_SIZE - 1
  // val SHIFT = 6
  // val depth = 12

  val BUCKET_SIZE = 256l //must be power of two
  val MASK = BUCKET_SIZE - 1
  val SHIFT = 8
  val depth = 8


  // val BUCKET_SIZE = 512 //must be power of two
  // val MASK = BUCKET_SIZE - 1
  // val SHIFT = 16
  // val depth = 4

  def hash(h:Long):Long = {
    // This function ensures that hashCodes that differ only by
    // constant multiples at each bit position have a bounded
    // number of collisions (approximately 8 at default load factor).
    //  val hh = h.hashCode
    //  val hr = hh ^ ((hh >>> 20) ^ (hh >>> 12))
    //  hr ^ (hr >>> 7) ^ (hr >>> 4)

    //no hash for sorting purpose
    h
  }

  // def CRC32(n:Long) = {
  //   var crc32 = new CRC32();
  //   import scala.math.BigInt
  //   val buf = BigInt(elem).toByteArray
  //   require(buf.length == 8)
  //   crc32.update(buf, 0, buf.length)
  //   crc32.getValue
  // }
}

abstract class RecursiveHashTable {
  def add(t:Long): RecursiveHashTable
  def remove(t:Long): RecursiveHashTable
  def isEmpty: Boolean
  def contains(t:Long): Boolean

  def size:Long
  def crc32:Long

  def crc32WithLen:(Long,Long)

  type Crc32List = ListBuffer[Long]

  def curLevelCrc32:Crc32List

  def levelCrc32(level:Int):Crc32List

  // def crc32(begin:Int, end:Int):Long
  // def crc32WithLen(begin:Int, end:Int):(Long,Long)

  def print:Unit
}

class AggregateTable(data:HashMap[Long, RecursiveHashTable], val depth:Int) extends RecursiveHashTable {
  import AggregateTable._
  import RecursiveHashTable._

  override def print =
    data.foreach{_._2.print}

  override def add(t:Long): RecursiveHashTable = {
    val idx = indexFor(hash(t), depth)
    val subbucket = data(idx)
    new AggregateTable(data.updated(idx, subbucket.add(t)), depth)
  }

  override def remove(t:Long) : RecursiveHashTable = {
    val idx = indexFor(hash(t), depth)
    val subbucket = data(idx)
    new AggregateTable(data.updated(idx, subbucket.remove(t)), depth)
  }

  override def contains(t:Long) = {
    val idx = indexFor(hash(t), depth)
    val subbucket = data(idx)
    subbucket.contains(t)
  }

  override def isEmpty = false

  override def size = data.foldLeft(0l) {
    (s, d) => s + d._2.size
  }

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

  override def curLevelCrc32:Crc32List = {
    ListBuffer[Long](data.map{ case (idx, elem) =>
      elem.crc32
    }.toSeq:_*)
  }

  override def levelCrc32(level:Int):Crc32List = {
    if(level<depth) {
      println(level)
      println(depth)
      val r = curLevelCrc32.foldLeft(0l){ (prev, cur) =>
        JZlib.crc32_combine(prev, cur, 8)
      }
      ListBuffer[Long](r)
    } else if(level == depth) {
      //println("hhhh")
      curLevelCrc32
    } else {
      //println("dddd")
      // ListBuffer[Long](data.foldLeft(ListBuffer.empty[Long]){ case (collected, current) =>
      //    collected ++= current._2.levelCrc32(level)
      // }.toSeq:_*)

      var ret = ListBuffer.empty[Long]
      for(d <- data) {
        ret ++= d._2.levelCrc32(level)
      }
      ret
    }
  }

  // override def crc32(begin:Int, end:Int):Long = {
  //   val start = List.empty[Long]
  //   data.foldLeft((start, 0l)) { (prev, elem) =>
//      val curElemSize = elem._2.size
//      val prevSize = prev._2
//
//      val curBeing = Math.max(0, (begin - prevSize))
//      val curLen = Math.min(curElemSize - curBeing, end - begin)
//
//      val ret = elem.crc32WithLen(curBegin, curBegin + curLen)
//
//      (ret :: prev._1, prevSize + curElemSize)
//    }
//    ._1
//    .filter{ elem =>
//      elem.size > 0
//    }
//    data.map{ elem =>
//      elem.crc32WithLen
//    }
  //   .foldLeft(0l){ (prev, cur) =>
  //     JZlib.crc32_combine(prev, cur._1, cur._2)
  //   }
  // }
  //
  // override def crc32WithLen(begin:Int, end:Int):(Long,Long)
}

object AggregateTable {
  import RecursiveHashTable._

  def indexFor(h:Long, depth:Int):Int = {
    ((h >> SHIFT*depth) & MASK).toInt
  }

  def empty:RecursiveHashTable = {
    val array = for(i <- 0l until BUCKET_SIZE) yield {
      (i, new FinalTable(SortedSet.empty, 0)).asInstanceOf[(Long,RecursiveHashTable)]
    }

    new AggregateTable(HashMap(array:_*), 0)
  }

  def apply (data:Seq[Long]):RecursiveHashTable= {
    data.foldLeft(AggregateTable.empty){ (t,e) =>
      t.add(e)
    }
  }
}

class FinalTable(val data:SortedSet[Long], val depth:Int) extends RecursiveHashTable {
  override def print = {
    if(data.size != 0) {
      println("Bucket")
      println(s"Depth $depth")
      println(s"Size: ${data.size}")
      println(data.mkString(","))
      println("Bucket end\n")
    }
  }

  override def add(t: Long): RecursiveHashTable = {
    if (data.size < RecursiveHashTable.BUCKET_SIZE) {
      new FinalTable(data + t, depth)
    } else {
      repack(t)
    }
  }

  private def repack(t: Long): RecursiveHashTable = {
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
        (x, new FinalTable(SortedSet.empty[Long], depth + 1)).asInstanceOf[(Long, RecursiveHashTable)]
      }

      var table:RecursiveHashTable = new AggregateTable(HashMap(ar:_*), depth+1)

      for(d <- (data+t)) yield {
        table = table.add(d)
      }

      table
  }

  override def remove(t:Long): RecursiveHashTable = {
    new FinalTable(data - t, depth)
  }

  override def isEmpty = data.isEmpty

  override def contains(t:Long) = {
    data.contains(t)
  }

  override def size = data.size

  override def crc32 = {
    var crc32 = new CRC32();
    data.foreach { elem =>
      import scala.math.BigInt
      var buf = BigInt(elem).toByteArray
      buf = Array.fill[Byte](8 - buf.length)(0) ++ buf
      require(buf.length == 8)
      crc32.update(buf, 0, buf.length)
    }
    crc32.getValue
  }

  override def crc32WithLen = {
    var crc32 = new CRC32();
    val len = data.foldLeft(0l) { (prev, cur) =>
      import scala.math.BigInt
      var buf = BigInt(cur).toByteArray
      buf = Array.fill[Byte](8 - buf.length)(0) ++ buf
      crc32.update(buf, 0, buf.length)
      require(buf.length == 8)
      prev + buf.length
    }
    (crc32.getValue, len)
  }

  override def curLevelCrc32:Crc32List = {
    //  (0l until RecursiveHashTable.BUCKET_SIZE).map{ idx =>
    //    if(data.exists{elem => idx == AggregateTable.indexFor(RecursiveHashTable.hash(elem), depth)}) {
    //      var crc32 = new CRC32();
    //      import scala.math.BigInt
    //      val elem = data.find{elem => idx == AggregateTable.indexFor(RecursiveHashTable.hash(elem), depth)}.get
    //      var buf = BigInt(elem).toByteArray
    //      buf = Array.fill[Byte](8 - buf.length)(0) ++ buf
    //      crc32.update(buf, 0, buf.length)
    //      require(buf.length == 8)
    //      crc32.getValue.toLong
    //    } else {
    //      0l
    //    }
    //  }.toList
    ListBuffer[Long](data.map { elem =>
      var crc32 = new CRC32();
      import scala.math.BigInt
      var buf = BigInt(elem).toByteArray
      buf = Array.fill[Byte](8 - buf.length)(0) ++ buf
      crc32.update(buf, 0, buf.length)
      require(buf.length == 8)
      crc32.getValue
    }.toSeq:_*)
  }

  override def levelCrc32(level:Int):Crc32List = { //7
    // println("==============")
    // println(level)
    // println(data)
    // if(data.size != 0) {
    //   println("============")
    // }
    if(level<depth) {
      //println("aaaaaa")
      val r = curLevelCrc32.foldLeft(0l){ (prev, cur) =>
        JZlib.crc32_combine(prev, cur, 8)
      }
      ListBuffer[Long](r)
    } else
    if (depth != RecursiveHashTable.depth - 1) { //6
      require(level >= depth)
      val levelsReminded = level - depth // 1
      //println(depth, levelsReminded, level)
      //val blockSize = scala.math.pow(RecursiveHashTable.BUCKET_SIZE, levelsReminded-1)
      var emptyRet = Array.fill(scala.math.pow(RecursiveHashTable.BUCKET_SIZE, levelsReminded + 1).toInt)(0l) //blocksize pow 2 elems
      val ret = for (ielem <- data.zipWithIndex) {
        //val idx = AggregateTable.indexFor(RecursiveHashTable.hash(ielem._1), depth)
        val elem = ielem._1

        //println("Elem:", elem)

        var offset = 0l
        var prevLvl = depth
        var curLvl = depth + 1
        var shift = scala.math.pow(RecursiveHashTable.BUCKET_SIZE, levelsReminded-1).toLong

        //println(elem)

        while(curLvl != level + 1) {
          val curLevelIdx = AggregateTable.indexFor(RecursiveHashTable.hash(elem), curLvl)
          val prevLevelIdx = AggregateTable.indexFor(RecursiveHashTable.hash(elem), prevLvl)

          offset = offset + prevLevelIdx * shift

          prevLvl = curLvl
          curLvl = curLvl + 1
          shift = shift / RecursiveHashTable.BUCKET_SIZE
        }

        var crc32 = new CRC32();
        import scala.math.BigInt
        var buf = BigInt(elem).toByteArray
        buf = Array.fill[Byte](8 - buf.length)(0) ++ buf
        crc32.update(buf, 0, buf.length)
        require(buf.length == 8)

        try {
          val prevCrc = emptyRet(offset.toInt)
          val newCrc = JZlib.crc32_combine(prevCrc, crc32.getValue, 8)

          emptyRet(offset.toInt) = newCrc
        } catch {
            case e:Exception => {
              println(depth)
              println(offset)
              println(emptyRet.size)
              println(data)
              println(elem)
              println(level)
            }
        }

      }
      // if(data.size != 0) {
      //   println("Return: ", emptyRet.toList.filter{_!=0})
      // }
      ListBuffer[Long](emptyRet.toSeq:_*)
    } else {
      //println("asd")
      require(level == depth)
      curLevelCrc32
    }
  }

  // def crc32(begin:Int, end:Int):Long = {
  //   var crc32 = new CRC32();
  //   data.slice(begin, end).foreach { elem =>
  //     import scala.math.BigInt
  //     val buf = BigInt(elem.asInstanceOf[Long]).toByteArray
  //     crc32.update(buf, 0, buf.length)
  //   }
  //   crc32.getValue
  // }
  //
  // def crc32WithLen(begin:Int, end:Int):(Long,Long) = {
  //   var crc32 = new CRC32();
  //   val len = data.slice(begin, end).foldLeft(0l) { (prev, cur) =>
  //     import scala.math.BigInt
  //     val buf = BigInt(cur.asInstanceOf[Long]).toByteArray
  //     crc32.update(buf, 0, buf.length)
  //     prev + buf.length
  //   }
  //   (crc32.getValue, len)
  // }
}
