import scala.collection.immutable

object Test003 {
  def main(args: Array[String]): Unit = {
    val a: Iterator[String]  = Iterator[String]()
    val s: immutable.IndexedSeq[Any] = a +: "aa"
    println(s.size)
    println(s.size!=0)
    s.foreach(println)
  }

}
