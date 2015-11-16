/**
 * Created by katja on 11/11/15.
 */
trait ComputeCanonicalLabelling {
  val name: String
  def compute(x: Any): String
}

case class ComputeNautyCanonicalLabelling(vertexInvariant: Int) extends ComputeCanonicalLabelling {
  val name = "Nauty"
  override def compute(x: Any): String = ???
}

// TODO Bliss, etc?

/*
  System.loadLibrary("NautyJNI")
  val sample = new NautyJNI
  val square = sample.intMethod(5)
  val bool = sample.booleanMethod(true)
  val text = sample.stringMethod("java")
  val sum = sample.intArrayMethod(Array(1, 1, 2, 3, 5, 8, 13))

  println(s"intMethod: $square")
  println(s"booleanMethod: $bool")
  println(s"stringMethod: $text")
  println(s"intArrayMethod: $sum")
  */

/*
val c20i1 = Map(0 -> Array(4, 9, 10),
  1 -> Array(2, 11, 13),
  2 -> Array(14, 1, 6),
  3 -> Array(4, 13, 9),
  4 -> Array(3, 0, 18),
  5 -> Array(15, 8, 18),
  6 -> Array(7, 16, 2),
  7 -> Array(6, 14, 12),
  8 -> Array(12, 5, 19),
  9 -> Array(0, 3, 17),
  10 -> Array(15, 0, 18),
  11 -> Array(17, 14, 1),
  12 -> Array(8, 7, 16),
  13 -> Array(17, 3, 1),
  14 -> Array(2, 7, 11),
  15 -> Array(10, 19, 5),
  16 -> Array(6, 19, 12),
  17 -> Array(13, 11, 9),
  18 -> Array(5, 10, 4),
  19 -> Array(15, 16, 8)
)
val graph = c20i1
val levels = mutable.Map.empty[Int, Array[Int]]

def isValid(f: Array[Boolean]): Boolean = {
  levels.get(f.length - 1).foreach(list => { // Option
    return list.map(vertex => { // for each vertex that needs to be checked at this level
      !graph.apply(vertex).map(v => f.apply(v)).foldLeft(false)((b, a) => b ^ a) | f.apply(vertex)
    }).foldLeft(true)((b, a) => b & a) // all need to sum up ok
  })
  true
}

def countFunctions(f: Array[Boolean]): Int = {
  if (isValid(f)) if (f.length == graph.size) 1 else countFunctions(f :+ false) + countFunctions(f :+ true)
  else 0
}

def main (args: Array[String]) {
  Range(0, graph.size).foreach(v => {
    val max = (graph.apply(v) :+ v).max
    levels.update(max, levels.getOrElse(max, Array()) :+ v)
  })
  println(countFunctions(Array()))
}
*/