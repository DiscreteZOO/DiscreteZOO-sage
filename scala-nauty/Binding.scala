import scala.collection.mutable.ArrayBuffer

/**
 * Created by katja on 04/11/15.
 */
class Binding {
  @native
  def sparseNauty(indices: Array[Int], degrees: Array[Int], neighbours: Array[Int], mininvarlevel: Int, maxinvarlevel: Int, invararg: Int): String
  def callSparseNauty(adjacencyList: Map[Int, Set[Int]]): String = {
    val vertexIndices = new ArrayBuffer[Int]
    val vertexDegrees = new ArrayBuffer[Int]
    val neighboursList = new ArrayBuffer[Int]

    def appendVertex(neighbours: Set[Int]): Unit = {
      vertexIndices += neighboursList.length
      vertexDegrees += neighbours.size
      neighboursList ++= neighbours
    }

    adjacencyList.zipWithIndex.toSeq.sortWith(_._2 < _._2).foreach(z => appendVertex(z._1._2))
    sparseNauty(vertexIndices.toArray, vertexDegrees.toArray, neighboursList.toArray, 0, 1, 0)
  }
  System.loadLibrary("ScalaNauty")
}
