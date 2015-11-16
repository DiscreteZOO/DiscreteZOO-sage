/**
 * Created by katja on 10/11/15.
 */

import scala.collection.mutable

class ZooGraph(adjacenciesString: String) {

  val adjacencyList = build(adjacenciesString)

  class Vertex {
    val neighbours = mutable.Set[Vertex]()
    def addNeighbour(neighbour: Vertex): Unit = neighbours += neighbour
  }

  private def build(adjacenciesString: String): Map[Int, Vertex] = {
    val verticesById = mutable.Map[Int, Vertex]()

    def addEdge(vertexNo1: Int, vertexNo2: Int): Unit = {
      val vertex1 = verticesById.getOrElseUpdate(vertexNo1, new Vertex)
      val vertex2 = verticesById.getOrElseUpdate(vertexNo2, new Vertex)
      vertex1.addNeighbour(vertex2)
      vertex2.addNeighbour(vertex1)
    }

    val edgePairs = """(\d+[\ ,\ ]+\d+)""".r.findAllIn(adjacenciesString).map(s => """(\d+)""".r.findAllIn(s).toSeq match {
      case Seq(a, b) => (a.toInt, b.toInt)
    })

    edgePairs.foreach(edge => addEdge(edge._1, edge._2))
    verticesById.values.zipWithIndex.map(pair => pair._2 -> pair._1).toMap
  }

}
