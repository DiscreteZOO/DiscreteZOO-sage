/**
 * Created by katja on 10/11/15.
 */

import scala.collection.mutable
import scala.util.matching.Regex

class ZooGraph(adjacenciesString: String) {

  val adjacencyList = build(adjacenciesString)
  val idsByVertices = adjacencyList.map(_.swap)
  val order = adjacencyList.size

  class Vertex {
    val neighbours = mutable.Set[Vertex]()
    def addNeighbour(neighbour: Vertex): Unit = neighbours += neighbour
    def getId(): Option[Int] = idsByVertices.get(this)
    def getIdString(): String = this.getId().map(_.toString).getOrElse("#")
    def getIdInt(): Int = this.getId().getOrElse(0)
    override def toString(): String = {
      val sortedNeighbours = neighbours.toSeq.sortWith(_.getIdInt() < _.getIdInt())
      s"${this.getIdString}: ${sortedNeighbours.drop(1).map(_.getIdString).foldLeft(sortedNeighbours.head.getIdString)((a,b) => s"$a, $b")}"
    }
  }

  private def build(adjacenciesString: String): Map[Int, Vertex] = {

    val verticesById = mutable.Map[Int, Vertex]()
    val edgePattern = new Regex("""(\d+)[\ ,]+(\d+)""", "vertex1", "vertex2")

    def addEdge(vertexNo1: Int, vertexNo2: Int): Unit = {
      val vertex1 = verticesById.getOrElseUpdate(vertexNo1, new Vertex)
      val vertex2 = verticesById.getOrElseUpdate(vertexNo2, new Vertex)
      vertex1.addNeighbour(vertex2)
      vertex2.addNeighbour(vertex1)
    }

    edgePattern.findAllIn(adjacenciesString).matchData foreach {
      edge => addEdge(edge.group("vertex1").toInt, edge.group("vertex2").toInt)
    }
    verticesById.values.zipWithIndex.map(pair => pair._2 -> pair._1).toMap
  }

  override def toString(): String = {
    adjacencyList.map(pair => pair._2.toString()).foldLeft(s"An undirected graph of order $order.\n")((a,b) => s"$a$b\n")
  }
}
