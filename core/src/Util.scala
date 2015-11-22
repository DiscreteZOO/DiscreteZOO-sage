import scala.collection.mutable
import scala.util.matching.Regex

/**
  * Created by katja on 17/11/15.
  */
object Util {

  def commaJoin(a: Any, b: Any): String = s"$a, $b"

  def edgeListStringToAdjacencyList(adjacenciesString: String): Map[Int, LabelledVertex] = {

    val verticesByIds = mutable.Map[Int, LabelledVertex]()

    def addEdge(vertexNo1: Int, vertexNo2: Int): Unit = {
      val vertex1 = verticesByIds.getOrElseUpdate(vertexNo1, new LabelledVertex(vertexNo1))
      val vertex2 = verticesByIds.getOrElseUpdate(vertexNo2, new LabelledVertex(vertexNo2))
      vertex1.addNeighbour(vertex2)
      vertex2.addNeighbour(vertex1)
    }

    val edgePattern = new Regex("""(\d+)[\ ,]+(\d+)""", "v1", "v2")

    edgePattern.findAllIn(adjacenciesString).matchData.foreach(edge => {
      addEdge(edge.group("v1").toInt, edge.group("v2").toInt)
    })

    verticesByIds.values.zipWithIndex.map(_.swap).toMap
    //verticesByIds.values.zipWithIndex.map(pair => pair._2 -> pair._1).toMap
  }

}
