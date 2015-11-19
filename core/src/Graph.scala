/**
 * Created by katja on 10/11/15.
 */

class Graph(adjacenciesString: String) {

  val adjacencies = IO.edgeListStringToGraph(adjacenciesString)
  val order = adjacencies.size
  val description = s"An undirected graph of order $order."

  def minDegree = adjacencies.mapValues(_.degree).values.min
  def maxDegree = adjacencies.mapValues(_.degree).values.max
  override def toString() = description

}
