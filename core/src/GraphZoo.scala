

/**
 * Created by katja on 20/08/15.
 */

object GraphZoo {

  def main (args: Array[String]) {

    val adjacenciesString = "[[0,1], [1, 2], [2,0]]"
    val graph = new RegularGraph(adjacenciesString)
    println(graph.nautyCanonical)
    println(graph)

  }

}