

/**
 * Created by katja on 20/08/15.
 */

object GraphZoo {

  def main (args: Array[String]) {

    println(new Binding().foo(1, 2, 3, "to je string"))

    val adjacenciesString = "[[0,1], [1, 2], [2,0]]"
    val graph = new RegularGraph(adjacenciesString)
    println(graph)


  }

}