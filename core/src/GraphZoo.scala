/**
 * Created by katja on 20/08/15.
 */

object GraphZoo {

  def main (args: Array[String]) {

    //println(new Binding().foo(1, 2, 3, "to je string"))

    val v = new LabelledVertex(0)
    val w = new LabelledVertex(1)
    v.addNeighbour(w)
    println(v.neighbours)

    val adjacenciesString = "[[0,1], [1, 2], [2,0]]"
    val c3 = new RegularGraph(adjacenciesString)
    println(c3)

  }

}