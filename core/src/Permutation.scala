/**
 * Created by katja on 11/11/15.
 */
case class Permutation(permutation: Map[Int, Int]) {

  val setSize = this.permutation.size

  require(this.permutation.keySet == this.permutation.values.toSet, "The argument is not a permutation")
  require(this.permutation.keySet == Range(0, setSize).toSet, "A permutation on " + setSize + " elements must be defined on the set {0, ..., " + (setSize-1) + "}.")

  def apply(_1: Int) = permutation(_1)

}