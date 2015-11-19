/**
  * Created by katja on 17/11/15.
  */
class LabelledVertex(val label: Int) extends VertexNeighbours[LabelledVertex] {

  override def toString = s"$label: $neighboursString"
  def sortedNeighbours = neighbours.toSeq.sortWith(_.label < _.label)
  def neighboursString: String = {
    //val first = sortedNeighbours.head.label.toString
    //sortedNeighbours.map(_.label.toString).drop(1).foldLeft(first)((a,b) => s"$a, $b")
    "ha"
  }

}
