import scala.collection.mutable

/**
  * Created by katja on 17/11/15.
  */
trait VertexNeighbours[T <: VertexNeighbours[T]] {

  val neighbours = mutable.Set[T]()

  def degree: Int = neighbours.size
  def addNeighbour(neighbour: T): Unit = neighbours += neighbour

}
