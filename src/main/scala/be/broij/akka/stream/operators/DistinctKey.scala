package be.broij.akka.stream.operators

import akka.NotUsed
import akka.stream.scaladsl.Flow

object DistinctKey {
  /**
    * Creates a flow filtering out the elements whose key is the same than the one of the precedent element. The 
    * function keyOf is used to extract the keys of the elements.
    */
  def apply[T, K](keyOf: T => K): Flow[T, T, NotUsed] =
    Flow[T].scan[(Option[T], Option[T])]((None, None)) {
      (f, s) => (f._2, Some(s))
    }.collect {
      case (f, Some(s)) if f.forall(keyOf(_) != keyOf(s)) => s
    }
}
