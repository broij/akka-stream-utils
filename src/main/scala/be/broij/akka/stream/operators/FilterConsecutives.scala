package be.broij.akka.stream.operators

import akka.NotUsed
import akka.stream.scaladsl.Flow

object FilterConsecutives {
  /**
    * Creates a flow filtering out the elements not matching the shouldFilter predicate. The function shouldFilter is a 
    * predicate taking as parameters an Option wrapping the last element emitted (None if no element was sent emitted 
    * yet) and the current element to be tested.
    */
  def apply[T](shouldFilter: (Option[T], T) => Boolean): Flow[T, T, NotUsed] =
    Flow[T].scan[(Option[T], Boolean)]((None, false)) {
      case ((current, _), next) => 
        if (shouldFilter(current, next)) (Some(next), true) 
        else (current, false)
    }.filter(_._2)
    .mapConcat(_._1.toList)
}
