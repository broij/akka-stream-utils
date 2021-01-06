package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import be.broij.akka.stream.FlowExtensions.JoinWithPrioritiesFlowConversion

object MapJoinWithPriorities {
  /**
    * Creates a flow using the mapper function to turn each element of a stream in a substream and then flattening all 
    * of these substreams via the [[JoinWithPriorities.apply]] operator. One can refer to the 
    * [[JoinWithPriorities.apply]] specification to get detailed information about the role of the parameters of this 
    * method that weren't covered in this specification.
    */
  def apply[T, U, P: Ordering](mapper: T => Source[U, NotUsed], priorityOf: U => P,
                               breadth: Option[BigInt]): Flow[T, U, NotUsed] =
    Flow[T].map(mapper).joinWithPriorities(priorityOf, breadth)
}
