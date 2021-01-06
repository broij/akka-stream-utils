package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import be.broij.akka.stream.FlowExtensions.JoinFlowConversion

object MapJoin {
  /**
    * Creates a flow using the mapper function to turn each element of a stream in a substream and then flattening all 
    * of these substreams via the [[Join.apply]] operator. One can refer to the [[Join.apply]] specification to get 
    * detailed information about the role of the parameters that weren't covered in this specification.
    */
  def apply[T, U](mapper: T => Source[U, NotUsed], breadth: Option[BigInt]): Flow[T, U, NotUsed] =
    Flow[T].map(mapper).join(breadth)
}
