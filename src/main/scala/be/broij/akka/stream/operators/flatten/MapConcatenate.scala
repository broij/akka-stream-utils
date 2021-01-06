package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import be.broij.akka.stream.FlowExtensions.ConcatenateFlowConversion

object MapConcatenate {
  /**
    * Creates a flow using the mapper function to turn each element of a stream in a substream and then flattening all 
    * of these substreams via the [[Concatenate.apply]] operator.
    */
  def apply[T, U](mapper: T => Source[U, NotUsed]): Flow[T, U, NotUsed] =
    Flow[T].map(mapper).concatenate
}
