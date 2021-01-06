package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import be.broij.akka.stream.FlowExtensions.SwitchFlowConversion

object MapSwitch {
  /**
    * Creates a flow using the mapper function to turn each element of a stream in a substream and then flattening all 
    * of these substreams via the [[Switch.apply]] operator. One can refer to the [[Switch.apply]] specification to get 
    * detailed information about the role of the parameters of this method that weren't covered in this specification.
    */
  def apply[T, U](mapper: T => Source[U, NotUsed])(implicit materializer: Materializer): Flow[T, U, NotUsed] =
    Flow[T].map(mapper).switch
}
