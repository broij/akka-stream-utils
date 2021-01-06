package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.{KillSwitches, Materializer, OverflowStrategy, UniqueKillSwitch}
import be.broij.akka.stream.FlowExtensions.ConcatenateFlowConversion

object Switch {
  /**
    * Creates a flow concatenating streams with preemption. It turns streams of streams of a given abstraction into 
    * streams of that given abstraction. Let us refer to the streams embedded in a stream as substreams. The flow takes 
    * the first substream and emits all of its elements one by one in order. As soon as the next substream is available, 
    * it stops the substream currently being processed. When the substream being processed completes, the flow takes the
    * next substream and repeats that process. The flow completes when the stream completes and all of its substreams 
    * have been processed. It fails when the stream fails or the substream being processed fails. The materializer is 
    * used to pre-materialize each substream.
    */
  def apply[T](implicit materializer: Materializer): Flow[Source[T, NotUsed], T, NotUsed] =
    Flow[Source[T, NotUsed]].scan[Option[(UniqueKillSwitch, Source[T, NotUsed])]](None) {
      case (state, newSource) =>
        state.foreach(_._1.shutdown())
        Some(newSource.viaMat(KillSwitches.single)(Keep.right).preMaterialize())
    }.collect {
      case Some((_, source)) => source
    }.buffer(1, OverflowStrategy.backpressure).concatenate
}
