package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet, SourceShape}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import scala.collection.mutable.ListBuffer

class JoinFairly[T](n: BigInt, breadth: Option[BigInt])
    extends GraphStage[FlowShape[Graph[SourceShape[T], NotUsed], T]] {
  protected val in: Inlet[Graph[SourceShape[T], NotUsed]] =
    Inlet[Graph[SourceShape[T], NotUsed]]("joinFairlyN.in")
  protected val out: Outlet[T] = Outlet[T]("joinFairlyN.out")

  override def shape: FlowShape[Graph[SourceShape[T], NotUsed], T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var currentSink: SubSinkInlet[T] = _
      var i = 0
      val sinks: ListBuffer[SubSinkInlet[T]] = ListBuffer.empty
      var nSinks = 0
      var exhausted = false

      def startUsing(sink: SubSinkInlet[T]): Unit = {
        i = 0
        currentSink = sink
        sink.pull()
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val sink = new SubSinkInlet[T]("joinFairlyN.subSink")
          sink.setHandler(new InHandler {
            override def onPush(): Unit = {
              push(out, sink.grab())
              i += 1
              if (i >= n) {
                sinks += currentSink
                currentSink = null
              }
            }

            override def onUpstreamFinish(): Unit = {
              nSinks -= 1
              if (exhausted) {
                if (nSinks == 0) completeStage()
              } else if (breadth.forall(_ > nSinks)) pull(in)

              if (sink == currentSink) {
                if (isAvailable(out) && nSinks > 0) startUsing(sinks.remove(0))
                else currentSink = null
              } else sinks -= sink
            }
          })

          subFusingMaterializer.materialize(
            Source.fromGraph(grab(in)).to(sink.sink),
            defaultAttributes = inheritedAttributes
          )

          if (isAvailable(out) && nSinks == 0) startUsing(sink)
          else sinks += sink

          nSinks += 1
          if (breadth.forall(_ > nSinks)) pull(in)
        }

        override def onUpstreamFinish(): Unit = exhausted = true
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (nSinks > 0) {
            if (currentSink == null) startUsing(sinks.remove(0))
            else currentSink.pull()
          } else pull(in)
      })
    }
}

object JoinFairly {
  /**
    * Creates a flow joining streams with a fair policy. It turns streams of streams of a given abstraction into streams 
    * of that given abstraction. Let us refer to the streams embedded in a stream as substreams. The flow takes the 
    * first M substreams and join them by emitting the next n elements from the first substream being joined followed by 
    * the next n elements from the second substream being joined, and so on so forth. When one of the substreams being 
    * joined completes, the flow takes the next substream and continues its process with that substream added in the set 
    * of substreams it joins. The flow completes when the stream completes and all of its substreams have been 
    * processed. It fails when the stream fails or one of the substreams being processed fails. The number of substreams 
    * to join at the same time is given by the parameter breadth. When that optional value is set None, there is no 
    * limit on the maximum number to process simultaneously.
    */
  def apply[T](n: BigInt, breadth: Option[BigInt]): Flow[Source[T, NotUsed], T, NotUsed] =
    Flow.fromGraph(new JoinFairly[T](n, breadth))
}
