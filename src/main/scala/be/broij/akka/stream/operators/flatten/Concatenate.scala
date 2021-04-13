package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet, SourceShape}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

class Concatenate[T] extends GraphStage[FlowShape[Graph[SourceShape[T], NotUsed], T]] {
  protected val in: Inlet[Graph[SourceShape[T], NotUsed]] = Inlet[Graph[SourceShape[T], NotUsed]]("concat.in")
  protected val out: Outlet[T] = Outlet[T]("concat.out")

  override def shape: FlowShape[Graph[SourceShape[T], NotUsed], T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var currentSink: Option[SubSinkInlet[T]] = None
      var exhausted = false

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val sink = new SubSinkInlet[T]("concat.subSink")
          sink.setHandler(new InHandler {
            override def onPush(): Unit = push(out, sink.grab())
            override def onUpstreamFinish(): Unit = {
              currentSink = None
              if (exhausted) completeStage()
              else if (isAvailable(out)) pull(in)
            }
          })

          subFusingMaterializer.materialize(
            Source.fromGraph(grab(in)).to(sink.sink),
            defaultAttributes = inheritedAttributes
          )

          currentSink = Some(sink)
          sink.pull()
        }

        override def onUpstreamFinish(): Unit = exhausted = true

        override def onUpstreamFailure(throwable: Throwable): Unit = {
          if (currentSink.nonEmpty) currentSink.get.cancel(throwable)
          failStage(throwable)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = currentSink.map(_.pull()).getOrElse(pull(in))

        override def onDownstreamFinish(throwable: Throwable): Unit = {
          currentSink.foreach(_.cancel(throwable))
          failStage(throwable)
        }
      })
    }
}

object Concatenate {
  /**
    * Creates a flow concatenating streams. It turns streams of streams of a given abstraction into streams of that 
    * given abstraction. Let us refer to the streams embedded in a stream as substreams. The flow takes the first 
    * substream and emits all of its elements one by one in order. Then, when the substream completes, it takes the next
    * substream and repeats that process. The flow completes when the stream completes and all of its substreams have 
    * been processed. It fails when the stream fails or the substream being processed fails.
    */
  def apply[T]: Flow[Source[T, NotUsed], T, NotUsed] =
    Flow.fromGraph(new Concatenate[T])
}