package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet, SourceShape}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import scala.collection.mutable.ListBuffer

class Join[T](breadth: Option[BigInt]) extends GraphStage[FlowShape[Graph[SourceShape[T], NotUsed], T]] {
  protected val in: Inlet[Graph[SourceShape[T], NotUsed]] = Inlet[Graph[SourceShape[T], NotUsed]]("join.in")
  protected val out: Outlet[T] = Outlet[T]("join.out")

  override def shape: FlowShape[Graph[SourceShape[T], NotUsed], T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      val buffer: ListBuffer[(T, SubSinkInlet[T])] = ListBuffer.empty
      val sinks: ListBuffer[SubSinkInlet[T]] = ListBuffer.empty
      var nSinks = 0
      var exhausted = false

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val sink = new SubSinkInlet[T]("join.subSink")
          sink.setHandler(new InHandler {
            override def onPush(): Unit =
              if (isAvailable(out)) {
                push(out, sink.grab())
                sink.pull()
              } else buffer += ((sink.grab(), sink))

            override def onUpstreamFinish(): Unit = {
              nSinks -= 1
              sinks -= sink
              if (exhausted) {
                if (nSinks == 0 && buffer.isEmpty) completeStage()
              } else if (breadth.forall(_ > nSinks)) pull(in)
            }
          })

          subFusingMaterializer.materialize(
            Source.fromGraph(grab(in)).to(sink.sink),
            defaultAttributes = inheritedAttributes
          )

          sink.pull()
          nSinks += 1
          sinks += sink
          if (breadth.forall(_ > nSinks)) pull(in)
        }

        override def onUpstreamFinish(): Unit = exhausted = true
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (buffer.nonEmpty) {
            val (item, sink) = buffer.remove(0)
            push(out, item)
            if (!sink.isClosed) sink.pull()
          } else if (nSinks == 0) {
            if (exhausted) completeStage()
            else pull(in)
          }

        override def onDownstreamFinish(cause: Throwable): Unit = sinks.foreach(_.cancel(cause))
      })
    }
}

object Join {
  /**
    * Creates a flow joining streams. It turns streams of streams of a given abstraction into streams of that given 
    * abstraction. Let us refer to the streams embedded in a stream as substreams. The flow takes the first N substreams
    * and join them by emitting all of their elements one by one in a FIFO fashion. When one of the substreams being 
    * joined completes, the flow takes the next substream and continues its process with that substream added in the set 
    * of substreams it joins. The flow completes when the stream completes and all of its substreams have been 
    * processed. It fails when the stream fails or one of the substreams being processed fails. The number of substreams 
    * to join at the same time is given by the parameter breadth. When that optional value is set None, there is no 
    * limit on the maximum number to process simultaneously.
    */
  def apply[T](breadth: Option[BigInt]): Flow[Source[T, NotUsed], T, NotUsed] =
    Flow.fromGraph(new Join[T](breadth))
}
