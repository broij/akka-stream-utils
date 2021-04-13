package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet, SourceShape}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class JoinWithPriorities[T, P: Ordering](priorityOf: T => P, breadth: Option[BigInt])
    extends GraphStage[FlowShape[Graph[SourceShape[T], NotUsed], T]] {
  protected val in: Inlet[Graph[SourceShape[T], NotUsed]] =
    Inlet[Graph[SourceShape[T], NotUsed]]("joinPriorities.in")
  protected val out: Outlet[T] = Outlet[T]("joinPriorities.out")

  override def shape: FlowShape[Graph[SourceShape[T], NotUsed], T] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {
      val buffer: mutable.PriorityQueue[(T, SubSinkInlet[T])] = mutable.PriorityQueue.empty(Ordering.by(priorityOfItem))
      val sinks: ListBuffer[SubSinkInlet[T]] = ListBuffer.empty
      var nSinks = 0
      var exhausted = false

      protected def priorityOfItem(bufferItem: (T, SubSinkInlet[T])): P = priorityOf(bufferItem._1)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val source = grab(in)
          val sink = new SubSinkInlet[T]("joinPriorities.subSink")
          sink.setHandler(new InHandler {
            override def onPush(): Unit =
              if (isAvailable(out)) {
                push(out,sink.grab())
                sink.pull()
              } else buffer += ((sink.grab(), sink))

            override def onUpstreamFinish(): Unit = {
              nSinks -= 1
              sinks -= sink
              if (exhausted && nSinks == 0 && buffer.isEmpty) completeStage()
            }
          })

          subFusingMaterializer.materialize(
            Source.fromGraph(source).to(sink.sink),
            defaultAttributes = inheritedAttributes
          )

          sink.pull()
          nSinks += 1
          sinks += sink
          if (breadth.forall(_ > nSinks)) pull(in)
        }

        override def onUpstreamFinish(): Unit = exhausted = true

        override def onUpstreamFailure(throwable: Throwable): Unit = {
          sinks.foreach(_.cancel(throwable))
          failStage(throwable)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (buffer.nonEmpty) {
            val (item, sink) = buffer.dequeue()
            push(out, item)
            if (sink.isClosed) {
              if (!exhausted) pull(in)
            } else sink.pull()
          } else if (nSinks == 0) {
            if (exhausted) completeStage()
            else pull(in)
          }

        override def onDownstreamFinish(throwable: Throwable): Unit = {
          sinks.foreach(_.cancel(throwable))
          failStage(throwable)
        }
      })
    }
  }
}

object JoinWithPriorities {
  /**
    * Creates a flow joining streams; using a priority based policy when several elements are available at the same 
    * time. It turns streams of streams of a given abstraction into streams of that given abstraction. Let us refer to 
    * the streams embedded in a stream as substreams. The flow takes the first N substreams and join them by emitting 
    * all of their elements. The function priorityOf is used to attribute a priority to each elements and when several 
    * are available for emission, the one with the highest priority is emitted. When one of the substreams being joined 
    * completes, the flow takes the next substream and continues its process with that substream added in the set of 
    * substreams it joins. The flow completes when the stream completes and all of its substreams have been processed. 
    * It fails when the stream fails or one of the substreams being processed fails. The number of substreams to join at 
    * the same time is given by the parameter breadth. When that optional value is set None, there is no limit on the 
    * maximum number to process simultaneously.
    */
  def apply[T, P: Ordering](priorityOf: T => P, breadth: Option[BigInt]): Flow[Source[T, NotUsed], T, NotUsed] =
    Flow.fromGraph(new JoinWithPriorities[T, P](priorityOf, breadth))
}
