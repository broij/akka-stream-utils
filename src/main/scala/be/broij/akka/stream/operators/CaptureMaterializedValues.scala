package be.broij.akka.stream.operators

import akka.NotUsed
import akka.stream.QueueOfferResult.QueueClosed
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet, OverflowStrategy, QueueOfferResult}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import scala.util.{Failure, Success, Try}

class CaptureMaterializedValues[T, M](implicit mat: Materializer)
    extends GraphStageWithMaterializedValue[FlowShape[Source[T, M], Source[T, NotUsed]], Source[M, NotUsed]] {
  protected val in: Inlet[Source[T, M]] = Inlet[Source[T, M]]("captureMaterializedValues.in")
  protected val out: Outlet[Source[T, NotUsed]] = Outlet[Source[T, NotUsed]]("captureMaterializedValues.out")

  override def shape: FlowShape[Source[T, M], Source[T, NotUsed]] = FlowShape(in, out)

  override def createLogicAndMaterializedValue(
    inheritedAttributes: Attributes
  ): (GraphStageLogic, Source[M, NotUsed]) = {
    val logic = new GraphStageLogic(shape) {
      val (queue, matVals) = Source.queue[M](0, OverflowStrategy.backpressure).preMaterialize()(mat)
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val (matVal, source) = grab(in).preMaterialize()(mat)
          queue.offer(matVal).onComplete {
            getAsyncCallback[Try[QueueOfferResult]] {
              case Success(QueueClosed) | Success(QueueOfferResult.Failure(_)) | Failure(_) =>
                setHandler(in, new InHandler {
                  override def onPush(): Unit = push(out, grab(in).preMaterialize()(mat)._2)
                })
                push(out, source)
              case _ => push(out, source)
            }.invoke
          }(subFusingMaterializer.executionContext)
        }

        override def onUpstreamFinish(): Unit = {
          queue.complete()
          completeStage()
        }

        override def onUpstreamFailure(throwable: Throwable): Unit = {
          queue.fail(throwable)
          failStage(throwable)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
    (logic, logic.matVals)
  }
}

object CaptureMaterializedValues {
  /**
    * Creates a flow working with streams of streams. Let us refer to the streams embedded in a stream as substreams. 
    * The flow erases the materialized value of each substream. Its materialized value is another stream where the nth 
    * element gives the materialized value of the nth substream. The materializer is used to pre-materialize each 
    * substream in order to access their own materialized value.
    */
  def apply[T, M](implicit materializer: Materializer): Flow[Source[T, M], Source[T, NotUsed], Source[M, NotUsed]] =
    Flow.fromGraph(new CaptureMaterializedValues[T, M])
}
