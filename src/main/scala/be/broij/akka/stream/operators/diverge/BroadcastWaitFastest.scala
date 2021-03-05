package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import be.broij.akka.stream.operators.diverge.BehaviorBased.{BaseConsumer, Request, ConsumerLogic, Register, Registered, Response, Unregister, Unregistered}
import be.broij.akka.stream.operators.diverge.Broadcast.forwardItemTo
import be.broij.akka.stream.operators.diverge.BroadcastWaitFastest.{Producer, Pull}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class BroadcastWaitFastest[T](source: Source[T, NotUsed], val restartSource: Boolean, bufferSize: Int,
                              val baseTimeoutDelay: FiniteDuration)
                             (implicit val actorSystem: ActorSystem)
    extends GraphStage[SourceShape[T]] with BehaviorBased[T] {
  implicit lazy val scheduler: Scheduler = actorSystem.toTyped.scheduler
  implicit lazy val executionContext: ExecutionContext = actorSystem.dispatcher
  protected lazy val out: Outlet[T] = Outlet[T]("broadcastWaitFastest.out")

  override protected def producerBehavior(): Behavior[Request] = Producer(source, bufferSize).behavior()
  def shape: SourceShape[T] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ConsumerLogic(this) {
      override protected def onPullCommand(replyTo: ActorRef[Response]): Request = Pull(nextItemId, replyTo)
      override def consumer(): BehaviorBased.Consumer = BaseConsumer(consumerId)
    }
}

object BroadcastWaitFastest {
  case class Pull(itemId: BigInt, replyTo: ActorRef[Response]) extends Request

  class Producer[T](source: Source[T, NotUsed], bufferSize: Int)(implicit materializer: Materializer) {
    protected lazy val stream: SinkQueueWithCancel[T] = source.toMat(Sink.queue[T]())(Keep.right).run()
    protected lazy val buffer = Array.ofDim[Future[Option[T]]](bufferSize)

    protected def fillBufferUntil(requestId: BigInt, latestItemId: BigInt): BigInt =
      if (latestItemId >= requestId) {
        if (requestId < latestItemId - bufferSize + 1) latestItemId - bufferSize + 1 else requestId
      } else {
        val tmp = requestId - bufferSize + 1
        val start = if (latestItemId < tmp) tmp else latestItemId + 1
        for (i <- start to requestId) {
          val item = stream.pull()
          buffer((i % bufferSize).toInt) = item
        }
        requestId
      }

    def behavior(latestItemId: BigInt = -1): Behavior[Request] =
      Behaviors.receive {
        case (context, Pull(requestId, replyTo)) =>
          val itemId = fillBufferUntil(requestId, latestItemId)
          forwardItemTo(buffer, bufferSize, itemId, replyTo)(context.executionContext)
          if (itemId > latestItemId) behavior(itemId)
          else Behaviors.same
        case (_, Register(_, replyTo)) =>
          replyTo ! Registered
          Behaviors.same
        case (_, Unregister(_, replyTo)) =>
          replyTo ! Unregistered
          Behaviors.same
      }
  }

  object Producer {
    def apply[T](source: Source[T, NotUsed], bufferSize: Int)(implicit materializer: Materializer) =
      new Producer(source, bufferSize)
  }

  /**
    * Creates a source allowing to broadcast the elements of a given source to several consumers. Its materializations 
    * are consumers registered to the same producer. The producer emits the elements of the given source it wraps one 
    * after the others. Each element is sent to all of its consumers. A buffer allows the fastest consumers to advance 
    * without having to wait for the slowest ones to consume the current element. The size of the buffer is finite and 
    * defined with the bufferSize parameter. When the buffer is full, the producer will follow the pace of the fastest
    * consumers (which means that the slowest consumers may skip some elements). The source completes when the producer 
    * completes. It fails when the producer fails. The job of the producer is to emit the elements of the wrapped source to
    * the registered consumers. It manages a dynamic group of consumers that grows or shrinks as consumers register and
    * unregister. A special flag called restartSource allows to specify how the producer should react when there is no
    * more consumers. When the flag is set to true, the producer will stop the wrapped source and restart it from the
    * beginning when some new consumer register. When set to false, it will let the wrapped source continue to execute.
    * If an element sent to a consumer isn't acknowledged to the producer before a FiniteDuration defined by
    * baseTimeoutDelay, the element is sent again to that consumer. This duration is increased exponentially by a power
    * of two each time the same element is sent again to the same consumer. Note that this mechanism is completely
    * transparent for the final user: nothing more than providing that baseTimeoutDelay is expected to be done by the
    * user; when an element is received several times by the same consumer due to retransmissions, it will appear only
    * once in the corresponding stream.
    */
  def apply[T](source: Source[T, NotUsed], restartSource: Boolean, bufferSize: Int, baseTimeoutDelay: FiniteDuration)
              (implicit actorSystem: ActorSystem): Source[T, NotUsed] =
    Source.fromGraph(new BroadcastWaitFastest(source, restartSource, bufferSize, baseTimeoutDelay))
}
