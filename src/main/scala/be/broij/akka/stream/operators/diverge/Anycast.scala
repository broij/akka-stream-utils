package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.stream.scaladsl.Source
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import be.broij.akka.stream.operators.diverge.Anycast.Producer
import be.broij.akka.stream.operators.diverge.BehaviorBased.{BaseConsumer, Request, ConsumerLogic, Response}
import be.broij.akka.stream.operators.diverge.OneToOne.Pull
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class Anycast[T](source: Source[T, NotUsed], val restartSource: Boolean, val baseTimeoutDelay: FiniteDuration)
                (implicit val actorSystem: ActorSystem) extends GraphStage[SourceShape[T]] with BehaviorBased[T] {
  implicit lazy val scheduler: Scheduler = actorSystem.toTyped.scheduler
  implicit lazy val executionContext: ExecutionContext = actorSystem.dispatcher
  protected lazy val out: Outlet[T] = Outlet[T]("anycast.out")

  override protected def producerBehavior(): Behavior[Request] = Producer(source).behavior()
  def shape: SourceShape[T] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ConsumerLogic(this) {
      override protected def onPullCommand(replyTo: ActorRef[Response]): Request = Pull(consumerId, nextItemId, replyTo)
      override def consumer(): BehaviorBased.Consumer = BaseConsumer(consumerId)
    }
}

object Anycast {
   class Producer[T](source: Source[T, NotUsed])(implicit materializer: Materializer)
      extends OneToOne.Producer[T, BaseConsumer](source) {
     protected var requestsMap = mutable.Map.empty[BigInt, Pull]

     override def itemSent(): Unit = requestsMap = requestsMap.tail
     override def addRequest(request: Pull): Unit = requestsMap += request.consumerId -> request
     override def nextRequest(): Option[Pull] = requestsMap.headOption.map(_._2)
     override def requests(): Iterator[Pull] = requestsMap.values.iterator
     override def clearRequests(): Unit = requestsMap.clear()
     override def register(consumer: BaseConsumer): Unit = {}
     override def unregister(consumer: BaseConsumer): Unit = requestsMap -= consumer.id
  }

  object Producer {
    def apply[T](source: Source[T, NotUsed])(implicit materializer: Materializer) =
      new Producer(source)
  }

  /**
    * Creates a source allowing to distribute the elements of a given source to several consumers. Its materializations 
    * are consumers registered to the same producer. The producer emits the elements of the given source it wraps one 
    * after the others. Each element is sent to one of the consumers, taking the first one available or using a FIFO 
    * policy when several consumers are available. The source completes when the producer completes. It fails when the 
    * producer fails. The job of the producer is to emit the elements of the wrapped source to the registered consumers.
    * It manages a dynamic group of consumers that grows or shrinks as consumers register and unregister. A special flag
    * called restartSource allows to specify how the producer should react when there is no more consumers. When the
    * flag is set to true, the producer will stop the wrapped source and restart it from the beginning when some new
    * consumer register. When set to false, it will let the wrapped source continue to execute. If an element sent to a
    * consumer isn't acknowledged to the producer before a FiniteDuration defined by baseTimeoutDelay, the element is
    * sent again to that consumer. This duration is increased exponentially by a power of two each time the same element
    * is sent again to the same consumer. Note that this mechanism is completely transparent for the final user: nothing
    * more than providing that baseTimeoutDelay is expected to be done by the user; when an element is received several
    * times by the same consumer due to retransmissions, it will appear only once in the corresponding stream.
    */
  def apply[T](source: Source[T, NotUsed], restartSource: Boolean, baseTimeoutDelay: FiniteDuration)
              (implicit actorSystem: ActorSystem): Source[T, NotUsed] =
    Source.fromGraph(new Anycast(source, restartSource, baseTimeoutDelay))
}