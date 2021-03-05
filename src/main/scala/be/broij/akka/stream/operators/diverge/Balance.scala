package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic}
import be.broij.akka.stream.operators.diverge.Balance.Producer
import be.broij.akka.stream.operators.diverge.BehaviorBased.{BaseConsumer, Request, ConsumerLogic, Response}
import be.broij.akka.stream.operators.diverge.OneToOne.Pull
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.math.BigInt.int2bigInt

class Balance[T](source: Source[T, NotUsed], n: BigInt, val restartSource: Boolean,
                 val baseTimeoutDelay: FiniteDuration)
                (implicit val actorSystem: ActorSystem) extends GraphStage[SourceShape[T]] with BehaviorBased[T] {
  implicit lazy val scheduler: Scheduler = actorSystem.toTyped.scheduler
  implicit lazy val executionContext: ExecutionContext = actorSystem.dispatcher
  lazy val out: Outlet[T] = Outlet[T](s"balance$n.out")

  override protected def producerBehavior(): Behavior[Request] = Producer(source, n).behavior()
  def shape: SourceShape[T] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ConsumerLogic(this) {
      override protected def onPullCommand(replyTo: ActorRef[Response]): Request = Pull(consumerId, nextItemId, replyTo)
      override def consumer(): BehaviorBased.Consumer = BaseConsumer(consumerId)
    }
}

object Balance {
  class Producer[T](source: Source[T, NotUsed], n: BigInt)(implicit materializer: Materializer)
      extends OneToOne.Producer[T, BaseConsumer](source) {
    protected lazy val requestsMap = mutable.Map.empty[BigInt, Pull]
    protected lazy val consumingOrder: ArrayBuffer[BigInt] = mutable.ArrayBuffer.empty[BigInt]

    override def itemSent(): Unit = {
      val consumerId = consumingOrder.remove(0)
      consumingOrder += consumerId
      requestsMap -= consumerId
    }
    override def addRequest(request: Pull): Unit = requestsMap += request.consumerId -> request
    override def nextRequest(): Option[Pull] = consumingOrder.headOption.flatMap(requestsMap.get)
    override def requests(): Iterator[Pull] = requestsMap.valuesIterator
    override def clearRequests(): Unit = requestsMap.clear()
    override def register(consumer: BaseConsumer): Unit = for (_ <- 0 until n) consumingOrder += consumer.id
    override def unregister(consumer: BaseConsumer): Unit = {
      requestsMap -= consumer.id
      consumingOrder --= consumingOrder.filter(_ == consumer.id)
    }
  }

  object Producer {
    def apply[T](source: Source[T, NotUsed], n: BigInt)(implicit materializer: Materializer) =
      new Producer(source, n)
  }

  /**
    * Creates a source allowing to balance the elements of a given source to several consumers. Its materializations are 
    * consumers registered to the same producer. The producer emits the elements of the given source it wraps one after 
    * the others. Each element is sent to one of its consumers. The n first elements are sent to the first consumer, 
    * then the n next elements are sent to the second consumer and so on so forth in a circular fashion. The constant n 
    * is a parameter giving the amount of elements to send to an individual consumer before switching to the next. The 
    * source completes when the producer completes. It fails when the producer fails. The job of the producer is to emit
    * the elements of the wrapped source to the registered consumers. It manages a dynamic group of consumers that grows
    * or shrinks as consumers register and unregister. A special flag called restartSource allows to specify how the
    * producer should react when there is no more consumers. When the flag is set to true, the producer will stop the
    * wrapped source and restart it from the beginning when some new consumer register. When set to false, it will let
    * the wrapped source continue to execute. If an element sent to a consumer isn't acknowledged to the producer before
    * a FiniteDuration defined by baseTimeoutDelay, the element is sent again to that consumer. This duration is
    * increased exponentially by a power of two each time the same element is sent again to the same consumer. Note that
    * this mechanism is completely transparent for the final user: nothing more than providing that baseTimeoutDelay is
    * expected to be done by the user; when an element is received several times by the same consumer due to
    * retransmissions, it will appear only once in the corresponding stream.
    */
  def apply[T](source: Source[T, NotUsed], n: BigInt, restartSource: Boolean, baseTimeoutDelay: FiniteDuration)
              (implicit actorSystem: ActorSystem): Source[T, NotUsed] =
    Source.fromGraph(new Balance(source, n, restartSource, baseTimeoutDelay))
}

