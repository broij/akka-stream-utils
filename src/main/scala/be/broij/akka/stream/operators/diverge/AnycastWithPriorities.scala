package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic}
import be.broij.akka.stream.operators.diverge.AnycastWithPriorities.{Consumer, Producer}
import be.broij.akka.stream.operators.diverge.BehaviorBased.{Request, ConsumerLogic, Response}
import be.broij.akka.stream.operators.diverge.OneToOne.Pull
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class AnycastWithPriorities[T, P: Ordering](source: Source[T, NotUsed], val restartSource: Boolean,
                                            val baseTimeoutDelay: FiniteDuration)
                                           (implicit val actorSystem: ActorSystem) extends BehaviorBased[T] {
  implicit lazy val scheduler: Scheduler = actorSystem.toTyped.scheduler
  implicit lazy val executionContext: ExecutionContext = actorSystem.dispatcher
  protected lazy val selfRef: AnycastWithPriorities[T, P] = this
  protected lazy val out: Outlet[T] = Outlet[T](s"anycastWithPriorities.out")

  override protected def producerBehavior(): Behavior[Request] = Producer(source).behavior()
  def shape: SourceShape[T] = SourceShape(out)

  def withPriority(priority: P): Source[T, NotUsed] = Source.fromGraph(new GraphStage[SourceShape[T]] {
    override def shape: SourceShape[T] = selfRef.shape

    def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new ConsumerLogic(selfRef) {
        override protected def onPullCommand(replyTo: ActorRef[Response]): Request =
          Pull(consumerId, nextItemId, replyTo)
        override def consumer(): BehaviorBased.Consumer = Consumer(consumerId, priority)
      }
  })
}

object AnycastWithPriorities {
  case class Consumer[P: Ordering](id: BigInt, priority: P) extends BehaviorBased.Consumer

  class Producer[T, P: Ordering](source: Source[T, NotUsed])(implicit materializer: Materializer)
      extends OneToOne.Producer[T, Consumer[P]](source) {
    protected lazy val prioritiesMap = mutable.Map.empty[BigInt, P]
    protected var requestsMap: mutable.Map[(BigInt, P), Pull] = mutable.SortedMap.empty(
      Ordering.by(_._2)
    )

    override def isNext(consumerId: BigInt): Boolean = requestsMap.headOption.map(_._1._1).contains(consumerId)
    override def itemSent(): Unit = requestsMap = requestsMap.tail
    override def addRequest(request: Pull): Unit = prioritiesMap.get(request.consumerId).foreach {
      priority => requestsMap += (request.consumerId, priority) -> request
    }

    override def nextRequest(): Option[Pull] = requestsMap.headOption.map(_._2)
    override def requests(): Iterator[Pull] = requestsMap.valuesIterator
    override def clearRequests(): Unit = requestsMap.clear()
    override def register(consumer: Consumer[P]): Unit = prioritiesMap += consumer.id -> consumer.priority
    override def unregister(consumer: Consumer[P]): Unit = prioritiesMap.get(consumer.id).foreach {
      priority =>
        prioritiesMap -= consumer.id
        requestsMap -= ((consumer.id, priority))
    }
  }

  object Producer {
    def apply[T, P: Ordering](source: Source[T, NotUsed])(implicit materializer: Materializer) =
      new Producer(source)
  }

  /**
    * Creates an object allowing to distribute the elements of a given source to several consumers. The object enable to
    * create sources whose materializations are consumers registered to the same producer. The producer emits the 
    * elements of the source it wraps one after the others. These sources can be created via the method 
    * [[AnycastWithPriorities.withPriority]]. Each consumer is bound to the priority that is given when the source it 
    * materializes is created. Each element is sent to one of the consumers, taking the first one available or the one 
    * with the highest priority when several are available. The created sources complete when the producer completes. 
    * They fail when the producer or any of its consumers fail. The job of the producer is to emit the elements of the 
    * wrapped source to the registered consumers. It manages a dynamic group of consumers that grows or shrinks as 
    * consumers register and unregister. A special flag called restartSource allows to specify how the producer should 
    * react when there is no more consumers. When the flag is set to true, the producer will stop the wrapped source and 
    * restart it from the beginning when some new consumer register. When set to false, it will let the wrapped source 
    * continue to execute. If an element sent to a consumer isn't ackowledged to the producer before a FiniteDuration 
    * defined by baseTimeoutDelay, the element is sent again to that consumer. This duration is increased exponentially 
    * by a power of two each time the same element is sent again to the same consumer. Note that this mecanism is 
    * completely transparent for the final user: nothing more than providing that baseTimeoutDelay is expected to be 
    * done by the user; when an element is received several time by the same consumer due to retransmissions, it will 
    * appear only once in the corresponding stream.
    */
  def apply[T, P: Ordering](source: Source[T, NotUsed], restartSource: Boolean, baseTimeoutDelay: FiniteDuration)
                           (implicit actorSystem: ActorSystem) =
    new AnycastWithPriorities(source, restartSource, baseTimeoutDelay)
}