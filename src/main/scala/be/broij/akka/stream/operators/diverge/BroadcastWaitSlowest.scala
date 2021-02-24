package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import be.broij.akka.stream.operators.diverge.BehaviorBased.{BaseConsumer, Closed, Request, ConsumerLogic, Fail, Offer, Register, Registered, Response, Unregister, Unregistered}
import be.broij.akka.stream.operators.diverge.BroadcastWaitSlowest.{Producer, Pull}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.math.BigInt.int2bigInt
import scala.util.{Failure, Success}

class BroadcastWaitSlowest[T](source: Source[T, NotUsed], val restartSource: Boolean, bufferSize: Int,
                              val baseTimeoutDelay: FiniteDuration)
                             (implicit val actorSystem: ActorSystem)
    extends GraphStage[SourceShape[T]] with BehaviorBased[T] {
  implicit lazy val scheduler: Scheduler = actorSystem.toTyped.scheduler
  implicit lazy val executionContext: ExecutionContext = actorSystem.dispatcher
  protected lazy val out: Outlet[T] = Outlet[T]("broadcastWaitSlowest.out")

  override protected def producerBehavior(): Behavior[Request] = Producer(source, bufferSize).behavior()
  def shape: SourceShape[T] = SourceShape(out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ConsumerLogic(this) {
      override protected def onPullCommand(replyTo: ActorRef[Response]): Request = Pull(consumerId, nextItemId, replyTo)
      override def consumer(): BehaviorBased.Consumer = BaseConsumer(consumerId)
    }
}

object BroadcastWaitSlowest {
  case class Pull(consumerId: BigInt, itemId: BigInt, replyTo: ActorRef[Response]) extends Request

  class Producer[T](source: Source[T, NotUsed], bufferSize: Int)(implicit materializer: Materializer) {
    protected lazy val stream: SinkQueueWithCancel[T] = source.toMat(Sink.queue[T]())(Keep.right).run()
    protected lazy val registeredConsumers = mutable.Set.empty[BigInt]
    protected lazy val unregisteredConsumers = mutable.Set.empty[BigInt]
    protected lazy val buffer = Array.ofDim[Future[Option[T]]](bufferSize)
    protected lazy val pullsMap = mutable.Map.empty[BigInt, Pull]
    protected lazy val pendingPulls: mutable.SortedSet[Pull] = mutable.SortedSet.empty[Pull](Ordering.fromLessThan {
      case (Pull(consumerId0, itemId0, _), Pull(consumerId1, itemId1, _)) =>
        itemId0 < itemId1 || (itemId0 == itemId1 && consumerId0 != null && consumerId1 != null &&
          consumerId0 < consumerId1)
    })
    protected var nPendingPulls: BigInt = 0
    protected lazy val commitMap = mutable.Map.empty[BigInt, BigInt]
    protected lazy val commitLog: mutable.SortedSet[(BigInt, BigInt)] =
      mutable.SortedSet.empty[(BigInt, BigInt)](Ordering.fromLessThan {
        case ((consumerId0, itemId0), (consumerId1, itemId1)) =>
          itemId0 < itemId1 || (itemId0 == itemId1 && consumerId0 < consumerId1)
      })

    protected def nearestItem(itemId: BigInt, latestItemId: BigInt): BigInt =
      if (latestItemId >= itemId) {
        if (itemId < latestItemId - bufferSize + 1) latestItemId - bufferSize + 1 else itemId
      } else {
        itemId
      }

    protected def updateCommitLog(itemId: BigInt, consumerId: BigInt): Unit = {
      commitMap.get(consumerId).map((consumerId, _)).foreach { commitLog -= _ }
      commitMap += consumerId -> itemId
      commitLog.update((consumerId, itemId), included = true)
    }

    protected def removeCommits(consumerId: BigInt): Unit = {
      commitMap.get(consumerId).map((consumerId, _)).foreach { commitLog -= _ }
      commitMap -= consumerId
    }

    protected def updatePendingPulls(pull: Pull): Unit = {
      pullsMap.get(pull.consumerId).foreach { k =>
        pendingPulls -= k
        nPendingPulls -= 1
      }
      pullsMap += pull.consumerId -> pull
      pendingPulls += pull
      nPendingPulls += 1
    }

    protected def removePendingPull(consumerId: BigInt): Unit = {
      pullsMap.get(consumerId).foreach { k =>
        pendingPulls -= k
        nPendingPulls -= 1
      }
      pullsMap -= consumerId
    }

    protected def resolvePendingPulls(latestItemId: BigInt)(implicit executionContext: ExecutionContext): Unit = {
      val completedPulls = pendingPulls.to(Pull(null, latestItemId, null))
      completedPulls.foreach {
        case Pull(consumerId, itemId, replyTo) => buffer((itemId % bufferSize).toInt).onComplete {
          case Success(Some(item)) => replyTo ! Offer(itemId, item)
          case Success(None) => replyTo ! Closed
          case Failure(reason) => replyTo ! Fail(reason)
        }
        nPendingPulls -= 1
        pullsMap -= consumerId
      }
      pendingPulls --= completedPulls
    }

    protected def fillItemsBuffer(latestItemId: BigInt): BigInt = {
      val freeSpace = bufferSize - (latestItemId - commitLog.head._2)
      val fillingPulls = pendingPulls.filter(_.itemId > latestItemId)
      val toFetch = freeSpace.min(fillingPulls.size)
      if (toFetch > 0) {
        val firstItem = fillingPulls.head.itemId
        for (i <- 0 until toFetch)
          buffer(((firstItem + i) % bufferSize).toInt) = stream.pull()
      }
      latestItemId + toFetch
    }

    def behavior(consumerCount: BigInt = 0, latestItemId: BigInt = -1): Behavior[Request] =
      Behaviors.receive {
        case (context, Pull(consumerId, requestId, replyTo))
            if registeredConsumers.contains(consumerId) && !unregisteredConsumers.contains(consumerId) =>
          val itemId = nearestItem(requestId, latestItemId)
          updateCommitLog(itemId - 1, consumerId)
          updatePendingPulls(Pull(consumerId, itemId, replyTo))
          val updatedLatestItemId = fillItemsBuffer(latestItemId)
          resolvePendingPulls(updatedLatestItemId)(context.executionContext)
          behavior(consumerCount, updatedLatestItemId)
        case (_, Register(BaseConsumer(consumerId), replyTo)) =>
          if (unregisteredConsumers.contains(consumerId)) {
            replyTo ! Unregistered
            Behaviors.same
          } else if (registeredConsumers.contains(consumerId)) {
            replyTo ! Registered
            Behaviors.same
          } else {
            replyTo ! Registered
            registeredConsumers += consumerId
            updateCommitLog(-1, consumerId)
            behavior(consumerCount + 1, latestItemId)
          }
        case (_, Unregister(BaseConsumer(consumerId), replyTo)) =>
          if (unregisteredConsumers.contains(consumerId)) {
            replyTo ! Unregistered
            Behaviors.same
          } else {
            replyTo ! Unregistered
            registeredConsumers -= consumerId
            unregisteredConsumers += consumerId
            removeCommits(consumerId)
            removePendingPull(consumerId)
            behavior(consumerCount - 1, latestItemId)
          }
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
    * defined with the bufferSize parameter. When the buffer is full, the producer will wait the slowest consumers 
    * (which means that the fastest consumers may have to wait for the slowest ones). The source completes when the 
    * producer completes. It fails when the producer or any of its consumers fails. The job of the producer is to emit 
    * the elements of the wrapped source to the registered consumers. It manages a dynamic group of consumers that grows 
    * or shrinks as consumers register and unregister. A special flag called restartSource allows to specify how the 
    * producer should react when there is no more consumers. When the flag is set to true, the producer will stop the 
    * wrapped source and restart it from the beginning when some new consumer register. When set to false, it will let 
    * the wrapped source continue to execute. If an element sent to a consumer isn't ackowledged to the producer before 
    * a FiniteDuration defined by baseTimeoutDelay, the element is sent again to that consumer. This duration is 
    * increased exponentially by a power of two each time the same element is sent again to the same consumer. Note that 
    * this mecanism is completely transparent for the final user: nothing more than providing that baseTimeoutDelay is 
    * expected to be done by the user; when an element is received several time by the same consumer due to 
    * retransmissions, it will appear only once in the corresponding stream. 
    */
  def apply[T](source: Source[T, NotUsed], restartSource: Boolean, bufferSize: Int, baseTimeoutDelay: FiniteDuration)
              (implicit actorSystem: ActorSystem): Source[T, NotUsed] =
    Source.fromGraph(new BroadcastWaitSlowest(source, restartSource, bufferSize, baseTimeoutDelay))
}
