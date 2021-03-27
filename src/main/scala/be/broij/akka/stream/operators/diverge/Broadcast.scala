package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.ActorRef
import akka.stream.scaladsl.Source
import be.broij.akka.stream.operators.diverge.BehaviorBased.{Completed, Failed, Offer, Response}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Broadcast {
  protected[diverge] def forwardItemTo[T](buffer: Array[Future[Option[T]]], bufferSize: Int, itemId: BigInt,
                                          destination: ActorRef[Response])
                                         (implicit executionContext: ExecutionContext): Unit = {
    buffer((itemId % bufferSize).toInt).onComplete {
      case Success(Some(item)) => destination ! Offer(itemId, item)
      case Success(None) => destination ! Completed
      case Failure(reason) => destination ! Failed(reason)
    }
  }

  /**
    * Creates a source allowing to broadcast the elements of a given source to several consumers. Its materializations 
    * are consumers registered to the same producer. The producer emits the elements of the given source it wraps one 
    * after the others. Each element is sent to all of its consumers. A buffer allows the fastest consumers to advance 
    * without having to wait for the slowest ones to consume the current element. The size of the buffer is finite and 
    * defined with the bufferSize parameter. Two behaviors are available when the buffer is full: wait the slowest 
    * consumers when waitSlowest is set to true or follow the pace of the fastest consumers (which means that the 
    * slowest consumers may skip some elements) when waitSlowest is set to false. The source completes when the producer 
    * completes. It fails when the producer. The job of the producer is to emit the elements of the wrapped source to
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
  def apply[T](source: Source[T, NotUsed], restartSource: Boolean, waitSlowest: Boolean, bufferSize: Int,
               baseTimeoutDelay: FiniteDuration)
              (implicit actorSystem: ActorSystem): Source[T, NotUsed] = {
    if (waitSlowest) BroadcastWaitSlowest(source, restartSource, bufferSize, baseTimeoutDelay)
    else BroadcastWaitFastest(source, restartSource, bufferSize, baseTimeoutDelay)
  }
}
