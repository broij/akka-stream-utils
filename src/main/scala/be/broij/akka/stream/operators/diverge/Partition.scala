package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import scala.concurrent.duration.FiniteDuration

object Partition {
  /**
    * Creates a function allowing to distribute the elements of a given source to several consumers. The object enables 
    * to create sources whose materializations are consumers registered to the same producer. The producer emits the 
    * elements of the source it wraps one after the others. Each consumer is bound to a partition P that is given when 
    * the source it materializes is created. Each element is attributed to a given partition using a partitioning 
    * function, partitionOf. Each consumer subscribes to the partition it is bound to and receives all the elements 
    * attributed to this partition. A buffer allows the fastest consumers to advance without having to wait for the 
    * slowest ones to consume the current element. This buffer is shared by all consumers no matter the partition they 
    * are subscribed to. The size of the buffer is finite and provided by the user with the bufferSize parameter. Two 
    * behaviors are available when the buffer is full: wait the slowest consumers when waitSlowest is set to true or 
    * follow the pace of the fastest consumers (which means that the slowest consumers may skip some elements) when 
    * waitSlowest is set to false. The created sources complete when the producer completes. They fail when the
    * producer fails. The job of the producer is to emit the elements of the wrapped source to the registered consumers.
    * It manages a dynamic group of consumers that grows or shrinks as consumers register and unregister. A special
    * flag, restartSource, allows to specify how the producer should react when there is no more consumers. When the
    * flag is set to true, the producer will stop the wrapped source and restart it from the beginning when some new
    * consumer register. When set to false, it will let the wrapped source continue to execute. If an element sent to a
    * consumer isn't acknowledged to the producer before a FiniteDuration defined by baseTimeoutDelay, the element is
    * sent again to that consumer. This duration is increased exponentially by a power of two each time the same element
    * is sent again to the same consumer. Note that this mechanism is completely transparent for the final user: nothing
    * more than providing that baseTimeoutDelay is expected to be done by the user; when an element is received several
    * times by the same consumer due to retransmissions, it will appear only once in the corresponding stream.
    */
  def apply[T, P](source: Source[T, NotUsed], partitionOf: T => P, restartSource: Boolean, waitSlowest: Boolean,
                  bufferSize: Int, baseTimeoutDelay: FiniteDuration)
                 (implicit actorSystem: ActorSystem): P => Source[T, NotUsed] = {
    val broadcast = Broadcast(source, restartSource, waitSlowest, bufferSize, baseTimeoutDelay)
    def bind(partition: P): Source[T, NotUsed] = broadcast.filter(partitionOf(_) == partition)
    bind
  }
}
