package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source}
import be.broij.akka.stream.operators.diverge.BehaviorBased.{Closed, Command, Consumer, Fail, Offer, Register, Registered, Response, Unregister, Unregistered}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object OneToOne {
  case class Pull(consumerId: BigInt, itemId: BigInt, replyTo: ActorRef[Response]) extends Command
  case object FetchItem extends Command
  case class FetchResult[T](result: Try[Option[T]]) extends Command

  abstract class Producer[T, C <: Consumer](source: Source[T, NotUsed])(implicit materializer: Materializer) {
    protected lazy val stream: SinkQueueWithCancel[T] = source.toMat(Sink.queue[T]())(Keep.right).run()
    protected lazy val memory = mutable.Map.empty[BigInt, Offer[T]]

    protected def completedBehavior(): Behavior[Command] =
      Behaviors.receive {
        case (_, Pull(consumerId, itemId, replyTo)) =>
          memory.get(consumerId).filter(_.itemId == itemId) match {
            case Some(response) =>
              replyTo ! response
              Behaviors.same
            case _ =>
              replyTo ! Closed
              Behaviors.same
          }
        case _ => Behaviors.same
      }

    protected def failedBehavior(fail: Fail): Behavior[Command] =
      Behaviors.receive {
        case (_, Pull(consumerId, itemId, replyTo)) =>
          memory.get(consumerId).filter(_.itemId == itemId) match {
            case Some(response) =>
              replyTo ! response
              Behaviors.same
            case _ =>
              replyTo ! fail
              Behaviors.same
          }
        case _ => Behaviors.same
      }

    def behavior(queueAvailable: Boolean = true, bufferizedItem: Option[T] = None): Behavior[Command] =
      Behaviors.receive {
        case (context, request @ Pull(consumerId, itemId, replyTo)) =>
          memory.get(consumerId).filter(_.itemId == itemId) match {
            case Some(response) =>
              replyTo ! response
              Behaviors.same
            case None =>
              bufferizedItem match {
                case Some(item) if isNext(consumerId) =>
                  val response = Offer(itemId, item)
                  memory += consumerId -> response
                  replyTo ! response
                  itemSent()
                  behavior(queueAvailable, None)
                case None =>
                  addRequest(request)
                  if (queueAvailable && isNext(consumerId)) {
                    context.self ! FetchItem
                    behavior(queueAvailable = false, bufferizedItem)
                  } else {
                    Behaviors.same
                  }
              }
          }
        case (context, FetchItem) =>
          context.pipeToSelf(stream.pull())(FetchResult(_))
          Behaviors.same
        case (_, FetchResult(Success(None))) =>
          requests().foreach(_.replyTo ! Closed)
          clearRequests()
          completedBehavior()
        case (context, FetchResult(Success(Some(item: T)))) =>
          nextRequest() match {
            case Some(Pull(consumerId, itemId, replyTo)) =>
              val response: Offer[T] = Offer(itemId, item)
              memory += consumerId -> response
              replyTo ! response
              itemSent()
              if (nextRequest().nonEmpty) {
                context.self ! FetchItem
                Behaviors.same
              } else {
                behavior(queueAvailable = true, bufferizedItem)
              }
            case None => behavior(queueAvailable = true, Some(item))
          }
        case (_, FetchResult(Failure(reason))) =>
          requests().foreach(_.replyTo ! Fail(reason))
          clearRequests()
          failedBehavior(Fail(reason))
        case (_, Register(consumer: C, replyTo)) =>
          register(consumer)
          replyTo ! Registered
          Behaviors.same
        case (context, Unregister(consumer: C, replyTo)) =>
          memory.remove(consumer.id)
          unregister(consumer)
          replyTo ! Unregistered
          if (nextRequest().nonEmpty && queueAvailable) {
            context.self ! FetchItem
            behavior(queueAvailable = false, bufferizedItem)
          } else {
            Behaviors.same
          }
      }

    def isNext(consumerId: BigInt): Boolean
    def itemSent(): Unit
    def addRequest(request: Pull): Unit
    def nextRequest(): Option[Pull]
    def requests(): Iterator[Pull]
    def clearRequests(): Unit
    def register(consumer: C): Unit
    def unregister(consumer: C): Unit
  }
}
