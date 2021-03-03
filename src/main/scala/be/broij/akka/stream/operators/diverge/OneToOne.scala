package be.broij.akka.stream.operators.diverge

import akka.NotUsed
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, Source}
import be.broij.akka.stream.operators.diverge.BehaviorBased.{Closed, Request, Consumer, Fail, Offer, Register, Registered, Response, Unregister, Unregistered}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object OneToOne {
  case class Pull(consumerId: BigInt, itemId: BigInt, replyTo: ActorRef[Response]) extends Request
  case object FetchItem extends Request
  case class FetchResult[T](result: Try[Option[T]]) extends Request

  abstract class Producer[T, C <: Consumer](source: Source[T, NotUsed])(implicit materializer: Materializer) {
    protected lazy val stream: SinkQueueWithCancel[T] = source.toMat(Sink.queue[T]())(Keep.right).run()
    protected lazy val memory = mutable.Map.empty[BigInt, Offer[T]]

    protected def completedBehavior(): Behavior[Request] =
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
        case (_, Register(_, replyTo)) =>
          replyTo ! Closed
          Behaviors.same
        case (_, Unregister(_, replyTo)) =>
          replyTo ! Unregistered
          Behaviors.same
        case _ => Behaviors.same
      }

    protected def failedBehavior(fail: Fail): Behavior[Request] =
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
        case (_, Register(_, replyTo)) =>
          replyTo ! fail
          Behaviors.same
        case (_, Unregister(_, replyTo)) =>
          replyTo ! fail
          Behaviors.same
        case _ => Behaviors.same
      }

    def behavior(queueAvailable: Boolean = true, bufferizedItem: Option[T] = None): Behavior[Request] =
      Behaviors.receive {
        case (context, request @ Pull(consumerId, itemId, replyTo)) =>
          memory.get(consumerId).filter(_.itemId == itemId) match {
            case Some(response) =>
              replyTo ! response
              Behaviors.same
            case None =>
              addRequest(request)
              nextRequest() match {
                case Some(Pull(consumerId, itemId, replyTo)) =>
                  bufferizedItem match {
                    case Some(item) =>
                      val response = Offer(itemId, item)
                      memory += consumerId -> response
                      replyTo ! response
                      itemSent()
                      nextRequest() match {
                        case Some(_) =>
                          context.self ! FetchItem
                          behavior(queueAvailable = false, None)
                        case None =>
                          behavior(queueAvailable, None)
                      }
                    case None if queueAvailable =>
                      context.self ! FetchItem
                      behavior(queueAvailable = false, bufferizedItem)
                    case _ => Behaviors.same
                  }
                case None =>  Behaviors.same
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

    def itemSent(): Unit
    def addRequest(request: Pull): Unit
    def nextRequest(): Option[Pull]
    def requests(): Iterator[Pull]
    def clearRequests(): Unit
    def register(consumer: C): Unit
    def unregister(consumer: C): Unit
  }
}
