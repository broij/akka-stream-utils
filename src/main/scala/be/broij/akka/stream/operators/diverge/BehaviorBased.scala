package be.broij.akka.stream.operators.diverge

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.typed.scaladsl.adapter.{ClassicActorSystemOps, TypedActorRefOps}
import akka.stream.{Outlet, SourceShape}
import akka.stream.stage.{GraphStageLogic, OutHandler}
import akka.util.Timeout
import be.broij.akka.stream.operators.diverge.BehaviorBased.{Closed, ConsumerLogic, Fail, Register, Registered, Request, Response, Unregister, Unregistered}
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

trait BehaviorBased[T] {
  implicit val scheduler: Scheduler
  implicit val executionContext: ExecutionContext
  implicit val actorSystem: ActorSystem
  val restartSource: Boolean
  val baseTimeoutDelay: FiniteDuration
  protected val out: Outlet[T]
  protected var producer: ActorRef[Request] = _
  protected var nextConsumerId: BigInt = -1
  protected var consumerCount = 0
  protected var completed = false
  protected var failed: Option[Throwable] = None

  protected def producerBehavior(): Behavior[Request]
  protected def shape: SourceShape[T]

  protected def preStartIncarnation(incarnation: ConsumerLogic[T]): Unit = synchronized {
    consumerCount += 1
    nextConsumerId += 1
    incarnation.consumerId = nextConsumerId
    if (completed) incarnation.completeStage()
    else if (failed.nonEmpty) incarnation.failStage(failed.get)
    else {
      if (consumerCount == 1) {
        if (producer == null) producer = actorSystem.spawnAnonymous(producerBehavior())
      }
      register(incarnation, baseTimeoutDelay)
    }
  }

  protected def register(incarnation: ConsumerLogic[T], timeoutDelay: FiniteDuration): Unit = synchronized {
    implicit val timeout: Timeout = Timeout(timeoutDelay)
    producer.ask {
      replyTo: ActorRef[Response] => Register(incarnation.consumer(), replyTo)
    }.onComplete(
      incarnation.getAsyncCallback[Try[Response]] {
        case Success(Registered) =>
          incarnation.isRegistered = true
          if (incarnation.isAvailable(out)) incarnation.pull(baseTimeoutDelay)
        case Success(Closed) => incarnation.completeStage()
        case Success(Fail(reason)) => producerFailed(incarnation, reason)
        case Failure(reason) =>
          if (reason.isInstanceOf[TimeoutException]
            && reason.getMessage.startsWith("A")) register(incarnation, timeoutDelay * 2)
          else producerFailed(incarnation, reason)
      }.invoke
    )
  }

  protected def postStopIncarnation(incarnation: ConsumerLogic[T]): Unit = synchronized {
    if (consumerCount == 1 && (restartSource || completed || failed.nonEmpty)) {
      if (producer != null) actorSystem.stop(producer.toClassic)
      producer = null
    }
    consumerCount -= 1
    unregister(incarnation, baseTimeoutDelay)
  }

  protected def unregister(incarnation: ConsumerLogic[T], timeoutDelay: FiniteDuration): Unit = synchronized {
    if (producer != null) {
      implicit val timeout: Timeout = Timeout(timeoutDelay)
      producer.ask {
        replyTo: ActorRef[Response] => Unregister(incarnation.consumer(), replyTo)
      }.onComplete {
        case Success(Unregistered) | Success(Closed) =>
        case Success(Fail(reason)) => producerFailed(incarnation, reason)
        case Failure(reason) =>
          if (reason.isInstanceOf[TimeoutException]
            && reason.getMessage.startsWith("A")) unregister(incarnation, timeoutDelay * 2)
      }
    }
  }

  protected def producerCompleted(incarnation: ConsumerLogic[T]): Unit = synchronized {
    completed = true
    incarnation.completeStage()
  }

  protected def producerFailed(incarnation: ConsumerLogic[T], reason: Throwable): Unit = synchronized {
    failed = Some(reason)
    incarnation.failStage(reason)
  }
}

object BehaviorBased {
  trait Request
  trait Response
  trait Consumer {
    val id: BigInt
  }

  case class BaseConsumer(id: BigInt) extends Consumer
  case class Register[C <: Consumer](consumer: C, replyTo: ActorRef[Response]) extends Request
  case class Unregister[C <: Consumer](consumer: C, replyTo: ActorRef[Response]) extends Request
  case object Registered extends Response
  case object Unregistered extends Response
  case class Offer[T](itemId: BigInt, item: T) extends Response
  case object Closed extends Response
  case class Fail(exception: Throwable) extends Response

  abstract class ConsumerLogic[T](operator: BehaviorBased[T])
                                 (implicit executionContext: ExecutionContext, scheduler: Scheduler)
      extends GraphStageLogic(operator.shape) {
    var consumerId: BigInt = _
    var isRegistered = false
    protected var nextItemId: BigInt = 0

    override def preStart(): Unit = operator.preStartIncarnation(this)
    override def postStop(): Unit = operator.postStopIncarnation(this)

    setHandler(operator.out, new OutHandler {
      def onPull(): Unit = if (isRegistered) pull(operator.baseTimeoutDelay)
    })

    def pull(timeoutDelay: FiniteDuration): Unit = {
      implicit val timeout: Timeout = Timeout(timeoutDelay)
      operator.producer.ask(onPullCommand).onComplete(
        getAsyncCallback[Try[Response]] {
          case Success(Offer(itemId, item: T)) =>
            nextItemId = itemId + 1
            push(operator.out, item)
          case Success(Closed) => operator.producerCompleted(this)
          case Success(Fail(reason)) => operator.producerFailed(this, reason)
          case Failure(reason) =>
            if (reason.isInstanceOf[TimeoutException]
              && reason.getMessage.startsWith("A")) pull(timeoutDelay * 2)
            else operator.producerFailed(this, reason)
        }.invoke
      )
    }

    protected def onPullCommand(replyTo: ActorRef[Response]): Request
    def consumer(): Consumer
  }
}