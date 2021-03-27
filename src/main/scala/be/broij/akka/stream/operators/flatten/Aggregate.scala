package be.broij.akka.stream.operators.flatten

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.typed.scaladsl.adapter.{ClassicActorSystemOps, TypedActorRefOps}
import akka.stream.{Attributes, Inlet, Materializer, OverflowStrategy, QueueOfferResult, SinkShape}
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.util.Timeout
import be.broij.akka.stream.operators.flatten.Aggregate.{Consumer, ConsumerCompleted, ConsumerFailed, ProducerCompleted, ProducerFailed, ProducerLogic, Register, Registered, Request, Response, Unregister, Unregistered}
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.util.{Failure, Success, Try}

class Aggregate[T](flatteningOperator: Sink[Source[T, NotUsed], NotUsed], restartSink: Boolean,
                   val baseTimeoutDelay: FiniteDuration)
                  (implicit val actorSystem: ActorSystem) extends GraphStage[SinkShape[T]] {
  implicit lazy val scheduler: Scheduler = actorSystem.toTyped.scheduler
  implicit lazy val executionContext: ExecutionContext = actorSystem.dispatcher

  protected var consumer: ActorRef[Request] = _
  protected var nextProducerId: BigInt = -1
  protected var producerCount = 0
  protected var consumerCompleted = false
  protected var consumerFailed: Option[Throwable] = None
  protected lazy val in: Inlet[T] = Inlet[T]("aggregate.in")

  def shape: SinkShape[T] = SinkShape(in)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new ProducerLogic(this)

  protected def preStartIncarnation(incarnation: ProducerLogic[T]): Unit = synchronized {
    System.err.println("preStart")
    producerCount += 1
    nextProducerId += 1
    incarnation.producerId = nextProducerId
    if (consumerCompleted) incarnation.completeStage()
    else if (consumerFailed.nonEmpty) incarnation.failStage(consumerFailed.get)
    else {
      if (producerCount == 1 && consumer == null) {
        System.err.println("spawning")
        consumer = actorSystem.spawnAnonymous(Consumer(flatteningOperator).behavior())
      }
      register(incarnation, baseTimeoutDelay)
    }
  }

  protected def register(incarnation: ProducerLogic[T], timeoutDelay: FiniteDuration): Unit = synchronized {
    implicit val timeout: Timeout = Timeout(timeoutDelay)
    consumer.ask {
      replyTo: ActorRef[Response] => Register(incarnation.producerId, replyTo)
    }.onComplete(
      incarnation.getAsyncCallback[Try[Response]] {
        case Success(Registered) => incarnation.pull()
        case Success(ConsumerCompleted) => consumerCompleted(incarnation)
        case Success(ConsumerFailed(reason)) => consumerFailed(incarnation, reason)
        case Failure(reason) =>
          if (reason.isInstanceOf[TimeoutException]
            && reason.getMessage.startsWith("A")) register(incarnation, timeoutDelay * 2)
          else consumerFailed(incarnation, reason)
      }.invoke
    )
  }

  protected def postStopIncarnation(incarnation: ProducerLogic[T]): Unit = synchronized {
    System.err.println("postStop")
    if (producerCount == 1 && (restartSink || consumerCompleted || consumerFailed.nonEmpty)) {
      if (consumer != null) {
        System.err.println(s"killing $consumerCompleted $consumerFailed")
        actorSystem.stop(consumer.toClassic)
      }
      consumer = null
    }
    producerCount -= 1
    unregister(incarnation, baseTimeoutDelay)
  }

  protected def unregister(incarnation: ProducerLogic[T], timeoutDelay: FiniteDuration): Unit = synchronized {
    if (consumer != null) {
      implicit val timeout: Timeout = Timeout(timeoutDelay)
      consumer.ask {
        replyTo: ActorRef[Response] => Unregister(incarnation.producerId, replyTo)
      }.onComplete {
        case Success(Unregistered) =>
        case Success(ConsumerCompleted) => consumerCompleted(incarnation)
        case Success(ConsumerFailed(reason)) => consumerFailed(incarnation, reason)
        case Failure(reason) =>
          if (reason.isInstanceOf[TimeoutException]
            && reason.getMessage.startsWith("A")) unregister(incarnation, timeoutDelay * 2)
      }
    }
  }

  protected def consumerCompleted(incarnation: ProducerLogic[T]): Unit = synchronized {
    consumerCompleted = true
    incarnation.completeStage()
  }

  protected def consumerFailed(incarnation: ProducerLogic[T], reason: Throwable): Unit = synchronized {
    consumerFailed = Some(reason)
    incarnation.failStage(reason)
  }
}

object Aggregate {
  protected trait Request
  protected trait Response
  protected case class Register(producerId: BigInt, replyTo: ActorRef[Response]) extends Request
  private case class RegisterResult(replyTo: ActorRef[Aggregate.Response], result: Try[QueueOfferResult]) extends Request
  protected case class Unregister(producerId: BigInt, replyTo: ActorRef[Response]) extends Request
  protected case class Offer[T](producerId: BigInt, requestId: BigInt, item: T, replyTo: ActorRef[Response]) extends Request
  private case class OfferResult(producerId: BigInt, requestId: BigInt, rollbackId: BigInt,
                                 replyTo: ActorRef[Aggregate.Response], result: Try[QueueOfferResult]) extends Request
  protected case object Registered extends Response
  protected case object Unregistered extends Response
  protected case class Ack(requestId: BigInt) extends Response
  protected case object ConsumerCompleted extends Response
  protected case class ConsumerFailed(exception: Throwable) extends Response
  protected case object ProducerCompleted extends Response
  protected case class ProducerFailed(exception: Throwable) extends Response

  protected class ProducerLogic[T](operator: Aggregate[T])
                                  (implicit executionContext: ExecutionContext, scheduler: Scheduler)
    extends GraphStageLogic(operator.shape) {
      var producerId: BigInt = _
      protected var nextRequestId: BigInt = 0

      override def preStart(): Unit = operator.preStartIncarnation(this)
      override def postStop(): Unit = operator.postStopIncarnation(this)

      setHandler(operator.in, new InHandler {
        def onPush(): Unit = push(grab(operator.in), operator.baseTimeoutDelay)
      })

      def pull(): Unit = pull(operator.in)

      def push(item: T, timeoutDelay: FiniteDuration): Unit = {
        implicit val timeout: Timeout = Timeout(timeoutDelay)
        operator.consumer.ask{
          ref: ActorRef[Response] => Offer(producerId, nextRequestId, item, ref)
        }.onComplete(
          getAsyncCallback[Try[Response]] {
            case Success(Ack(requestId: BigInt)) =>
              nextRequestId = requestId + 1
              pull(operator.in)
            case Success(ProducerCompleted) => completeStage()
            case Success(ProducerFailed(reason)) => failStage(reason)
            case Success(ConsumerCompleted) => operator.consumerCompleted(this)
            case Success(ConsumerFailed(reason)) => operator.consumerFailed(this, reason)
            case Failure(reason) =>
              if (reason.isInstanceOf[TimeoutException]
                && reason.getMessage.startsWith("A")) push(item, timeoutDelay * 2)
              else operator.consumerFailed(this, reason)
          }.invoke
        )
      }
  }

  protected class Consumer[T](flatteningOperator: Sink[Source[T, NotUsed], NotUsed])
                             (implicit materializer: Materializer) {
    implicit val executionContext = materializer.executionContext

    val producers = mutable.Map.empty[BigInt, SourceQueueWithComplete[T]]
    val commitLog = mutable.Map.empty[BigInt, BigInt]
    val inProgress = mutable.Map.empty[BigInt, BigInt]
    lazy val sourcesQueue: SourceQueueWithComplete[Source[T, NotUsed]] = {
      Source.queue(0, OverflowStrategy.backpressure).to(flatteningOperator).run()
    }

    protected def completedBehavior(): Behavior[Request] =
      Behaviors.receive {
        case (_, Register(_, replyTo)) =>
          replyTo ! ConsumerCompleted
          Behaviors.same
        case (_, Offer(_, _, _, replyTo)) =>
          replyTo ! ConsumerCompleted
          Behaviors.same
        case (_, Unregister(_, replyTo)) =>
          replyTo ! ConsumerCompleted
          Behaviors.same
        case _ => Behaviors.same
      }

    protected def failedBehavior(fail: ConsumerFailed): Behavior[Request] =
      Behaviors.receive {
        case (_, Register(_, replyTo)) =>
          replyTo ! fail
          Behaviors.same
        case (_, Offer(_, _, _, replyTo)) =>
          replyTo ! fail
          Behaviors.same
        case (_, Unregister(_, replyTo)) =>
          replyTo ! fail
          Behaviors.same
        case _ => Behaviors.same
      }

    def behavior(): Behavior[Request] = aliveBehavior(false)

    private def aliveBehavior(registering: Boolean): Behavior[Request] =
      Behaviors.receive {
        case (context, Register(producerId, replyTo)) =>
          if (registering) Behaviors.same
          else {
            val (queue, source) = Source.queue[T](0, OverflowStrategy.backpressure).preMaterialize()
            producers += producerId -> queue
            commitLog += producerId -> -1
            context.pipeToSelf(sourcesQueue.offer(source))(RegisterResult(replyTo, _))
            aliveBehavior(true)
          }
        case (_, RegisterResult(replyTo, result)) =>
          result match {
            case Success(QueueOfferResult.Enqueued) =>
              replyTo ! Registered
              aliveBehavior(false)
            case Success(QueueOfferResult.Failure(exception)) =>
              val response = ConsumerFailed(exception)
              replyTo ! response
              failedBehavior(response)
            case Failure(_) | Success(QueueOfferResult.QueueClosed) =>
              replyTo ! ConsumerCompleted
              completedBehavior()
            case Success(QueueOfferResult.Dropped) =>
              aliveBehavior(false)
          }
        case (context, Offer(producerId, requestId, item: T, replyTo)) =>
          if (commitLog.get(producerId).exists(_ < requestId)) {
            val rollbackId = commitLog(producerId)
            producers.get(producerId).foreach {
              queue => context.pipeToSelf(queue.offer(item))(OfferResult(producerId, requestId, rollbackId, replyTo, _))
            }
            commitLog += producerId -> requestId
            inProgress += producerId -> requestId
          } else if (!inProgress.get(producerId).contains(requestId)) {
            replyTo ! Ack(requestId)
          }
          Behaviors.same
        case (_, OfferResult(producerId, requestId, rollbackId, replyTo, result)) =>
          inProgress -= producerId
          result match {
            case Success(QueueOfferResult.Enqueued) =>
              replyTo ! Ack(requestId)
              Behaviors.same
            case Success(QueueOfferResult.Failure(exception)) =>
              commitLog += producerId -> rollbackId
              replyTo ! ProducerFailed(exception)
              Behaviors.same
            case Failure(_) | Success(QueueOfferResult.QueueClosed) =>
              commitLog += producerId -> rollbackId
              replyTo ! ProducerCompleted
              Behaviors.same
            case Success(QueueOfferResult.Dropped) =>
              commitLog += producerId -> rollbackId
              Behaviors.same
          }
        case (_, Unregister(producerId, replyTo)) =>
          producers.get(producerId).foreach(_.complete())
          commitLog.remove(producerId)
          inProgress.remove(producerId)
          replyTo ! Unregistered
          Behaviors.same
      }
  }

  protected object Consumer {
    def apply[T](flatteningOperator: Sink[Source[T, NotUsed], NotUsed])
                (implicit materializer: Materializer) = new Consumer(flatteningOperator)
  }

  def apply[T](flatteningOperator: Sink[Source[T, NotUsed], NotUsed], restartSink: Boolean,
               baseTimeoutDelay: FiniteDuration)
              (implicit actorSystem: ActorSystem): Sink[T, NotUsed] =
    Sink.fromGraph(new Aggregate(flatteningOperator, restartSink, baseTimeoutDelay))
}