package be.broij.akka.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Source}
import be.broij.akka.stream.operators.Window.{Frame, FrameFactory}
import be.broij.akka.stream.operators.diverge.{Anycast, AnycastWithPriorities, Balance, Broadcast, Partition}
import be.broij.akka.stream.operators.{CaptureMaterializedValues, DistinctKey, FilterConsecutives, Reorder}
import be.broij.akka.stream.operators.flatten.{Concatenate, Join, JoinFairly, JoinWithPriorities, MapConcatenate, MapJoin, MapJoinFairly, MapJoinWithPriorities, MapSwitch, Switch}
import be.broij.akka.stream.operators.window.{TimedSlidingWindow, TimedWindow, WeightedSlidingWindow, WeightedWindow}
import java.time.ZonedDateTime
import scala.concurrent.duration._
import scala.collection.immutable.Seq

object SourceExtensions {
  implicit class BroadcastSourceConversion[Out](source: Source[Out, NotUsed]) {
    /**
      * This function is an implicit conversion calling [[Broadcast.apply]]
      */
    def broadcast(restartSource: Boolean, waitSlowest: Boolean, bufferSize: Int, baseTimeoutDelay: FiniteDuration)
                 (implicit actorSystem: ActorSystem): Source[Out, NotUsed] =
      Broadcast(source, restartSource, waitSlowest, bufferSize, baseTimeoutDelay)
  }

  implicit class PartitionSourceConversion[Out, P](source: Source[Out, NotUsed]) {
    /**
      * This function is an implicit conversion calling [[Partition.apply]]
      */
    def partition(restartSource: Boolean, partitionOf: Out => P, waitSlowest: Boolean, bufferSize: Int,
                  baseTimeoutDelay: FiniteDuration)
                 (implicit actorSystem: ActorSystem): P => Source[Out, NotUsed] =
      Partition(source, partitionOf, restartSource, waitSlowest, bufferSize, baseTimeoutDelay)
  }

  implicit class AnycastSourceConversion[Out](source: Source[Out, NotUsed]) {
    /**
      * This function is an implicit conversion calling [[Anycast.apply]]
      */
    def anycast(restartSource: Boolean, baseTimeoutDelay: FiniteDuration)
               (implicit actorSystem: ActorSystem): Source[Out, NotUsed] =
      Anycast(source, restartSource, baseTimeoutDelay)
  }

  implicit class AnycastWithPrioritiesSourceConversion[Out, P: Ordering](source: Source[Out, NotUsed]) {
    /**
      * This function is an implicit conversion calling [[AnycastWithPriorities.apply]]
      */
    def anycastWithPriorities(restartSource: Boolean, baseTimeoutDelay: FiniteDuration)
                             (implicit actorSystem: ActorSystem): AnycastWithPriorities[Out, P] =
      AnycastWithPriorities(source, restartSource, baseTimeoutDelay)
  }

  implicit class BalanceSourceConversion[Out](source: Source[Out, NotUsed]) {
    /**
      * This function is an implicit conversion calling [[Balance.apply]]
      */
    def balance(restartSource: Boolean = true, n: BigInt, baseTimeoutDelay: FiniteDuration)
               (implicit actorSystem: ActorSystem): Source[Out, NotUsed] =
      Balance(source, n, restartSource, baseTimeoutDelay)
  }

  implicit class JoinSourceConversion[Out, Mat](source: Source[Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[Join.apply]]
      */
    def join(breadth: Option[BigInt]): Source[Out, Mat] = source.via(Join(breadth))
  }

  implicit class MapJoinSourceConversion[In, Out, Mat](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapJoin.apply]]
      */
    def flatMapJoin(mapper: In => Source[Out, NotUsed], breadth: Option[BigInt]): Source[Out, Mat] =
      source.via(MapJoin(mapper, breadth))
  }

  implicit class ConcatenateSourceConversion[Out, Mat](source: Source[Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[Concatenate.apply]]
      */
    def concatenate: Source[Out, Mat] = source.via(Concatenate.apply)
  }

  implicit class MapConcatenateSourceConversion[In, Out, Mat](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapConcatenate.apply]]
      */
    def flatMapConcatenate(mapper: In => Source[Out, NotUsed]): Source[Out, Mat] = source.via(MapConcatenate(mapper))
  }

  implicit class SwitchSourceConversion[Out, Mat](source: Source[Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[Switch.apply]]
      */
    def switch(implicit materializer: Materializer): Source[Out, Mat] = source.via(Switch.apply)
  }

  implicit class MapSwitchSourceConversion[In, Out, Mat](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapSwitch.apply]]
      */
    def flatMapSwitch(mapper: In => Source[Out, NotUsed])(implicit materializer: Materializer): Source[Out, Mat] =
      source.via(MapSwitch(mapper))
  }

  implicit class JoinFairlySourceConversion[Out, Mat](source: Source[Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[JoinFairly.apply]]
      */
    def joinFairly(n: BigInt, breadth: Option[BigInt]): Source[Out, Mat] = source.via(JoinFairly(n, breadth))
  }

  implicit class MapJoinFairlySourceConversion[In, Out, Mat](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapJoinFairly.apply]]
      */
    def flatMapJoinFairly(n: BigInt, mapper: In => Source[Out, NotUsed], breadth: Option[BigInt]): Source[Out, Mat] =
      source.via(MapJoinFairly(n, mapper, breadth))
  }

  implicit class JoinWithPrioritiesSourceConversion[Out, Mat, P: Ordering](source: Source[Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[JoinWithPriorities.apply]]
      */
    def joinWithPriorities(priorityOf: Out => P, breadth: Option[BigInt]): Source[Out, Mat] =
      source.via(JoinWithPriorities(priorityOf, breadth))
  }

  implicit class MapJoinWithPrioritiesSourceConversion[In, Out, Mat, P: Ordering](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapJoinWithPriorities.apply]]
      */
    def flatMapJoinWithPriorities(mapper: In => Source[Out, NotUsed], priorityOf: Out => P,
                                  breadth: Option[BigInt]): Source[Out, Mat] =
      source.via(MapJoinWithPriorities(mapper, priorityOf, breadth))
  }

  implicit class DistinctKeySourceConversion[In, K, Mat](source: Source[In, Mat]) {
    /**
     * This function is an implicit conversion calling [[DistinctKey.apply]]
     */
    def distinctKey(key: In => K): Source[In, Mat] =
      source.via(DistinctKey(key))
  }

  implicit class FilterConsecutivesSourceConversion[In, K, Mat](source: Source[In, Mat]) {
    /**
     * This function is an implicit conversion calling [[FilterConsecutives.apply]]
     */
    def filterConsecutives(shouldFilter: (Option[In], In) => Boolean): Source[In, Mat] =
      source.via(FilterConsecutives(shouldFilter))
  }

  implicit class CaptureMaterializedValuesSourceConversion[In, Mat1, Mat2](source: Source[Source[In, Mat1], Mat2]) {
    /**
     * This function is an implicit conversion calling [[CaptureMaterializedValues.apply]]
     */
    def captureMaterializedValues(
      implicit materializer: Materializer
    ): Source[Source[In, NotUsed], Source[Mat1, NotUsed]] =
      source.viaMat(CaptureMaterializedValues(materializer))(Keep.right)
  }

  implicit class WeightedWindowSourceConversion[In, Mat, W: Numeric](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[WeightedWindow.apply]]
      */
    def weightedWindow(maxWeight: W, weightOf: In => W): Source[Seq[In], Mat] =
      source.via(WeightedWindow(maxWeight, weightOf))
  }

  implicit class WeightedSlidingWindowSourceConversion[In, Mat, W: Numeric](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[WeightedSlidingWindow.apply]]
      */
    def weightedSlidingWindow(maxWeight: W, weightOf: In => W): Source[Seq[In], Mat] =
      source.via(WeightedSlidingWindow(maxWeight, weightOf))
  }

  implicit class TimedWindowSourceConversion[In, Mat](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[TimedWindow.apply]]
      */
    def timedWindow(maxPeriod: FiniteDuration, timeOf: In => ZonedDateTime): Source[Seq[In], Mat] =
      source.via(TimedWindow(maxPeriod, timeOf))
  }

  implicit class TimedSlidingWindowSourceConversion[In, Mat](source: Source[In, Mat]) {
    /**
      * This function is an implicit conversion calling [[TimedSlidingWindow.apply]]
      */
    def timedSlidingWindow(maxPeriod: FiniteDuration, timeOf: In => ZonedDateTime): Source[Seq[In], Mat] =
      source.via(TimedSlidingWindow(maxPeriod, timeOf))
  }

  implicit class ReorderSourceConversion[In: Ordering, Mat](source: Source[In, Mat]) {
    /**
     * This function is an implicit conversion calling [[Reorder.apply]]
     */
    def reorder[F <: Frame[In]](implicit frameFactory: FrameFactory[In, F]): Source[In, Mat] =
      source.via(Reorder.apply(implicitly[Ordering[In]], frameFactory))

    /**
     * This function is an implicit conversion calling [[Reorder.apply]]
     */
    def reorder[W: Numeric](maxWeight: W, weightOf: In => W): Source[In, Mat] =
      source.via(Reorder(maxWeight, weightOf))

    /**
     * This function is an implicit conversion calling [[Reorder.apply]]
     */
    def reorder(maxPeriod: FiniteDuration, timeOf: In => ZonedDateTime): Source[In, Mat] =
      source.via(Reorder(maxPeriod, timeOf))
  }
}
