package be.broij.akka.stream

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import be.broij.akka.stream.operators.{CaptureMaterializedValues, DistinctKey, FilterConsecutives, Reorder}
import be.broij.akka.stream.operators.flatten.{Concatenate, Join, JoinFairly, JoinWithPriorities, MapConcatenate, MapJoin, MapJoinFairly, MapJoinWithPriorities, MapSwitch, Switch}
import be.broij.akka.stream.operators.window.{TimedSlidingWindow, TimedWindow, WeightedSlidingWindow, WeightedWindow}
import be.broij.akka.stream.operators.Window.{Frame, FrameFactory}
import java.time.ZonedDateTime
import scala.concurrent.duration._
import scala.collection.immutable.Seq

object FlowExtensions {
  implicit class ConcatenateFlowConversion[In, Out, Mat](flow: Flow[In, Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[Concatenate.apply]]
      */
    def concatenate: Flow[In, Out, Mat] = flow.via(Concatenate.apply)
  }

  implicit class MapConcatenateFlowConversion[In, Out1, Out2, Mat](flow: Flow[In, Out1, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapConcatenate.apply]]
      */
    def flatMapConcatenate(mapper: Out1 => Source[Out2, NotUsed]): Flow[In, Out2, Mat] = 
      flow.via(MapConcatenate(mapper))
  }

  implicit class SwitchFlowConversion[In, Out, Mat](flow: Flow[In, Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[Switch.apply]]
      */
    def switch(implicit materializer: Materializer): Flow[In, Out, Mat] = flow.via(Switch.apply)
  }

  implicit class MapSwitchFlowConversion[In, Out1, Out2, Mat](flow: Flow[In, Out1, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapSwitch.apply]]
      */
    def flatMapSwitch(mapper: Out1 => Source[Out2, NotUsed])
                     (implicit materializer: Materializer): Flow[In, Out2, Mat] =
      flow.via(MapSwitch(mapper))
  }

  implicit class JoinFlowConversion[In, Out, Mat](flow: Flow[In, Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[Join.apply]]
      */
    def join(breadth: Option[BigInt] = None): Flow[In, Out, Mat] = flow.via(Join(breadth))
  }

  implicit class MapJoinFlowConversion[In, Out1, Out2, Mat](flow: Flow[In, Out1, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapJoin.apply]]
      */
    def flatMapJoin(mapper: Out1 => Source[Out2, NotUsed], breadth: Option[BigInt] = None): Flow[In, Out2, Mat] =
      flow.via(MapJoin(mapper, breadth))
  }

  implicit class JoinFairlyFlowConversion[In, Out, Mat](flow: Flow[In, Source[Out, NotUsed], Mat]) {
    /**
      * This function is an implicit conversion calling [[JoinFairly.apply]]
      */
    def joinFairly(n: BigInt, breadth: Option[BigInt] = None): Flow[In, Out, Mat] = flow.via(JoinFairly(n, breadth))
  }

  implicit class MapJoinFairlyFlowConversion[In, Out1, Out2, Mat](flow: Flow[In, Out1, Mat]) {
    /**
      * This function is an implicit conversion calling [[MapJoinFairly.apply]]
      */
    def flatMapJoinFairly(n: BigInt, mapper: Out1 => Source[Out2, NotUsed],
                          breadth: Option[BigInt] = None): Flow[In, Out2, Mat] =
      flow.via(MapJoinFairly(n, mapper, breadth))
  }

  implicit class JoinWithPrioritiesFlowConversion[In, Out, Mat, P: Ordering](
    flow: Flow[In, Source[Out, NotUsed], Mat]
  ) {
    /**
      * This function is an implicit conversion calling [[JoinWithPriorities.apply]]
      */
    def joinWithPriorities(priorityOf: Out => P, breadth: Option[BigInt] = None): Flow[In, Out, Mat] =
      flow.via(JoinWithPriorities(priorityOf, breadth))
  }

  implicit class FlatMapJoinWithPrioritiesFlowConversion[In, Out1, Out2, Mat, P: Ordering](
    flow: Flow[In, Out1, Mat]
  ) {
    /**
      * This function is an implicit conversion calling [[MapJoinWithPriorities.apply]]
      */
    def flatMapJoinWithPriorities(mapper: Out1 => Source[Out2, NotUsed], priorityOf: Out2 => P,
                                  breadth: Option[BigInt] = None): Flow[In, Out2, Mat] =
      flow.via(MapJoinWithPriorities(mapper, priorityOf, breadth))
  }

  implicit class DistinctKeyFlowConversion[In, Out, K, Mat](flow: Flow[In, Out, Mat]) {
    /**
     * This function is an implicit conversion calling [[DistinctKey.apply]]
     */
    def distinctKey(key: Out => K): Flow[In, Out, Mat] =
      flow.via(DistinctKey(key))
  }

  implicit class FilterConsecutivesFlowConversion[In, Out, K, Mat](flow: Flow[In, Out, Mat]) {
    /**
     * This function is an implicit conversion calling [[FilterConsecutives.apply]]
     */
    def filterConsecutives(shouldFilter: (Option[Out], Out) => Boolean): Flow[In, Out, Mat] =
      flow.via(FilterConsecutives(shouldFilter))
  }

  implicit class CaptureMaterializedValuesFlowConversion[In, Out, Mat1, Mat2](flow: Flow[In, Source[Out, Mat1], Mat2]) {
    /**
     * This function is an implicit conversion calling [[CaptureMaterializedValues.apply]]
     */
    def captureMaterializedValues(
      implicit materializer: Materializer
    ): Flow[In, Source[Out, NotUsed], Source[Mat1, NotUsed]] =
      flow.viaMat(CaptureMaterializedValues(materializer))(Keep.right)
  }

  implicit class TimedWindowFlowConversion[In, Out, Mat](flow: Flow[In, Out, Mat]) {
    /**
      * This function is an implicit conversion calling [[TimedWindow.apply]]
      */
    def timedWindow(maxPeriod: FiniteDuration, timeOf: Out => ZonedDateTime): Flow[In, Seq[Out], Mat] =
      flow.via(TimedWindow(maxPeriod, timeOf))
  }

  implicit class TimedSlidingWindowFlowConversion[In, Out, Mat](flow: Flow[In, Out, Mat]) {
    /**
      * This function is an implicit conversion calling [[TimedSlidingWindow.apply]]
      */
    def timedSlidingWindow(maxPeriod: FiniteDuration, timeOf: Out => ZonedDateTime): Flow[In, Seq[Out], Mat] =
      flow.via(TimedSlidingWindow(maxPeriod, timeOf))
  }

  implicit class WeightedWindowFlowConversion[In, Out, Mat, W: Numeric](flow: Flow[In, Out, Mat]) {
    /**
      * This function is an implicit conversion calling [[WeightedWindow.apply]]
      */
    def weightedWindow(maxWeight: W, weightOf: Out => W): Flow[In, Seq[Out], Mat] =
      flow.via(WeightedWindow(maxWeight, weightOf))
  }

  implicit class WeightedSlidingWindowFlowConversion[In, Out, Mat, W: Numeric](flow: Flow[In, Out, Mat]) {
    /**
      * This function is an implicit conversion calling [[WeightedSlidingWindow.apply]]
      */
    def weightedSlidingWindow(maxWeight: W, weightOf: Out => W): Flow[In, Seq[Out], Mat] =
      flow.via(WeightedSlidingWindow(maxWeight, weightOf))
  }

  implicit class ReorderFlowConversion[In, Out: Ordering, Mat](flow: Flow[In, Out, Mat]) {
    /**
      * This function is an implicit conversion calling [[Reorder.apply]]
      */
    def reorder[F <: Frame[Out]](implicit frameFactory: FrameFactory[Out, F]): Flow[In, Out, Mat] =
      flow.via(Reorder.apply(implicitly[Ordering[Out]], frameFactory))

    /**
      * This function is an implicit conversion calling [[Reorder.apply]]
      */
    def reorder[W: Numeric](maxWeight: W, weightOf: Out => W): Flow[In, Out, Mat] =
      flow.via(Reorder(maxWeight, weightOf))

    /**
      * This function is an implicit conversion calling [[Reorder.apply]]
      */
    def reorder(maxPeriod: FiniteDuration, timeOf: Out => ZonedDateTime): Flow[In, Out, Mat] =
      flow.via(Reorder(maxPeriod, timeOf))
  }
}
